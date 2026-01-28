// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include <math.h>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include "switch.h"
#include "hpccpacket.h"
#include "queue_lossless_output.h"
#include "queue_lossless_input.h"
#include "rocepacket.h"

int LosslessOutputQueue::_ecn_enabled = false;
int LosslessOutputQueue::_K = 0;
flowid_t LosslessOutputQueue::_fg_flowid_threshold = 1000;
simtime_picosec LosslessOutputQueue::_fg_util_sample_period = timeFromMs(0.25);
bool LosslessOutputQueue::_log_spine0_ecn = false;
std::string LosslessOutputQueue::_spine0_ecn_substr = "";

namespace {
bool is_fg_flow(const Packet& pkt) {
    flowid_t id = pkt.flow_id();
    return id > 0 && id <= LosslessOutputQueue::_fg_flowid_threshold;
}

bool is_spine_to_leaf1_queue(LosslessOutputQueue* queue) {
    const string& name = queue->str();
    return name.rfind("US", 0) == 0 && name.find("->LS_1(") != string::npos;
}

struct FgQueueState {
    simtime_picosec last_change = 0;
    simtime_picosec busy_in_window = 0;
    double link_bps = 0.0;
    bool fg_busy = false;
    bool initialized = false;
};

class FgSpineLeaf1UtilMonitor : public EventSource {
public:
    FgSpineLeaf1UtilMonitor(EventList& eventlist, simtime_picosec period)
        : EventSource(eventlist, "fg_spine_leaf1_util"),
          _period(period) {
        if (_period > 0) {
            eventlist.sourceIsPendingRel(*this, _period);
        }
    }

    void txStart(LosslessOutputQueue* queue, const Packet& pkt) {
        simtime_picosec now = eventlist().now();
        auto& state = _states[queue];
        if (!state.initialized) {
            state.initialized = true;
            state.last_change = now;
            state.busy_in_window = 0;
            state.fg_busy = false;
        }
        if (state.link_bps == 0.0) {
            simtime_picosec drain_ps = queue->drainTime(const_cast<Packet*>(&pkt));
            if (drain_ps > 0 && pkt.size() > 0) {
                state.link_bps = (static_cast<double>(pkt.size()) * 8.0 * 1e12) /
                                 static_cast<double>(drain_ps);
            }
        }
        if (state.fg_busy) {
            return;
        }
        state.last_change = now;
        state.fg_busy = true;
    }

    void txEnd(LosslessOutputQueue* queue) {
        auto it = _states.find(queue);
        if (it == _states.end()) {
            return;
        }
        simtime_picosec now = eventlist().now();
        auto& state = it->second;
        if (state.fg_busy) {
            state.busy_in_window += now - state.last_change;
            state.fg_busy = false;
        }
        state.last_change = now;
    }

    void doNextEvent() override {
        if (_period <= 0) {
            return;
        }
        simtime_picosec now = eventlist().now();
        double period_ps = static_cast<double>(_period);
        double baseline_bps = speedFromGbps(50.0);
        for (auto& entry : _states) {
            auto& state = entry.second;
            simtime_picosec busy = state.busy_in_window;
            if (state.fg_busy) {
                busy += now - state.last_change;
            }
            double util = 0.0;
            if (period_ps > 0.0) {
                util = static_cast<double>(busy) / period_ps;
            }
            double throughput_bps = util * state.link_bps;
            double ratio_50g = (baseline_bps > 0.0) ? (throughput_bps / baseline_bps) : 0.0;
            cout << "FG_SPINE_LEAF1_UTIL time_us " << timeAsUs(now)
                 << " queue " << entry.first->str()
                 << " throughput_gbps " << (throughput_bps / 1.0e9)
                 << " ratio_50g " << ratio_50g
                 << endl;
            state.busy_in_window = 0;
            state.last_change = now;
        }
        eventlist().sourceIsPendingRel(*this, _period);
    }

private:
    simtime_picosec _period;
    std::unordered_map<LosslessOutputQueue*, FgQueueState> _states;
};

FgSpineLeaf1UtilMonitor* fg_spine_leaf1_monitor = nullptr;

void record_fg_spine_leaf1_tx_start(LosslessOutputQueue* queue, Packet& pkt) {
    if (!is_fg_flow(pkt) || !is_spine_to_leaf1_queue(queue)) {
        return;
    }
    if (LosslessOutputQueue::_fg_util_sample_period <= 0) {
        return;
    }
    if (!fg_spine_leaf1_monitor) {
        fg_spine_leaf1_monitor = new FgSpineLeaf1UtilMonitor(queue->eventlist(),
                                                             LosslessOutputQueue::_fg_util_sample_period);
    }
    fg_spine_leaf1_monitor->txStart(queue, pkt);
}

void record_fg_spine_leaf1_tx_end(LosslessOutputQueue* queue, Packet& pkt) {
    if (!is_fg_flow(pkt) || !is_spine_to_leaf1_queue(queue)) {
        return;
    }
    if (!fg_spine_leaf1_monitor) {
        return;
    }
    fg_spine_leaf1_monitor->txEnd(queue);
}
} // namespace

LosslessOutputQueue::LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize, 
                                         EventList& eventlist, QueueLogger* logger)
    : Queue(bitrate,maxsize,eventlist,logger), 
      _state_send(READY)
{
    //assume worst case: PAUSE frame waits for one MSS packet to be sent to other switch, and there is 
    //an MSS just beginning to be sent when PAUSE frame arrives; this means 2 packets per incoming
    //port, and we must have buffering for all ports except this one (assuming no one hop cycles!)

    _sending = 0;

    _txbytes = 0;

    stringstream ss;
    ss << "queue lossless output(" << bitrate/1000000 << "Mb/s," << maxsize << "bytes)";
    _nodename = ss.str();
}


void
LosslessOutputQueue::receivePacket(Packet& pkt){
    if (pkt.type()==ETH_PAUSE)
        receivePacket(pkt,NULL);
    else {
        LosslessInputQueue* q = pkt.get_ingress_queue();
        pkt.clear_ingress_queue();
        receivePacket(pkt,dynamic_cast<VirtualQueue*>(q));
    }
}

void
LosslessOutputQueue::receivePacket(Packet& pkt,VirtualQueue* prev) 
{
    //is this a PAUSE frame? 
    if (pkt.type()==ETH_PAUSE){
        EthPausePacket* p = (EthPausePacket*)&pkt;

        if (p->sleepTime()>0){
            //remote end is telling us to shut up.
            //assert(_state_send == READY);
            if (_sending)
                //we have a packet in flight
                _state_send = PAUSE_RECEIVED;
            else
                _state_send = PAUSED;

            //cout << timeAsMs(eventlist().now()) << " " << _name << " PAUSED "<<endl;            
        }
        else {
            //we are allowed to send!
            _state_send = READY;
            //cout << timeAsMs(eventlist().now()) << " " << _name << " GO "<<endl;

            //start transmission if we have packets to send!
            if(_enqueued.size()>0&&!_sending)
                beginService();
        }
        
        pkt.free();
        return;
    }

    /* normal packet, enqueue it */

    //remember the virtual queue that has sent us this packet; will notify the vq once the packet has left our buffer.
    assert(prev!=NULL);

    pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_ARRIVE);

    bool queueWasEmpty = _enqueued.empty();

    _vq.push_front(prev);
    Packet* pkt_p = &pkt;
    _enqueued.push(pkt_p);

    _queuesize += pkt.size();
    if (_queuesize > _max_recorded_size) {
        _max_recorded_size = _queuesize;
    }
    if (pkt.flow_id() > 0 && pkt.flow_id() <= 32 && pkt.type() == ROCE) {
        RocePacket* rp = dynamic_cast<RocePacket*>(&pkt);
        if (rp && rp->seqno() == 1) {
            const Route* route = pkt.route();
            cout << "QUEUE_FG_FIRST time_us " << timeAsUs(eventlist().now())
                 << " flow " << pkt.flow_id()
                 << " queue " << _name
                 << " pathid " << pkt.pathid()
                 << " route_ptr " << route
                 << " route_path_id " << (route ? route->path_id() : -1)
                 << " route_no_paths " << (route ? route->no_of_paths() : -1)
                 << endl;
        }
    }


    if (_queuesize > _maxsize){
        static bool logged = false;
        if (!logged) {
            cout << " Queue " << _name
                 << " LOSSLESS not working! I should have dropped this packet"
                 << _queuesize / Packet::data_packet_size() << endl;
            logged = true;
        }
    }

    if (_logger) 
        _logger->logQueue(*this, QueueLogger::PKT_ENQUEUE, pkt);

    if (queueWasEmpty && _state_send == READY) {
        /* schedule the dequeue event */
        assert(_enqueued.size()==1);
        beginService();
    }
}

void LosslessOutputQueue::beginService(){
    assert(_state_send==READY&&!_sending);

    Packet* pkt = _enqueued.back();
    record_fg_spine_leaf1_tx_start(this, *pkt);

    Queue::beginService();
    _sending = 1;
}

namespace {
bool is_ecn_eligible(const Packet& pkt) {
    switch (pkt.type()) {
    case TCP:
    case SWIFT:
    case STRACK:
    case NDP:
    case NDPLITE:
    case ROCE:
    case HPCC:
    case EQDSDATA:
    case UECDATA:
        return true;
    default:
        return false;
    }
}
} // namespace

void LosslessOutputQueue::completeService(){
    /* dequeue the packet */
    assert(!_enqueued.empty());

    Packet* pkt = _enqueued.pop();
    _packets_served++;
    VirtualQueue* q = _vq.back();

    //_enqueued.pop_back();
    _vq.pop_back();

    //mark on deque
    if (_ecn_enabled && _queuesize > _K && is_ecn_eligible(*pkt)) {
        pkt->set_flags(pkt->flags() | ECN_CE);
        if (_log_spine0_ecn &&
            (_spine0_ecn_substr.empty() ||
             _name.find(_spine0_ecn_substr) != std::string::npos)) {
            cout << "ECN_MARK_SPINE0 time_us " << timeAsUs(eventlist().now())
                 << " queue " << _name
                 << " queuesize " << _queuesize
                 << " K " << _K
                 << endl;
        }
    }

    if (pkt->type()==HPCC){
        //HPPC INT information adding to packet
        HPCCPacket* h = dynamic_cast<HPCCPacket*>(pkt);
        assert(h->_int_hop<5);

        h->_int_info[h->_int_hop]._queuesize = _queuesize;
        h->_int_info[h->_int_hop]._ts = eventlist().now();

        if (_switch){
            h->_int_info[h->_int_hop]._switchID = _switch->getID();
            h->_int_info[h->_int_hop]._type = _switch->getType();
        }

        h->_int_info[h->_int_hop]._txbytes = _txbytes;
        h->_int_info[h->_int_hop]._linkrate = _bitrate;

        h->_int_hop++;
    }   

    _queuesize -= pkt->size();
    _txbytes += pkt->size();

    pkt->flow().logTraffic(*pkt, *this, TrafficLogger::PKT_DEPART);

    if (_logger) _logger->logQueue(*this, QueueLogger::PKT_SERVICE, *pkt);

    //tell the virtual input queue this packet is done!
    q->completedService(*pkt);

    //this is used for bandwidth utilization tracking. 
    log_packet_send(drainTime(pkt));
    record_fg_spine_leaf1_tx_end(this, *pkt);

    //if (((uint64_t)timeAsUs(eventlist().now()))%5==0)
    //    cout << "Queue bandwidth utilization " << average_utilization() << "%" << endl;

    /* tell the packet to move on to the next pipe */
    pkt->sendOn();

    _sending = 0;

    //if (_state_send!=READY){
    //cout << timeAsMs(eventlist().now()) << " queue " << _name << " not ready but sending pkt " << pkt->type() << endl;
    //}

    if (_state_send == PAUSE_RECEIVED)
        _state_send = PAUSED;

    if (!_enqueued.empty()) {
        if (_state_send == READY)
            /* start packet transmission, schedule the next dequeue event */
            beginService();
    }
}
