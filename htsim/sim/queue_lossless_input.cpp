// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include "queue_lossless_input.h"
#include <math.h>
#include <iostream>
#include <sstream>
#include "switch.h"

uint64_t LosslessInputQueue::_high_threshold = 0;
uint64_t LosslessInputQueue::_low_threshold = 0;
uint64_t LosslessInputQueue::_pause_sent = 0;
uint64_t LosslessInputQueue::_pause_cleared = 0;
std::unordered_map<const LosslessInputQueue*, uint64_t> LosslessInputQueue::_pause_sent_by_q = {};
std::unordered_map<const LosslessInputQueue*, uint64_t> LosslessInputQueue::_pause_cleared_by_q = {};
bool LosslessInputQueue::_log_pfc_events = false;

LosslessInputQueue::LosslessInputQueue(EventList& eventlist)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _state_recv(READY)
{
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    _wire = NULL;
}

LosslessInputQueue::LosslessInputQueue(EventList& eventlist,BaseQueue* peer)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _state_recv(READY)
{
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    stringstream ss;
    ss << "VirtualQueue("<< peer->_name<< ")";
    _nodename = ss.str();
    _remoteEndpoint = peer;
    _switch = NULL;
    _wire = NULL;

    peer->setRemoteEndpoint(this);
}

LosslessInputQueue::LosslessInputQueue(EventList& eventlist,BaseQueue* peer, Switch* sw, simtime_picosec wire_latency)
    : Queue(speedFromGbps(1),Packet::data_packet_size()*2000,eventlist,NULL),
      VirtualQueue(),
      _state_recv(READY)
{
    assert(_high_threshold>0);
    assert(_high_threshold > _low_threshold);

    stringstream ss;
    ss << "VirtualQueue("<< peer->_name<< ")";
    _nodename = ss.str();
    _remoteEndpoint = peer;
    _switch = sw;

    _wire = new CallbackPipe(wire_latency, eventlist, _remoteEndpoint);

    assert(_switch);

    peer->setRemoteEndpoint(this);
}


void
LosslessInputQueue::receivePacket(Packet& pkt) 
{
    /* normal packet, enqueue it */
    _queuesize += pkt.size();

    //send PAUSE notifications if that is the case!
    assert(_queuesize > 0);
    if ((uint64_t)_queuesize > _high_threshold && _state_recv!=PAUSED){
        _state_recv = PAUSED;
        if (_log_pfc_events) {
            cout << "PFC_PAUSE_SEND time_us " << timeAsUs(eventlist().now())
                 << " queue " << _name
                 << " queuesize " << _queuesize
                 << " high_threshold " << _high_threshold
                 << endl;
        }
        sendPause(1000);
    }

    //if (_state_recv==PAUSED)
    //cout << timeAsMs(eventlist().now()) << " queue " << _name << " switch (" << _switch->_name << ") "<< " recv when paused pkt " << pkt.type() << " sz " << _queuesize << endl;        

    if (_queuesize > _maxsize){
        static bool logged = false;
        if (!logged) {
            cout << " Queue " << _name
                 << " LOSSLESS not working! I should have dropped this packet"
                 << _queuesize / Packet::data_packet_size() << endl;
            logged = true;
        }
    }
    
    //tell the output queue we're here!
    if (pkt.nexthop() < pkt.route()->size()){
        //this should not work...
        //assert(0);
        pkt.sendOn2(this);
    }
    else {
        assert(_switch);
        pkt.set_ingress_queue(this);
        _switch->receivePacket(pkt);
    }
}

void LosslessInputQueue::completedService(Packet& pkt){
    _queuesize -= pkt.size();

    //unblock if that is the case
    assert(_queuesize >= 0);
    if ((uint64_t)_queuesize < _low_threshold && _state_recv == PAUSED) {
        _state_recv = READY;
        if (_log_pfc_events) {
            cout << "PFC_PAUSE_CLEAR time_us " << timeAsUs(eventlist().now())
                 << " queue " << _name
                 << " queuesize " << _queuesize
                 << " low_threshold " << _low_threshold
                 << endl;
        }
        sendPause(0);
    }
}

void LosslessInputQueue::sendPause(unsigned int wait){
    //cout << "Ingress link " << getRemoteEndpoint() << " PAUSE " << wait << endl;    
    uint32_t switchID = 0;
    if (_switch)
        switchID = getSwitch()->getID();

    EthPausePacket* pkt = EthPausePacket::newpkt(wait,switchID);
    if (wait > 0) {
        _pause_sent++;
        _pause_sent_by_q[this]++;
    } else {
        _pause_cleared++;
        _pause_cleared_by_q[this]++;
    }

    if (_wire)
        _wire->receivePacket(*pkt);
    else
        getRemoteEndpoint()->receivePacket(*pkt);
};
