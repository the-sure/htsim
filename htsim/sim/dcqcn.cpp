// -*- c-basic-offset: 4; indent-tabs-mode: nil -*- 
#include <math.h>
#include <iostream>
#include <algorithm>
#include "dcqcn.h"
#include "queue.h"
#include <stdio.h>
#include "switch.h"
#include "trigger.h"
using namespace std;

////////////////////////////////////////////////////////////////
//  DCQCN SOURCE
////////////////////////////////////////////////////////////////

/* keep track of RTOs.  Generally, we shouldn't see RTOs if
   return-to-sender is enabled.  Otherwise we'll see them with very
   large incasts. */
simtime_picosec DCQCNSink::_cnp_interval = timeFromUs(50.0);
simtime_picosec DCQCNSrc::_cc_update_period = timeFromUs(55.0);
uint64_t DCQCNSink::_total_cnp_sent = 0;

double DCQCNSrc::_alpha = 1;
double DCQCNSrc::_g = .00390625; // g = 1/256
uint64_t DCQCNSrc::_B = 10000000; // number of bytes to go until we fire the byte counter

uint32_t DCQCNSrc::_F = 5;
linkspeed_bps DCQCNSrc::_RAI = 0;
linkspeed_bps DCQCNSrc::_RHAI = 0;
bool DCQCNSrc::_quiet = false;

DCQCNSrc::DCQCNSrc(RoceLogger* logger, TrafficLogger* pktlogger, EventList &eventlist, linkspeed_bps rate)
    : RoceSrc(logger,pktlogger,eventlist,rate)
{
    _cnps_received = 0;

    //linkspeed
    _link = rate;
    //current transmit rate
    _RC = rate;
    //target transmit rate
    _RT = rate;

    _RAI = rate / 20;
    _RHAI = rate / 10;
    _min_rate = std::max<linkspeed_bps>(rate / 1000, 1);

    _last_cc_update = 0;
    _last_alpha_update = 0;

    _T = 0;
    _BC = 0;
    _byte_counter = 0;
    _old_highest_sent = 0;
    _no_cc = false;
}

void DCQCNSrc::set_no_cc(bool enable){
    _no_cc = enable;
    if (_no_cc) {
        _RC = _link;
        _RT = _link;
        _pacing_rate = _link;
        update_spacing();
    }
}

void DCQCNSrc::processCNP(const CNPPacket& cnp){
    if (_no_cc) {
        return;
    }
    _RT = _RC;
    _RC = _RC * (1-_alpha/2);
    if (_RC < _min_rate) {
        _RC = _min_rate;
    }
    if (_RT < _min_rate) {
        _RT = _min_rate;
    }
    _alpha = (1-_g)*_alpha + _g;

    //_ai_state = increase_state::fast_recovery;

    _T = 0;
    _BC = 0;
    _byte_counter = 0;
    _old_highest_sent = _highest_sent;

    _pacing_rate = _RC;
    update_spacing();

    _last_cc_update = eventlist().now();
    _last_alpha_update = eventlist().now();

    eventlist().sourceIsPendingRel(*this, _cc_update_period);
}

void DCQCNSrc::processAck(const RoceAck& ack) {
    RoceSrc::processAck(ack);
    if (_done && !_quiet) {
        cout << "DCQCN_FINISH flowid " << _flow.flow_id()
             << " time_ps " << eventlist().now()
             << " time_ns " << timeAsNs(eventlist().now())
             << endl;
    }
}

void DCQCNSrc::increaseRate(){
    if (_RC>= _link) {
        //no need to increase, already at line rate!
        return;
    }

    if (max(_T,_BC) <= _F){
        //fast recovery
        _RC = (_RT + _RC) / 2;

    } else if (min(_T,_BC) > _F){
        //hyper increase
        _RT = _RT + (min(_T,_BC)-_F)* _RHAI;
        _RC = (_RT + _RC) / 2;

    } else {
        //active increase 
        _RT += _RAI;
        _RC = (_RT + _RC) / 2;

    }

    if (_RC > _link){
        _RC = _link;
    }
    if (_RC < _min_rate) {
        _RC = _min_rate;
    }
    if (_RT < _min_rate) {
        _RT = _min_rate;
    }

    _pacing_rate = _RC;
    update_spacing();

    // Silence verbose DCQCN rate updates to keep logs manageable.
}

void DCQCNSrc::doNextEvent(){
    if (!_flow_started) {
        // Quiet startflow: avoid heavy stdout logging for large runs.
        _flow_started = true;
        _highest_sent = 0;
        _last_acked = 0;
        _acked_packets = 0;
        _packets_sent = 0;
        _done = false;
        if (_flow_logger) {
            _flow_logger->logEvent(_flow, *this, FlowEventLogger::START, _flow_size, 0);
        }
        eventlist().sourceIsPendingRel(*this, 0);
        return;
    }
    bool reschedule = false;

    RoceSrc::doNextEvent();

    if (_done) {
        return;
    }
    if (_no_cc) {
        return;
    }

    _byte_counter += (_highest_sent - _old_highest_sent) * _mss;
    _old_highest_sent = _highest_sent;

    if (_byte_counter >= _B){
        _byte_counter = 0;
        _BC++;

        increaseRate();
        reschedule = true;
    }

    if (eventlist().now()-_last_alpha_update >= _cc_update_period){
        _alpha = (1-_g)*_alpha;
        _last_alpha_update = eventlist().now();
        reschedule = true;
    }    


    if (eventlist().now()-_last_cc_update >= _cc_update_period){
        _last_cc_update = eventlist().now();
        _T ++;

        increaseRate();
        reschedule = true;
    }

    if (reschedule)
        eventlist().sourceIsPendingRel(*this, _cc_update_period);
}

void DCQCNSrc::receivePacket(Packet& pkt) 
{
    if (!_flow_started){
        assert(pkt.type()==ETH_PAUSE);
        return; 
    }

    if (_stop_time && eventlist().now() >= _stop_time) {
        // stop sending new data, but allow us to finish any retransmissions
        _flow_size = _highest_sent+_mss;
        _stop_time = 0;
    }

    if (_done)
        return;

    switch (pkt.type()) {
    case ETH_PAUSE:    
        processPause((const EthPausePacket&)pkt);
        pkt.free();
        return;
    case ROCENACK: 
        _nacks_received++;
        processNack((const RoceNack&)pkt);
        pkt.free();
        return;
    case ROCEACK:
        _acks_received++;
        processAck((const RoceAck&)pkt);
        pkt.free();
        return;
    case CNP:
        _cnps_received++;
        processCNP((const CNPPacket&)pkt);
        pkt.free();
        return;

    default:
        abort();
    }
}

////////////////////////////////////////////////////////////////
//  DCQCN SINK
////////////////////////////////////////////////////////////////

/* Only use this constructor when there is only one for to this receiver */
DCQCNSink::DCQCNSink(EventList &eventlist)
    : RoceSink(), EventSource(eventlist, "DCQCN Sink")
{
    _last_cnp_sent_time = UINT64_MAX;
    _marked_packets_since_last_cnp = 0;
    _packets_since_last_cnp = 0;
}

// Receive a packet.
// Note: _cumulative_ack is the last byte we've ACKed.
// seqno is the first byte of the new packet.
void DCQCNSink::receivePacket(Packet& pkt) {
    // Cache the reverse route for CNPs on first data packet.
    if (_route == nullptr && pkt.reverse_route() != nullptr) {
        _route = pkt.reverse_route();
    }
    bool ecn_marked = ((pkt.flags() & ECN_CE) != 0);
    RoceSink::receivePacket(pkt);

    if (ecn_marked){
        //generate CNPs here.
        if (_last_cnp_sent_time == UINT64_MAX || eventlist().now() - _last_cnp_sent_time >= _cnp_interval){
            send_cnp();
            eventlist().sourceIsPendingRel(*this,_cnp_interval);
        }               
        else {
            _marked_packets_since_last_cnp++;
        }
    }
    _packets_since_last_cnp++;
}

void DCQCNSink::doNextEvent(){
    if (eventlist().now() - _last_cnp_sent_time >= _cnp_interval && _marked_packets_since_last_cnp >0){
        send_cnp();
        eventlist().sourceIsPendingRel(*this,_cnp_interval);
    }
}

void DCQCNSink::send_cnp() {
    CNPPacket *cnp = 0;
    cnp = CNPPacket::newpkt(_src->_flow, *_route, _cumulative_ack,_srcaddr);
    cnp->set_pathid(0);

    cnp->sendOn();
    _total_cnp_sent++;

    _last_cnp_sent_time = eventlist().now();
    _packets_since_last_cnp = 0;
    _marked_packets_since_last_cnp = 0;
}
