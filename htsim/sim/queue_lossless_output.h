// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#ifndef _LOSSLESS_OUTPUT_QUEUE_H
#define _LOSSLESS_OUTPUT_QUEUE_H
/*
 * A FIFO queue that supports PAUSE frames and lossless operation
 */

#include <list>
#include <string>
#include "queue.h"
#include "config.h"
#include "eventlist.h"
#include "network.h"
#include "loggertypes.h"
#include "eth_pause_packet.h"
#include "ecn.h"

class LosslessOutputQueue : public Queue {
public:
    LosslessOutputQueue(linkspeed_bps bitrate, mem_b maxsize, EventList &eventlist, QueueLogger* logger);

    void receivePacket(Packet& pkt);
    void receivePacket(Packet& pkt,VirtualQueue* q);

    void beginService();
    void completeService();

    bool is_paused() { return _state_send == PAUSED || _state_send == PAUSE_RECEIVED;}

    enum queue_state {PAUSED,READY,PAUSE_RECEIVED};

    static void set_fg_flowid_threshold(flowid_t threshold) { _fg_flowid_threshold = threshold; }
    static void set_fg_util_sample_period(simtime_picosec period) { _fg_util_sample_period = period; }

private:
    list<VirtualQueue*> _vq;

    int _state_send;
    int _sending;
    uint64_t _txbytes;

public:
    static int _ecn_enabled;
    static int _K;
    static flowid_t _fg_flowid_threshold;
    static simtime_picosec _fg_util_sample_period;
    static bool _log_spine0_ecn;
    static std::string _spine0_ecn_substr;
};

#endif
