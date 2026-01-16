// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        


#ifndef DCQCN_H
#define DCQCN_H

/*
 * A DCQCN source and sink
 */

#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>
//#include "util.h"
#include "math.h"
#include "config.h"
#include "network.h"
#include "rocepacket.h"
#include "cnppacket.h"
#include "queue.h"
#include "roce.h"
#include "uec_mp.h"
#include "eventlist.h"
#include "eth_pause_packet.h"
#include "trigger.h"
#include "ecn.h"
#define timeInf 0

class DCQCNSink;
class Switch;

class DCQCNSrc : public RoceSrc {
    friend class DCQCNSink;
public:
    DCQCNSrc(RoceLogger* logger,
             TrafficLogger* pktlogger,
             EventList &eventlist,
             linkspeed_bps rate,
             std::unique_ptr<UecMultipath> mp = nullptr);
    virtual ~DCQCNSrc();

    void addPath(Route* routeout, Route* routeback);
    Route* getPathRoute(uint16_t path_index);
    uint16_t selectPath();
    void processPathFeedback(uint16_t path_id, UecMultipath::PathFeedback feedback);

    virtual void receivePacket(Packet& pkt) override;
    virtual void processAck(const RoceAck& ack) override;
    virtual void processNack(const RoceNack& nack) override;
    virtual void processCNP(const CNPPacket& cnp);    
    virtual void increaseRate();
    virtual void doNextEvent();
    virtual bool isTraffic() override { return true; }
    void set_no_cc(bool enable);
    static void set_quiet(bool enable) { _quiet = enable; }
    static void set_log_rate(bool enable) { _log_rate = enable; }
    static void set_log_reps(bool enable) { _log_reps = enable; }
    static void set_log_finish(bool enable) { _log_finish = enable; }
    bool isDone() const { return _done; }
    linkspeed_bps pacing_rate() const { return _pacing_rate; }
    simtime_picosec packet_spacing() const { return _packet_spacing; }
    linkspeed_bps rc_rate() const { return _RC; }
    linkspeed_bps rt_rate() const { return _RT; }
    uint16_t T() const { return _T; }
    uint16_t BC() const { return _BC; }
    double alpha() const { return _alpha; }
    uint64_t flow_size_bytes() const { return _flow_size; }
    uint64_t sent_bytes() const { return _highest_sent * _mss; }
    uint64_t acked_bytes() const { return _last_acked * _mss; }

    // should really be private, but loggers want to see:
    uint32_t _cnps_received;    

    static simtime_picosec _cc_update_period;
    static double _alpha, _g;
    static uint32_t _F;
    static linkspeed_bps _RAI, _RHAI;
    static uint64_t _B;
    static bool _quiet;
    static bool _log_rate;
    static bool _log_reps;
    static bool _log_finish;

private:
    void send_packet() override;
    void log_rate(const char* reason);

    std::unique_ptr<UecMultipath> _mp;
    uint16_t _no_of_paths;
    uint16_t _last_path_id;
    std::vector<Route*> _paths_out;
    std::vector<Route*> _paths_back;
    std::unordered_map<RocePacket::seq_t, uint16_t> _pkt_path_map;

    simtime_picosec _last_cc_update, _last_alpha_update;
    linkspeed_bps _RC, _RT, _link;
    linkspeed_bps _min_rate;
    bool _no_cc;
    bool _reps_logged;
    
    enum increase_state {invalid = 0, fast_recovery=1,active_increase=2};
    //increase_state _ai_state;
    uint16_t _T,_BC;
    uint64_t _byte_counter, _old_highest_sent;

};

class DCQCNSink : public RoceSink, public EventSource {
    friend class DCQCNSrc;
public:
    DCQCNSink(EventList &eventlist);
    virtual void doNextEvent();

    virtual void receivePacket(Packet& pkt);  
    static simtime_picosec _cnp_interval;
    static uint64_t total_cnp_sent() { return _total_cnp_sent; }

    inline id_t get_id() const {return RoceSink::get_id();}

private:
    simtime_picosec _last_cnp_sent_time;
 
    uint32_t _marked_packets_since_last_cnp;
    uint32_t _packets_since_last_cnp;
    static uint64_t _total_cnp_sent;

    // Mechanism
    void send_cnp();
};

#endif
