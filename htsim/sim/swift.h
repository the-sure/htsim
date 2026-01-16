// -*- c-basic-offset: 4; indent-tabs-mode: nil -*- 
#ifndef SWIFT_H
#define SWIFT_H

/*
 * A Swift source and sink, loosely based off of Tcp
 */

#include <list>
#include <set>
#include <memory>
#include "config.h"
#include "network.h"
#include "swiftpacket.h"
#include "swift_scheduler.h"
#include "eventlist.h"
#include "uec_mp.h"
#include "sent_packets.h"
#include "trigger.h"

//#define MODEL_RECEIVE_WINDOW 1

#define timeInf 0

class SwiftSink;
class SwiftSubflowSrc;
class SwiftSubflowSink;
class SwiftRtxTimerScanner;
class BaseScheduler;
class FlowEventLogger;

class SwiftPacer : public EventSource {
public:
    SwiftPacer(SwiftSubflowSrc& sub, EventList& eventlist);
    bool is_pending() const {return _interpacket_delay > 0;}  // are we pacing?
    void schedule_send(simtime_picosec delay);  // schedule a paced packet "delay" picoseconds after the last packet was sent
    void cancel();     // cancel pacing
    void just_sent();  // called when we've just sent a packet, even if it wasn't paced
    void doNextEvent();
private:
    SwiftSubflowSrc* _sub;
    simtime_picosec _interpacket_delay; // the interpacket delay, or zero if we're not pacing
    simtime_picosec _last_send;  // when the last packet was sent (always set, even when we're not pacing)
    simtime_picosec _next_send;  // when the next scheduled packet should be sent
};

// stuff that is specific to a subflow rather than the whole connection
class SwiftSubflowSrc : public EventSource, public PacketSink, public ScheduledSrc {
    friend class SwiftSrc;
    friend class SwiftLoggerSimple;
public:
    struct Stats {
        uint64_t packets_sent;
        uint64_t acks_received;
        uint64_t retransmits;
        uint64_t timeouts;
        simtime_picosec total_rtt;
        uint64_t rtt_samples;

        Stats()
            : packets_sent(0),
              acks_received(0),
              retransmits(0),
              timeouts(0),
              total_rtt(0),
              rtt_samples(0) {}
    };

    SwiftSubflowSrc(SwiftSrc& src, TrafficLogger* pktlogger, int subflow_id);
    virtual const string& nodename() { return _nodename; }
    void connect(SwiftSink& sink, const Route& routeout, const Route& routeback, uint32_t flow_id, BaseScheduler* scheduler);
    virtual void receivePacket(Packet& pkt);
    void update_rtt(simtime_picosec delay);
    void adjust_cwnd(simtime_picosec delay, SwiftAck::seq_t ackno);
    void applySwiftLimits();
    void handle_ack(SwiftAck::seq_t ackno);
    void move_path();
    void reroute(const Route &route);
    void doNextEvent();
    void rtx_timer_hook(simtime_picosec now, simtime_picosec period);
    inline simtime_picosec pacing_delay() const {return _pacing_delay;}
    PacketFlow& flow() {return _flow;}
    uint32_t drops() const { return _drops;}
    bool send_next_packet();
    virtual void send_callback();  // called by scheduler when it has more space
    const Stats& get_stats() const { return _stats; }
    uint32_t get_cwnd() const { return _swift_cwnd; }
    simtime_picosec get_rtt() const { return _rtt; }
    bool is_established() const { return _established; }
    void set_multipath(std::unique_ptr<UecMultipath> mp, uint16_t no_of_paths);
    uint16_t selectPath();
    void processPathFeedback(uint16_t path_id, UecMultipath::PathFeedback feedback);
protected:
    // connection state
    bool _established;

    uint64_t _highest_sent;  //seqno is in bytes

    //round trip time estimate
    simtime_picosec _rtt, _rto, _min_rto, _mdev;

    // stuff needed for reno-like fast recovery
    uint32_t _inflate; // how much we're currently extending cwnd based off dup ack arrivals.
    uint64_t _recoverq;
    bool _in_fast_recovery;

    // remember the default cwnd
    static uint32_t _default_cwnd;
    uint32_t _swift_cwnd;  // congestion window controlled by swift algorithm
    uint32_t _prev_cwnd;
    uint64_t _packets_sent;
    uint64_t _last_acked; // ack number of the last packet we received a cumulative ack for
    uint16_t _dupacks;
    uint32_t _retransmit_cnt;

    bool _can_decrease;  // limit backoff to once per RTT
    simtime_picosec _last_decrease; //when we last decreased
    simtime_picosec _pacing_delay;  // inter-packet pacing when cwnd < 1 pkt.
    map <SwiftPacket::seq_t, SwiftPacket::seq_t> _dsn_map;  // map of subflow seqno to data seqno

    // PLB stuff
    int _decrease_count;
    simtime_picosec _last_good_path;

    uint32_t _drops;
    simtime_picosec _RFC2988_RTO_timeout;
    bool _rtx_timeout_pending;

    // Connectivity
    PacketFlow _flow;
    uint32_t _path_index;
    SwiftSubflowSink* _subflow_sink;
    const Route* _route;
    SwiftSrc& _src;
    SwiftPacer _pacer;
    Stats _stats;

    std::unique_ptr<UecMultipath> _mp;
    uint16_t _no_of_paths;
    uint16_t _last_path_id;
    map<SwiftPacket::seq_t, uint16_t> _pkt_path_map;
    bool _reps_logged;
    vector<Route*> _mp_routes;

private:
    int send_packets();
    void retransmit_packet();
    inline EventList& eventlist() const;
    inline uint16_t mss() const;
    inline double ai() const;
    inline double beta() const;
    inline double max_mdf() const;
    bool _deferred_send;  // set if we tried to send and the scheduler said no.
    simtime_picosec _plb_interval;
    string _nodename;
    void init_multipath_routes();
};

class SwiftSrc : public EventSource, public TriggerTarget {
    friend class SwiftSink;
    friend class SwiftRtxTimerScanner;
    //friend class SwiftSubflowSrc;
public:
    SwiftSrc(SwiftRtxTimerScanner& rtx_scanner, SwiftLogger* logger, TrafficLogger* pktlogger, EventList &eventlist);
    void log(SwiftSubflowSrc* sub, SwiftLogger::SwiftEvent event);
    virtual void connect(const Route& routeout, const Route& routeback, 
                         SwiftSink& sink, simtime_picosec startTime);
    virtual void multipath_connect(SwiftSink& sink, simtime_picosec startTime, uint32_t no_of_subflows);
    void startflow();

    void doNextEvent();
    void update_dsn_ack(SwiftAck::seq_t ds_ackno);
    //virtual void receivePacket(Packet& pkt);

    void set_flowsize(uint64_t flow_size_in_bytes) {
        _flow_size = flow_size_in_bytes;
        cout << "Setting flow size to " << _flow_size << endl;
    }

    void set_flowid(flowid_t id) {
        _flow_id = id;
        for (auto* sub : _subs) {
            sub->flow().set_flowid(id);
        }
    }
    flowid_t get_flowid() const { return _flow_id; }
    void set_dst(int dst) { _dst = dst; }
    int get_dst() const { return _dst; }
    void set_end_trigger(Trigger& trigger) { _end_trigger = &trigger; }
    void set_start_trigger(Trigger& trigger) { _start_trigger = &trigger; }
    void logFlowEvents(FlowEventLogger& logger) { _flow_logger = &logger; }
    void set_base_delay(simtime_picosec delay) {
        _base_delay = delay;
        _h = _base_delay / 6.55;
        cout << "Swift flow " << _flow_id << " base_delay: " << timeAsUs(delay) << " us" << endl;
    }
    void set_ai(double ai) {
        _ai = ai;
        cout << "Swift flow " << _flow_id << " ai: " << ai << endl;
    }
    void set_beta(double beta) {
        _beta = beta;
        cout << "Swift flow " << _flow_id << " beta: " << beta << endl;
    }
    void set_max_mdf(double max_mdf) {
        _max_mdf = max_mdf;
        cout << "Swift flow " << _flow_id << " max_mdf: " << max_mdf << endl;
    }
    SwiftSubflowSrc* get_subflow() { return _subs.empty() ? nullptr : _subs.front(); }
    uint32_t get_cwnd() const { return _subs.empty() ? 0 : _subs.front()->get_cwnd(); }
    bool is_done() const;
    bool is_finished() const { return _finished; }
    void addPath(Route* routeout, Route* routeback);
    Route* getPathRoute(uint16_t path_index);
    Route* getPathBackRoute(uint16_t path_index);
    uint16_t get_no_of_paths() const { return _no_of_paths; }

    void set_stoptime(simtime_picosec stop_time) {
        _stop_time = stop_time;
        cout << "Setting stop time to " << timeAsSec(_stop_time) << endl;
    }

    bool more_data_available() const;

    SwiftPacket::seq_t get_next_dsn() {
        SwiftPacket::seq_t dsn = _highest_dsn_sent + 1;
        _highest_dsn_sent += mss();
        return dsn;
    }

    bool check_stoptime();
    
    void set_cwnd(uint32_t cwnd);

    void set_hdiv(double hdiv);

    // add paths for PLB
    void enable_plb() {_plb = true;}
    void set_paths(vector<const Route*>* rt);
    void permute_paths();
    bool _plb;
    inline bool plb() const {return _plb;}


    // should really be private, but loggers want to see:
    uint64_t _highest_dsn_sent;  //seqno is in bytes - data sequence number, for MPSwift
    uint64_t _flow_size;
    simtime_picosec _stop_time;
    bool _stopped;
    uint32_t _maxcwnd;
    int32_t _app_limited;
    uint32_t drops();

    uint16_t _mss;
    inline uint16_t mss() const {return _mss;}
    inline double ai() const {return _ai;}
    inline double beta() const {return _beta;}
    inline double max_mdf() const {return _max_mdf;}

    // state needed for swift congestion control
    double _ai;  // increase constant.  
    double _beta;   // decrease constant
    double _max_mdf; // max multiplicate decrease factor
    double _h;      // multi-hop scaling constant
    simtime_picosec _base_delay;    // configured base target delay
    uint32_t _rtx_reset_threshold; // constant
    uint32_t _min_cwnd;  // minimum cwnd we can use
    uint32_t _max_cwnd;  // maximum cwnd we can use

    // flow scaling constants
    double _fs_alpha;
    double _fs_beta;  // I think this beta is different from Algorithm 1 beta
    simtime_picosec _fs_range;
    double _fs_min_cwnd;  // note: in packets
    double _fs_max_cwnd;  // note: in packets

    // paths for PLB or MPSwift
    vector<const Route*> _paths;
    vector<Route*> _paths_out;
    vector<Route*> _paths_back;
    uint16_t _no_of_paths;

    SwiftSink* _sink;
    void set_app_limit(int pktps);
    virtual void activate() { startflow(); }


    // Swift helper functions
    simtime_picosec targetDelay(uint32_t cwnd, const Route& route);

    int queuesize(int flow_id);

private:
    flowid_t _flow_id;
    int _dst;
    Trigger* _end_trigger;
    Trigger* _start_trigger;
    FlowEventLogger* _flow_logger;
    // Housekeeping
    SwiftLogger* _logger;
    TrafficLogger* _traffic_logger;
    BaseScheduler* _scheduler;
    SwiftRtxTimerScanner* _rtx_timer_scanner;

    // Mechanism
    void clear_timer(uint64_t start,uint64_t end);

    // list of subflows
    vector<SwiftSubflowSrc*> _subs;
    bool _finished;

};

/**********************************************************************************/
/** SUBFLOW SINK                                                                 **/
/**********************************************************************************/

class SwiftSubflowSink : public PacketSink, public DataReceiver {
    friend class SwiftSrc;
    friend class SwiftSink;
public:
    SwiftSubflowSink(SwiftSink& sink);

    void receivePacket(Packet& pkt);
    SwiftAck::seq_t _cumulative_ack; // seqno of the last byte in the packet we have
    uint64_t cumulative_ack();
    virtual const string& nodename();

    // cumulatively acked
    uint64_t _packets;
    uint32_t _drops;
    uint32_t drops();

    list<SwiftAck::seq_t> _received; /* list of packets above a hole, that 
                                        we've received */
private:
    // Connectivity
    void connect(SwiftSubflowSrc& src, const Route& route);
    const Route* _route;

    // Mechanism
    void send_ack(simtime_picosec ts);

    SwiftSubflowSrc* _subflow_src;
    SwiftSink& _sink;
};

/**********************************************************************************/
/** SINK                                                                         **/
/**********************************************************************************/

class SwiftSink : public PacketSink, public DataReceiver {
    friend class SwiftSrc;
    friend class SwiftSubflowSrc;
public:
    SwiftSink();

    void add_buffer_logger(ReorderBufferLogger *logger) {
        _buffer_logger = logger;
    }

    void receivePacket(Packet& pkt);
    SwiftAck::seq_t _cumulative_data_ack; // seqno of the last DSN byte in the packet we have
                                          // cumulatively acked
    virtual const string& nodename() { return _nodename; }

    set <SwiftPacket::seq_t> _dsn_received; // use a map because multipath packets will arrive out of order

    void set_src_id(int src) { _src_id = src; }
    int get_src_id() const { return _src_id; }
    flowid_t get_flowid() const { return _src ? _src->get_flowid() : 0; }
    uint64_t get_cumulative_ack() const;
    uint64_t get_cumulative_data_ack() const { return _cumulative_data_ack; }
    uint64_t get_packets_received() const;

    SwiftSrc* _src;
    uint64_t cumulative_ack();
    uint32_t drops();

    vector <SwiftSubflowSink*> _subs; // public so the logger can see
private:
    // Connectivity, called by Src
    SwiftSubflowSink* connect(SwiftSrc& src, SwiftSubflowSrc&, const Route& route);
    string _nodename;
    ReorderBufferLogger* _buffer_logger;
    int _src_id;
};

class SwiftRtxTimerScanner : public EventSource {
public:
    SwiftRtxTimerScanner(simtime_picosec scanPeriod, EventList& eventlist);
    void doNextEvent();
    void registerSubflow(SwiftSubflowSrc &subflow_src);
private:
    simtime_picosec _scanPeriod;
    typedef list<SwiftSubflowSrc*> subflows_t;
    subflows_t _subflows;
};

#endif
