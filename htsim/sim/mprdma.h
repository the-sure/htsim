// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef MPRDMA_H
#define MPRDMA_H

#include <deque>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "config.h"
#include "queue.h"
#include "trigger.h"
#include "eventlist.h"
#include "eth_pause_packet.h"
#include "mprdmapacket.h"

class MPRdmaSink;
enum MPRdmaCqeStatus {
    CQE_SUCCESS = 0,
    CQE_NO_WQE = 1
};

class MPRdmaCqeHandler {
public:
    virtual ~MPRdmaCqeHandler() = default;
    virtual void on_cqe(flowid_t qp_id, MPRdmaPacket::psn_t msn, MPRdmaCqeStatus status) = 0;
};

class MPRdmaSrc : public BaseQueue, public TriggerTarget {
    friend class MPRdmaSink;
public:
    MPRdmaSrc(TrafficLogger* pktlogger, EventList& eventlist, linkspeed_bps rate);

    virtual void connect(Route* routeout, Route* routeback, MPRdmaSink& sink, simtime_picosec start_time);
    void set_dst(uint32_t dst) {_dstaddr = dst;}
    void set_traffic_logger(TrafficLogger* pktlogger) {_pktlogger = pktlogger;}

    void set_flowsize(uint64_t flow_size_in_bytes);
    void set_max_path_id(uint16_t max_path_id) {
        _max_path_id = max_path_id == 0 ? 1 : max_path_id;
        _path_sent.assign(_max_path_id, 0);
    }
    void set_delta(uint32_t delta) {_delta = delta;}
    void set_bitmap_len(uint32_t len) {_bitmap_len = len == 0 ? 1 : len;}
    void set_probe_p(double p) {_probe_p = p;}
    void set_alpha(double alpha) {_alpha = alpha;}
    void set_initial_window(uint32_t iw) {_initial_window = iw == 0 ? 1 : iw;}
    void set_burst_timeout(simtime_picosec timeout) {_burst_timeout = timeout;}
    void set_idle_timeout(simtime_picosec timeout) {_idle_timeout = timeout;}
    void set_enable_cc(bool enable) {_enable_cc = enable;}
    void set_enable_recovery(bool enable) {_enable_recovery = enable;}
    void set_vp_seed(uint32_t seed) {
        _vp_seed = seed;
        _vp_seed_set = true;
    }
    void set_rto_timeout(simtime_picosec timeout) {_rto_timeout = timeout;}
    void set_min_rto(simtime_picosec min_rto) {_min_rto = min_rto;}
    void set_log_fct(bool enable) {_log_fct = enable;}
    void set_is_background(bool is_background) {_is_background = is_background;}
    void set_next_sync() {_sync_next = true;}
    void set_message_bytes(uint64_t bytes) {_msg_bytes = bytes;}
    void set_recv_wq_depth(uint32_t depth) {_recv_wq_depth = depth;}
    enum MPRdmaVerb {VERB_SEND, VERB_WRITE, VERB_READ_RESP};
    void set_verb(MPRdmaVerb verb) {_verb = verb;}

    inline void set_flowid(flowid_t flow_id) { _flow.set_flowid(flow_id); }
    void set_end_trigger(Trigger& trigger) { _end_trigger = &trigger; }
    virtual void activate() { startflow(); }

    virtual void doNextEvent();
    virtual void receivePacket(Packet& pkt);

    virtual void processPause(const EthPausePacket& pkt);
    virtual void processAck(const MPRdmaAck& ack);
    virtual void processNack(const MPRdmaNack& nack);

    virtual mem_b queuesize() const { return 0; }
    virtual mem_b maxsize() const { return 0; }

    virtual const string& nodename() { return _nodename; }
    inline uint32_t flow_id() const { return _flow.flow_id(); }
    bool is_done() const { return _done; }

protected:
    enum {PAUSED, READY};
    void startflow();
    void send_new_packet(uint16_t vp_id);
    void send_retx_packet(uint16_t vp_id);
    void maybe_send_burst();
    void schedule_burst_timer();
    void maybe_restart_idle();
    bool has_data() const;
    uint16_t choose_probe_vp(uint16_t echo_vp, simtime_picosec now);
    uint32_t next_vp_rand();
    void update_rtt(MPRdmaPacket::psn_t psn, simtime_picosec now);
    simtime_picosec compute_rto() const;
    simtime_picosec oldest_inflight_time() const;
    void schedule_rto_timer();
    void reset_path_stats();
    void record_path(uint16_t vp_id);
    simtime_picosec compute_sync_delay() const;
    void schedule_sync_send(MPRdmaPacket* pkt, simtime_picosec send_time);
    void send_pending_syncs(simtime_picosec now);

    PacketFlow _flow;
    TrafficLogger* _pktlogger;
    Trigger* _end_trigger;

    string _nodename;
    uint32_t _dstaddr;
    uint32_t _node_num;
    uint32_t _state_send;
    Route* _route;
    MPRdmaSink* _sink;

    linkspeed_bps _bitrate;
    uint16_t _mss;

    uint64_t _flow_size;
    uint64_t _total_pkts;

    double _cwnd;
    int _inflate;
    MPRdmaPacket::psn_t _snd_una;
    MPRdmaPacket::psn_t _snd_nxt;
    MPRdmaPacket::psn_t _snd_ooh;

    bool _in_recovery;
    MPRdmaPacket::psn_t _snd_retx;
    MPRdmaPacket::psn_t _recovery_psn;

    uint32_t _delta;
    uint32_t _bitmap_len;
    uint16_t _max_path_id;
    double _probe_p;
    double _alpha;

    uint32_t _initial_window;
    simtime_picosec _burst_timeout;
    simtime_picosec _idle_timeout;
    bool _enable_cc;
    bool _enable_recovery;
    bool _vp_seed_set;
    uint32_t _vp_seed;
    simtime_picosec _rto_timeout;
    simtime_picosec _min_rto;
    uint32_t _rto_backoff;
    bool _rtx_pending;
    simtime_picosec _rtx_time;
    EventList::Handle _rtx_handle;
    bool _log_fct;
    bool _logged_done;
    simtime_picosec _start_time;
    bool _is_background;
    std::vector<uint64_t> _path_sent;
    uint64_t _path_total;
    uint32_t _path_unique;

    simtime_picosec _srtt;
    simtime_picosec _rttvar;
    MPRdmaPacket::psn_t _rtt_seq;

    simtime_picosec _last_send_time;
    uint16_t _last_ack_vp;
    simtime_picosec _next_probe_time;

    struct PendingSync {
        MPRdmaPacket* pkt;
        simtime_picosec send_time;
    };
    std::deque<PendingSync> _sync_queue;
    std::unordered_set<MPRdmaPacket::psn_t> _sync_psns;
    bool _sync_next;
    bool _sync_pending;
    simtime_picosec _sync_time;

    struct SentMeta {
        MPRdmaPacket::psn_t msn;
        MPRdmaPacket::psn_t ipsn;
        bool is_tail;
        bool needs_completion;
        uint32_t payload;
    };
    std::unordered_map<MPRdmaPacket::psn_t, SentMeta> _sent_meta;

    uint64_t _msg_bytes;
    uint64_t _bytes_sent;
    MPRdmaPacket::psn_t _msg_index;
    MPRdmaPacket::psn_t _msg_pkt_index;
    uint64_t _msg_bytes_remaining;
    MPRdmaVerb _verb;
    uint32_t _recv_wq_depth;

    bool _burst_pending;
    simtime_picosec _burst_time;
    EventList::Handle _burst_handle;

    std::map<MPRdmaPacket::psn_t, simtime_picosec> _inflight_times;

    static uint32_t _global_node_count;
    bool _flow_started;
    bool _done;
};

class MPRdmaSink : public PacketSink, public DataReceiver {
    friend class MPRdmaSrc;
public:
    MPRdmaSink();

    virtual void receivePacket(Packet& pkt);
    virtual uint64_t cumulative_ack() { return _rcv_nxt; }
    virtual uint32_t drops() { return _drops; }
    virtual const string& nodename() { return _nodename; }

    void set_src(uint32_t s) {_srcaddr = s;}
    void set_cqe_handler(MPRdmaCqeHandler* handler) {_cqe_handler = handler;}

protected:
    void connect(MPRdmaSrc& src, Route* route);
    void send_ack(const MPRdmaPacket& pkt, bool ecn_echo);
    void send_nack(const MPRdmaPacket& pkt, MPRdmaPacket::psn_t nack_psn);
    void advance_rcv_nxt();

    MPRdmaSrc* _src;
    const Route* _route;
    string _nodename;
    uint32_t _srcaddr;

    uint32_t _drops;
    MPRdmaPacket::psn_t _rcv_nxt;
    uint32_t _bitmap_len;
    uint32_t _head;
    std::vector<uint8_t> _bitmap;
    std::vector<MPRdmaPacket::psn_t> _bitmap_msn;
    uint32_t _recv_wq_head;
    uint64_t _cqe_count;
    uint32_t _recv_wq_depth;
    MPRdmaCqeHandler* _cqe_handler;

    enum SlotState : uint8_t {
        SLOT_EMPTY = 0,
        SLOT_RECEIVED = 1,
        SLOT_TAIL = 2,
        SLOT_TAIL_COMPLETION = 3
    };
};

#endif
