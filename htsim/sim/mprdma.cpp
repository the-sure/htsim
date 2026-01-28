// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "mprdma.h"
#include <algorithm>
#include <limits>
#include "logfile.h"
#include "ecn.h"

uint32_t MPRdmaSrc::_global_node_count = 0;

static inline uint32_t ceil_div(uint64_t v, uint32_t d) {
    return (uint32_t)((v + d - 1) / d);
}

MPRdmaSrc::MPRdmaSrc(TrafficLogger* pktlogger, EventList& eventlist, linkspeed_bps rate)
    : BaseQueue(rate, eventlist, nullptr),
      _flow(pktlogger),
      _pktlogger(pktlogger),
      _end_trigger(nullptr),
      _dstaddr(UINT32_MAX),
      _node_num(_global_node_count++),
      _bitrate(rate),
      _mss(Packet::data_packet_size()),
      _flow_size(0),
      _total_pkts(0),
      _cwnd(1.0),
      _inflate(0),
      _snd_una(0),
      _snd_nxt(0),
      _snd_ooh(0),
      _in_recovery(false),
      _snd_retx(0),
      _recovery_psn(0),
      _delta(32),
      _bitmap_len(64),
      _max_path_id(1),
      _probe_p(0.01),
      _alpha(1.0),
      _initial_window(1),
      _burst_timeout(timeFromUs((uint32_t)1)),
      _idle_timeout(timeFromUs((uint32_t)0)),
      _enable_cc(true),
      _enable_recovery(true),
      _vp_seed_set(false),
      _vp_seed(0),
      _rto_timeout(0),
      _min_rto(0),
      _rto_backoff(1),
      _rtx_pending(false),
      _rtx_time(0),
      _rtx_handle(EventList::nullHandle()),
      _log_fct(false),
      _logged_done(false),
      _start_time(0),
      _is_background(false),
      _path_total(0),
      _path_unique(0),
      _srtt(0),
      _rttvar(0),
      _rtt_seq(0),
      _last_send_time(0),
      _last_ack_vp(0),
      _next_probe_time(0),
      _sync_next(false),
      _sync_pending(false),
      _sync_time(0),
      _msg_bytes(0),
      _bytes_sent(0),
      _msg_index(0),
      _msg_pkt_index(0),
      _msg_bytes_remaining(0),
      _verb(VERB_WRITE),
      _recv_wq_depth(0),
      _burst_pending(false),
      _burst_time(0),
      _burst_handle(EventList::nullHandle()),
      _flow_started(false),
      _done(false) {
    _nodename = "mprdma_src " + to_string(_node_num);
    _state_send = READY;
    _route = nullptr;
    _sink = nullptr;
}

void MPRdmaSrc::connect(Route* routeout, Route* routeback, MPRdmaSink& sink, simtime_picosec start_time) {
    _route = routeout;
    _sink = &sink;
    _flow.set_id(get_id());
    _sink->connect(*this, routeback);

    if (start_time != TRIGGER_START) {
        eventlist().sourceIsPending(*this, start_time);
    }
}

void MPRdmaSrc::set_flowsize(uint64_t flow_size_in_bytes) {
    _flow_size = flow_size_in_bytes;
    if (_flow_size == 0) {
        _total_pkts = UINT32_MAX;
    } else {
        _total_pkts = ceil_div(_flow_size, _mss);
    }
}

void MPRdmaSrc::startflow() {
    if (_total_pkts == 0) {
        set_flowsize(_flow_size);
    }
    _cwnd = (double)_initial_window;
    _inflate = 0;
    _snd_una = 0;
    _snd_nxt = 0;
    _snd_ooh = 0;
    _in_recovery = false;
    _inflight_times.clear();
    _last_ack_vp = 0;
    _next_probe_time = 0;
    _sync_queue.clear();
    _sync_psns.clear();
    _sent_meta.clear();
    _rto_backoff = 1;
    _rtx_pending = false;
    _rtx_time = 0;
    _logged_done = false;
    _sync_next = false;
    _sync_pending = false;
    _sync_time = 0;
    _rtt_seq = 0;
    _bytes_sent = 0;
    _msg_index = 0;
    _msg_pkt_index = 0;
    if (_msg_bytes > 0 && _flow_size > 0 && _flow_size < _msg_bytes) {
        _msg_bytes_remaining = _flow_size;
    } else {
        _msg_bytes_remaining = _msg_bytes;
    }
    _start_time = eventlist().now();
    reset_path_stats();

    uint32_t send_iw = _initial_window;
    for (uint32_t i = 0; i < send_iw && has_data(); ++i) {
        send_new_packet(next_vp_rand() % _max_path_id);
    }
    schedule_rto_timer();
}

void MPRdmaSrc::doNextEvent() {
    if (!_flow_started) {
        _flow_started = true;
        startflow();
        return;
    }
    simtime_picosec now = eventlist().now();
    if (_sync_pending && now >= _sync_time) {
        send_pending_syncs(now);
    }
    if (_burst_pending && now >= _burst_time) {
        _burst_pending = false;
        maybe_send_burst();
    }
    if (_rtx_pending && now >= _rtx_time) {
        _rtx_pending = false;
        if (_enable_recovery && _snd_una < _snd_nxt) {
            _in_recovery = true;
            _recovery_psn = _snd_nxt;
            _snd_retx = _snd_una;
            if (_enable_cc) {
                _cwnd = std::max(1.0, _cwnd / 2.0);
            }
            send_retx_packet(_last_ack_vp);
            _rto_backoff = std::min(_rto_backoff * 2u, 64u);
            schedule_rto_timer();
        }
    }
}

void MPRdmaSrc::receivePacket(Packet& pkt) {
    switch (pkt.type()) {
    case MPRDMAACK:
        processAck((const MPRdmaAck&)pkt);
        pkt.free();
        break;
    case MPRDMANACK:
        processNack((const MPRdmaNack&)pkt);
        pkt.free();
        break;
    case ETH_PAUSE:
        processPause((const EthPausePacket&)pkt);
        pkt.free();
        break;
    default:
        abort();
    }
}

void MPRdmaSrc::processPause(const EthPausePacket& pkt) {
    if (pkt.sleepTime() > 0) {
        _state_send = PAUSED;
    } else {
        _state_send = READY;
        eventlist().sourceIsPendingRel(*this, 0);
    }
}

void MPRdmaSrc::update_rtt(MPRdmaPacket::psn_t psn, simtime_picosec now) {
    if (_rtt_seq != 0 && psn != _rtt_seq) {
        return;
    }
    auto it = _inflight_times.find(psn);
    if (it == _inflight_times.end()) {
        return;
    }
    simtime_picosec sample = now - it->second;
    if (_srtt == 0) {
        _srtt = sample;
        _rttvar = sample / 2;
    } else {
        simtime_picosec err = sample > _srtt ? sample - _srtt : _srtt - sample;
        _rttvar = (3 * _rttvar + err) / 4;
        _srtt = (7 * _srtt + sample) / 8;
    }
    _rtt_seq = 0;
}

void MPRdmaSrc::processAck(const MPRdmaAck& ack) {
    simtime_picosec now = eventlist().now();
    update_rtt(ack.sack_psn(), now);
    _last_ack_vp = ack.pathid() % _max_path_id;

    _inflate += 1;
    MPRdmaPacket::psn_t old_una = _snd_una;
    if (ack.aack_psn() + 1 > _snd_una) {
        _snd_una = ack.aack_psn() + 1;
    }
    if (_snd_una > old_una) {
        _inflate -= (int)(_snd_una - old_una);
        for (MPRdmaPacket::psn_t psn = old_una; psn < _snd_una; ++psn) {
            _inflight_times.erase(psn);
            _sync_psns.erase(psn);
            _sent_meta.erase(psn);
        }
        if (_rtt_seq < _snd_una) {
            _rtt_seq = 0;
        }
        _rto_backoff = 1;
    }

    if (ack.sack_psn() > _snd_ooh) {
        _snd_ooh = ack.sack_psn();
    }

    bool gated = false;
    if (_enable_cc) {
        if (!ack.ecn_echo()) {
            _cwnd += 1.0 / _cwnd;
        } else {
            _cwnd -= 0.5;
        }
        if (_cwnd < 1.0) {
            _cwnd = 1.0;
        }

        if (!ack.retx_echo()) {
            MPRdmaPacket::psn_t snd_ool = (_snd_ooh >= _delta) ? (_snd_ooh - _delta) : 0;
            if (ack.sack_psn() < snd_ool) {
                _cwnd -= 1.0;
                if (_cwnd < 1.0) {
                    _cwnd = 1.0;
                }
                gated = true;
            }
        }
    }

    if (_in_recovery) {
        send_retx_packet(ack.pathid());
        if (_snd_una >= _recovery_psn) {
            _in_recovery = false;
        }
        schedule_rto_timer();
        return;
    }

    if (gated) {
        schedule_rto_timer();
        return;
    }

    maybe_restart_idle();

    double awnd = _cwnd + _inflate - (double)(_snd_nxt - _snd_una);
    int send_cnt = 0;
    while (awnd >= 1.0 && has_data() && send_cnt < 2) {
        uint16_t vp = choose_probe_vp(ack.pathid(), now);
        send_new_packet(vp);
        awnd -= 1.0;
        send_cnt++;
    }

    if (awnd >= 1.0 && !has_data()) {
        if (_enable_recovery && !_inflight_times.empty()) {
            _snd_retx = _snd_una;
            send_retx_packet(ack.pathid());
        }
        if (_enable_cc) {
            _cwnd -= 1.0;
            if (_cwnd < 1.0) {
                _cwnd = 1.0;
            }
        }
    }

    if (awnd >= 1.0 && send_cnt >= 2) {
        schedule_burst_timer();
    }

    if (!_done && _flow_size > 0 && _snd_una >= _total_pkts) {
        _done = true;
        if (_end_trigger) {
            _end_trigger->activate();
        }
        if (_log_fct && !_logged_done) {
            simtime_picosec finish = eventlist().now();
            simtime_picosec fct = (finish > _start_time) ? (finish - _start_time) : 0;
            double seconds = timeAsSec(fct);
            double gbps = 0.0;
            if (seconds > 0.0) {
                gbps = (_flow_size * 8.0) / (seconds * 1e9);
            }
            cout << "MPRDMA_FCT " << (_is_background ? "BG" : "FG")
                 << " flowid " << _flow.flow_id()
                 << " start_us " << timeAsUs(_start_time)
                 << " finish_us " << timeAsUs(finish)
                 << " fct_us " << timeAsUs(fct)
                 << " throughput_gbps " << gbps
                 << endl;
            cout << "MPRDMA_PATHS " << (_is_background ? "BG" : "FG")
                 << " flowid " << _flow.flow_id()
                 << " paths_used " << _path_unique
                 << " max_paths " << _max_path_id
                 << " packets " << _path_total
                 << endl;
            _logged_done = true;
        }
    }
    schedule_rto_timer();
}

void MPRdmaSrc::processNack(const MPRdmaNack& nack) {
    if (!_enable_recovery) {
        return;
    }
    _in_recovery = true;
    _recovery_psn = _snd_nxt;
    _snd_retx = nack.nack_psn();
    send_retx_packet(nack.pathid());
}

bool MPRdmaSrc::has_data() const {
    return _snd_nxt < _total_pkts;
}

uint16_t MPRdmaSrc::choose_probe_vp(uint16_t echo_vp, simtime_picosec now) {
    if (_max_path_id <= 1) {
        return 0;
    }
    uint16_t candidate = echo_vp % _max_path_id;
    if (_probe_p > 0.0 && now >= _next_probe_time) {
        if (drand() < _probe_p) {
            candidate = next_vp_rand() % _max_path_id;
        }
        simtime_picosec base_rtt = _srtt;
        if (base_rtt == 0) {
            base_rtt = _burst_timeout > 0 ? (2 * _burst_timeout) : timeFromUs((uint32_t)1);
        }
        _next_probe_time = now + base_rtt;
    }
    return candidate;
}

simtime_picosec MPRdmaSrc::compute_rto() const {
    if (_rto_timeout > 0) {
        return _rto_timeout;
    }
    simtime_picosec base = _srtt;
    if (base == 0) {
        base = _burst_timeout > 0 ? (2 * _burst_timeout) : timeFromUs((uint32_t)1);
    }
    simtime_picosec rto = base + 4 * _rttvar;
    if (_min_rto > 0 && rto < _min_rto) {
        rto = _min_rto;
    }
    return rto;
}

simtime_picosec MPRdmaSrc::oldest_inflight_time() const {
    if (_inflight_times.empty()) {
        return 0;
    }
    simtime_picosec oldest = std::numeric_limits<simtime_picosec>::max();
    for (const auto& entry : _inflight_times) {
        if (entry.second < oldest) {
            oldest = entry.second;
        }
    }
    return oldest == std::numeric_limits<simtime_picosec>::max() ? 0 : oldest;
}

void MPRdmaSrc::schedule_rto_timer() {
    if (!_enable_recovery) {
        _rtx_pending = false;
        return;
    }
    if (_snd_una >= _snd_nxt || _inflight_times.empty()) {
        _rtx_pending = false;
        return;
    }
    simtime_picosec oldest = _inflight_times.count(_snd_una)
        ? _inflight_times.at(_snd_una)
        : oldest_inflight_time();
    if (oldest == 0) {
        _rtx_pending = false;
        return;
    }
    simtime_picosec rto = compute_rto();
    if (_rto_backoff > 1) {
        rto *= _rto_backoff;
    }
    _rtx_pending = true;
    _rtx_time = oldest + rto;
    _rtx_handle = eventlist().sourceIsPendingGetHandle(*this, _rtx_time);
}

void MPRdmaSrc::reset_path_stats() {
    if (_max_path_id == 0) {
        _max_path_id = 1;
    }
    _path_sent.assign(_max_path_id, 0);
    _path_total = 0;
    _path_unique = 0;
}

void MPRdmaSrc::record_path(uint16_t vp_id) {
    if (_max_path_id == 0) {
        return;
    }
    uint16_t idx = vp_id % _max_path_id;
    if (_path_sent.empty()) {
        _path_sent.assign(_max_path_id, 0);
    }
    if (_path_sent[idx] == 0) {
        _path_unique++;
    }
    _path_sent[idx] += 1;
    _path_total += 1;
}

uint32_t MPRdmaSrc::next_vp_rand() {
    if (!_vp_seed_set) {
        return static_cast<uint32_t>(rand());
    }
    uint32_t x = _vp_seed;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    _vp_seed = x;
    return x;
}

void MPRdmaSrc::send_new_packet(uint16_t vp_id) {
    if (_state_send == PAUSED) {
        return;
    }
    if (!has_data()) {
        return;
    }

    uint32_t payload = _mss;
    if (_flow_size > 0) {
        uint64_t remaining = _flow_size - (uint64_t)_snd_nxt * _mss;
        if (remaining < payload) {
            payload = (uint32_t)remaining;
        }
    }

    bool sync = _sync_next;
    _sync_next = false;

    bool needs_completion = (_verb == VERB_SEND || _verb == VERB_READ_RESP);
    bool is_tail = true;
    MPRdmaPacket::psn_t msn = 0;
    MPRdmaPacket::psn_t ipsn = _snd_nxt;
    if (_msg_bytes > 0) {
        if (_msg_bytes_remaining == 0) {
            uint64_t remaining_flow = _flow_size > 0 ? (_flow_size - _bytes_sent) : _msg_bytes;
            _msg_bytes_remaining = remaining_flow < _msg_bytes ? remaining_flow : _msg_bytes;
            _msg_pkt_index = 0;
        }
        msn = _msg_index;
        ipsn = _msg_pkt_index;
        if (payload < _msg_bytes_remaining) {
            is_tail = false;
            _msg_bytes_remaining -= payload;
            _msg_pkt_index++;
        } else {
            is_tail = true;
            _msg_bytes_remaining = 0;
            _msg_index++;
            _msg_pkt_index = 0;
        }
    } else {
        is_tail = true;
    }

    MPRdmaPacket* p = MPRdmaPacket::newpkt(_flow, *_route, _snd_nxt, payload, false, sync, _dstaddr);
    p->set_msn(msn);
    p->set_ipsn(ipsn);
    p->set_tail(is_tail, is_tail && needs_completion);
    p->set_pathid(vp_id);
    if (sync) {
        simtime_picosec send_time = eventlist().now() + compute_sync_delay();
        _sync_psns.insert(_snd_nxt);
        schedule_sync_send(p, send_time);
    } else {
        record_path(vp_id);
        p->set_ts(eventlist().now());
        p->sendOn();
        if (_pktlogger) {
            _flow.logTraffic(*p, *this, TrafficLogger::PKT_CREATE);
        }
        if (_inflight_times.find(_snd_nxt) == _inflight_times.end()) {
            _inflight_times.emplace(_snd_nxt, eventlist().now());
        }
        _last_send_time = eventlist().now();
        schedule_rto_timer();
    }

    _sent_meta.emplace(_snd_nxt,
                       SentMeta{msn, ipsn, is_tail, is_tail && needs_completion, payload});
    if (_rtt_seq == 0) {
        _rtt_seq = _snd_nxt;
    }
    _bytes_sent += payload;
    _snd_nxt++;
}

void MPRdmaSrc::send_retx_packet(uint16_t vp_id) {
    if (_state_send == PAUSED) {
        return;
    }
    if (_snd_retx < _snd_una) {
        _snd_retx = _snd_una;
    }
    if (_snd_retx >= _snd_nxt) {
        return;
    }

    uint32_t payload = _mss;
    if (_flow_size > 0) {
        uint64_t remaining = _flow_size - (uint64_t)_snd_retx * _mss;
        if (remaining < payload) {
            payload = (uint32_t)remaining;
        }
    }

    bool sync = (_sync_psns.find(_snd_retx) != _sync_psns.end());
    auto meta_it = _sent_meta.find(_snd_retx);
    MPRdmaPacket::psn_t msn = 0;
    MPRdmaPacket::psn_t ipsn = _snd_retx;
    bool is_tail = (_flow_size > 0 && _snd_retx + 1 == _total_pkts);
    bool needs_completion = false;
    if (meta_it != _sent_meta.end()) {
        msn = meta_it->second.msn;
        ipsn = meta_it->second.ipsn;
        is_tail = meta_it->second.is_tail;
        needs_completion = meta_it->second.needs_completion;
    }
    MPRdmaPacket::psn_t sent_psn = _snd_retx;
    MPRdmaPacket* p = MPRdmaPacket::newpkt(_flow, *_route, sent_psn, payload, true, sync, _dstaddr);
    p->set_msn(msn);
    p->set_ipsn(ipsn);
    p->set_tail(is_tail, is_tail && needs_completion);
    p->set_pathid(vp_id);
    record_path(vp_id);
    p->set_ts(eventlist().now());
    p->sendOn();

    if (_pktlogger) {
        _flow.logTraffic(*p, *this, TrafficLogger::PKT_CREATE);
    }

    _inflight_times[sent_psn] = eventlist().now();
    _snd_retx++;
    _last_send_time = eventlist().now();
    schedule_rto_timer();
}

void MPRdmaSrc::maybe_send_burst() {
    simtime_picosec now = eventlist().now();
    double awnd = _cwnd + _inflate - (double)(_snd_nxt - _snd_una);
    int send_cnt = 0;
    while (awnd >= 1.0 && has_data() && send_cnt < 2) {
        uint16_t vp = choose_probe_vp(_last_ack_vp, now);
        send_new_packet(vp);
        awnd -= 1.0;
        send_cnt++;
    }
}

void MPRdmaSrc::schedule_burst_timer() {
    if (_burst_pending) {
        return;
    }
    _burst_pending = true;
    _burst_time = eventlist().now() + _burst_timeout;
    _burst_handle = eventlist().sourceIsPendingGetHandle(*this, _burst_time);
}

void MPRdmaSrc::maybe_restart_idle() {
    if (_idle_timeout == 0 || _last_send_time == 0 || !_sync_queue.empty()) {
        return;
    }
    if (eventlist().now() - _last_send_time > _idle_timeout && _snd_nxt == _snd_una) {
        _cwnd = (double)_initial_window;
        _inflate = 0;
    }
}

simtime_picosec MPRdmaSrc::compute_sync_delay() const {
    simtime_picosec base_rtt = _srtt;
    if (base_rtt == 0) {
        base_rtt = _burst_timeout > 0 ? (2 * _burst_timeout) : timeFromUs((uint32_t)1);
    }
    double delay = _alpha * (double)_delta * ((double)base_rtt / _cwnd);
    if (delay < 0) {
        delay = 0;
    }
    return (simtime_picosec)delay;
}

void MPRdmaSrc::schedule_sync_send(MPRdmaPacket* pkt, simtime_picosec send_time) {
    PendingSync entry{pkt, send_time};
    if (_sync_queue.empty() || send_time >= _sync_queue.back().send_time) {
        _sync_queue.push_back(entry);
    } else {
        auto it = _sync_queue.begin();
        while (it != _sync_queue.end() && it->send_time <= send_time) {
            ++it;
        }
        _sync_queue.insert(it, entry);
    }
    if (!_sync_pending || send_time < _sync_time) {
        _sync_pending = true;
        _sync_time = send_time;
        eventlist().sourceIsPendingGetHandle(*this, _sync_time);
    }
}

void MPRdmaSrc::send_pending_syncs(simtime_picosec now) {
    while (!_sync_queue.empty() && _sync_queue.front().send_time <= now) {
        MPRdmaPacket* p = _sync_queue.front().pkt;
        _sync_queue.pop_front();
        p->set_ts(now);
        p->sendOn();
        if (_pktlogger) {
            _flow.logTraffic(*p, *this, TrafficLogger::PKT_CREATE);
        }
        record_path(static_cast<uint16_t>(p->pathid()));
        if (_inflight_times.find(p->psn()) == _inflight_times.end()) {
            _inflight_times.emplace(p->psn(), now);
        }
        _last_send_time = now;
        schedule_rto_timer();
    }
    if (_sync_queue.empty()) {
        _sync_pending = false;
        _sync_time = 0;
    } else {
        _sync_time = _sync_queue.front().send_time;
        eventlist().sourceIsPendingGetHandle(*this, _sync_time);
    }
}

MPRdmaSink::MPRdmaSink()
    : DataReceiver("mprdma_sink"),
      _src(nullptr),
      _route(nullptr),
      _nodename("mprdma_sink"),
      _srcaddr(UINT32_MAX),
      _drops(0),
      _rcv_nxt(0),
      _bitmap_len(64),
      _head(0),
      _bitmap(_bitmap_len, SLOT_EMPTY),
      _bitmap_msn(_bitmap_len, 0),
      _recv_wq_head(0),
      _cqe_count(0),
      _recv_wq_depth(0),
      _cqe_handler(nullptr) {}

void MPRdmaSink::connect(MPRdmaSrc& src, Route* route) {
    _src = &src;
    _route = route;
    _rcv_nxt = 0;
    _drops = 0;
    _bitmap_len = src._bitmap_len;
    _bitmap.assign(_bitmap_len, SLOT_EMPTY);
    _bitmap_msn.assign(_bitmap_len, 0);
    _head = 0;
    _recv_wq_head = 0;
    _cqe_count = 0;
    _recv_wq_depth = src._recv_wq_depth;
}

void MPRdmaSink::receivePacket(Packet& pkt) {
    assert(pkt.dst() == _src->_dstaddr);
    if (pkt.type() != MPRDMA) {
        abort();
    }

    MPRdmaPacket* p = (MPRdmaPacket*)(&pkt);
    MPRdmaPacket::psn_t psn = p->psn();
    bool ecn = (p->flags() & ECN_CE);
    MPRdmaPacket::psn_t old_rcv_nxt = _rcv_nxt;
    uint32_t old_head = _head;
    bool sync = p->synchronise();

    if (psn >= _rcv_nxt + _bitmap_len) {
        send_nack(*p, _rcv_nxt);
        pkt.free();
        _drops++;
        return;
    }

    if (psn >= _rcv_nxt) {
        uint32_t offset = (uint32_t)(psn - _rcv_nxt);
        uint32_t idx = (_head + offset) % _bitmap_len;
        if (p->is_tail()) {
            _bitmap[idx] = p->needs_completion() ? SLOT_TAIL_COMPLETION : SLOT_TAIL;
        } else {
            _bitmap[idx] = SLOT_RECEIVED;
        }
        _bitmap_msn[idx] = p->msn();
    }

    advance_rcv_nxt();
    if (sync && psn >= old_rcv_nxt && psn >= _rcv_nxt) {
        if (psn < old_rcv_nxt + _bitmap_len) {
            uint32_t offset = (uint32_t)(psn - old_rcv_nxt);
            uint32_t idx = (old_head + offset) % _bitmap_len;
            _bitmap[idx] = SLOT_EMPTY;
            _bitmap_msn[idx] = 0;
        }
        send_nack(*p, psn);
        pkt.free();
        return;
    }
    send_ack(*p, ecn);
    pkt.free();
}

void MPRdmaSink::advance_rcv_nxt() {
    while (true) {
        if (_bitmap[_head] == SLOT_EMPTY) {
            return;
        }
        uint32_t idx = _head;
        uint32_t count = 0;
        bool found_tail = false;
        bool needs_completion = false;
        MPRdmaPacket::psn_t tail_msn = 0;

        while (count < _bitmap_len) {
            uint8_t state = _bitmap[idx];
            if (state == SLOT_EMPTY) {
                return;
            }
            count++;
            if (state == SLOT_TAIL || state == SLOT_TAIL_COMPLETION) {
                found_tail = true;
                needs_completion = (state == SLOT_TAIL_COMPLETION);
                tail_msn = _bitmap_msn[idx];
                break;
            }
            idx = (idx + 1) % _bitmap_len;
        }

        if (!found_tail) {
            return;
        }

        for (uint32_t i = 0; i < count; ++i) {
            _bitmap[_head] = SLOT_EMPTY;
            _bitmap_msn[_head] = 0;
            _head = (_head + 1) % _bitmap_len;
            _rcv_nxt++;
        }

        if (needs_completion) {
            MPRdmaCqeStatus status = CQE_NO_WQE;
            if (_recv_wq_depth > 0) {
                _recv_wq_head = (_recv_wq_head + 1) % _recv_wq_depth;
                _cqe_count++;
                status = CQE_SUCCESS;
            }
            if (_cqe_handler) {
                _cqe_handler->on_cqe(_src->flow_id(), tail_msn, status);
            }
        }
    }
}

void MPRdmaSink::send_ack(const MPRdmaPacket& pkt, bool ecn_echo) {
    MPRdmaPacket::psn_t aack_psn = (_rcv_nxt == 0) ? 0 : _rcv_nxt - 1;
    MPRdmaAck* ack = MPRdmaAck::newpkt(_src->_flow, *_route,
                                       pkt.psn(), aack_psn,
                                       pkt.pathid(), ecn_echo,
                                       pkt.retransmitted(), pkt.synchronise(),
                                       _srcaddr);
    ack->set_ts(pkt.ts());
    ack->sendOn();
}

void MPRdmaSink::send_nack(const MPRdmaPacket& pkt, MPRdmaPacket::psn_t nack_psn) {
    MPRdmaNack* nack = MPRdmaNack::newpkt(_src->_flow, *_route, nack_psn, pkt.pathid(), _srcaddr);
    nack->set_ts(pkt.ts());
    nack->sendOn();
}
