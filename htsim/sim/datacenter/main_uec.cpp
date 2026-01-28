// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
//#include "config.h"
#include <cassert>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <unordered_map>
#include <memory>
#include <sstream>
#include <streambuf>
#include <string>
#include <string.h>

#include <math.h>
#include <unistd.h>
#include "network.h"
#include "pipe.h"
#include "eventlist.h"
#include "logfile.h"
#include "uec_logger.h"
#include "clock.h"
#include "uec_base.h"
#include "uec.h"
#include "uec_mp.h"
#include "uec_pdcses.h"
#include "swift.h"
#include "compositequeue.h"
#include "queue.h"
#include "topology.h"
#include "connection_matrix.h"
#include "pciemodel.h"
#include "oversubscribed_cc.h"
#include "queue_lossless_input.h"
#include "queue_lossless_output.h"
#include "mprdma.h"

// ===== New：DCQCN 头文件 =====
#include "dcqcn.h"
#include "cnppacket.h"
// ================================

#include "fat_tree_topology.h"
#include "fat_tree_switch.h"

#include <list>

// Simulation params

//#define PRINTPATHS 1

#include "main.h"

int DEFAULT_NODES = 128;
uint32_t DEFAULT_TRIMMING_QUEUESIZE_FACTOR = 2;
uint32_t DEFAULT_NONTRIMMING_QUEUESIZE_FACTOR = 5;
// #define DEFAULT_CWND 50

EventList eventlist;

enum ForegroundCCType { FG_NSCC, FG_DCQCN, FG_SWIFT, FG_MCC_IDEAL, FG_MCC_INCAST, FG_PFC_ONLY, FG_MPRDMA };

static const char* fg_cc_name(ForegroundCCType type) {
    switch (type) {
    case FG_NSCC:
        return "NSCC";
    case FG_DCQCN:
        return "DCQCN";
    case FG_SWIFT:
        return "Swift";
    case FG_MCC_IDEAL:
        return "MCC-Ideal";
    case FG_MCC_INCAST:
        return "MCC-Incast";
    case FG_PFC_ONLY:
        return "PFC-only";
    case FG_MPRDMA:
        return "MPRDMA";
    default:
        return "Unknown";
    }
}

class DebugOutputFilterBuf : public std::streambuf {
public:
    explicit DebugOutputFilterBuf(std::streambuf* dest) : _dest(dest) {}

protected:
    int overflow(int ch) override {
        if (ch == EOF) {
            return _dest->sputc(ch);
        }
        char c = static_cast<char>(ch);
        _line.push_back(c);
        if (c == '\n') {
            flushLine();
        }
        return ch;
    }

    std::streamsize xsputn(const char* s, std::streamsize count) override {
        for (std::streamsize i = 0; i < count; ++i) {
            overflow(static_cast<unsigned char>(s[i]));
        }
        return count;
    }

private:
    void flushLine() {
        if (!shouldFilter(_line)) {
            _dest->sputn(_line.data(), _line.size());
        }
        _line.clear();
    }

    bool shouldFilter(const std::string& line) const {
        if (line.rfind("DEBUG_", 0) == 0) {
            return true;
        }
        return line.find("mcc_ideal_update") != std::string::npos;
    }

    std::streambuf* _dest;
    std::string _line;
};

class FlowEventLoggerFct : public FlowEventLogger {
public:
    FlowEventLoggerFct(Logfile& logfile, EventList& eventlist, FlowEventLoggerSimple* raw_logger,
                       uint64_t bg_threshold)
        : _logfile(logfile),
          _eventlist(eventlist),
          _raw_logger(raw_logger),
          _bg_threshold(bg_threshold) {}

    void logEvent(PacketFlow& flow, Logged& location, FlowEvent ev, mem_b bytes, uint64_t pkts) override {
        if (_raw_logger) {
            _raw_logger->logEvent(flow, location, ev, bytes, pkts);
        }
        flowid_t fid = flow.flow_id();
        if (ev == FlowEventLogger::START) {
            _starts[fid] = _eventlist.now();
            return;
        }
        if (ev == FlowEventLogger::FINISH) {
            simtime_picosec finish = _eventlist.now();
            simtime_picosec start = 0;
            auto it = _starts.find(fid);
            if (it != _starts.end()) {
                start = it->second;
            }
            simtime_picosec fct = finish - start;
            stringstream ss;
            ss << "FCT flowid " << fid
               << " start_us " << timeAsUs(start)
               << " finish_us " << timeAsUs(finish)
               << " fct_us " << timeAsUs(fct);
            _logfile.write(ss.str());
            double seconds = timeAsSec(fct);
            double gbps = 0.0;
            if (seconds > 0.0 && bytes > 0) {
                gbps = (bytes * 8.0) / (seconds * 1e9);
            }
            cout << "UEC_FCT " << (fid > _bg_threshold ? "BG" : "FG")
                 << " flowid " << fid
                 << " start_us " << timeAsUs(start)
                 << " finish_us " << timeAsUs(finish)
                 << " fct_us " << timeAsUs(fct)
                 << " throughput_gbps " << gbps
                 << endl;
        }
    }

private:
    Logfile& _logfile;
    EventList& _eventlist;
    FlowEventLoggerSimple* _raw_logger;
    std::unordered_map<flowid_t, simtime_picosec> _starts;
    uint64_t _bg_threshold;
};

void exit_error(char* progr) {
    cout << "Usage " << progr << " [-nodes N]\n\t[-cwnd cwnd_size]\n\t[-pfc_only_cwnd pkts]\n\t[-q queue_size]\n\t[-queue_type composite|random|lossless|lossless_input|]\n\t[-tm traffic_matrix_file]\n\t[-strat route_strategy (single,rand,perm,pull,ecmp,\n\tecmp_host path_count,ecmp_ar,ecmp_rr,\n\tecmp_host_ar ar_thresh)]\n\t[-log log_level]\n\t[-seed random_seed]\n\t[-end end_time_in_usec]\n\t[-mtu MTU]\n\t[-hop_latency x] per hop wire latency in us,default 1\n\t[-target_q_delay x] target_queuing_delay in us, default is 6us \n\t[-switch_latency x] switching latency in us, default 0\n\t[-host_queue_type  swift|prio|fair_prio]\n\t[-logtime dt] sample time for sinklogger, etc\n\t[-conn_reuse] enable connection reuse\n\t[-quiet] suppress per-flow finish logs\n\t[-verbose] keep per-flow logs even for large runs\n\t[-no_ecn] disable ECN marking\n\t[-lossless_ecn_enable] force ECN on lossless output queues\n\t[-dcqcn_no_cc] DCQCN flows ignore CNP (PFC only)\n\t[-dcqcn_single_path] force DCQCN single path\n\t[-dcqcn_ar single|bitmap|reps|reps_legacy|oblivious|mixed]\n\t[-pfc_thresholds low high]\n\t[-min_rto us] minimum RTO in us\n\t[-ar_granularity packet|flow]\n\t[-fg_ar_granularity packet|flow]\n\t[-bg_ar_granularity packet|flow]\n\t[-fg_path|-fg_paths N] foreground paths (ECMP)\n\t[-bg_path|-bg_paths N] background paths (ECMP)\n\t[-ar_method pause|queue|bandwidth|pqb|pq|pb|qb]\n\t[-fg_cc nscc|dcqcn|swift|mcc|mprdma|pfc]\n\t[-mprdma_L bitmap_len]\n\t[-mprdma_delta delta]\n\t[-mprdma_alpha alpha]\n\t[-mprdma_probe p]\n\t[-mprdma_iw iw]\n\t[-mprdma_burst_us t]\n\t[-mprdma_idle_us t]\n\t[-mprdma_msg_bytes bytes]\n\t[-mprdma_verb send|write|read_resp]\n\t[-mprdma_recv_wq depth]\n\t[-mprdma_enable_pathselection 0|1]\n\t[-mprdma_enable_cc 0|1]\n\t[-mprdma_enable_recovery 0|1]\n\t[-mprdma_vp_seed seed]\n\t[-mprdma_timeout sec]\n\t[-TIMEOUT sec]\n\t[-mprdma_min_rto_us us]\n\t[-mcc_rtt_thresh us]\n\t[-mcc_r1 val]\n\t[-mcc_r2 val]\n\t[-mcc_r3 val]\n\t[-bg_threshold N] flowid > N is background (PFC only)\n\t[-fg_ar ecmp|adaptive|mixed]\n\t[-bg_ar ecmp|adaptive]\n"<< endl;
    exit(1);
}

class MPRdmaCqeTrigger : public MPRdmaCqeHandler {
public:
    explicit MPRdmaCqeTrigger(Trigger* trigger) : _trigger(trigger) {}
    void on_cqe(flowid_t qp_id, MPRdmaPacket::psn_t msn, MPRdmaCqeStatus status) override {
        (void)qp_id;
        (void)msn;
        if (_trigger && status == CQE_SUCCESS) {
            _trigger->activate();
        }
    }
private:
    Trigger* _trigger;
};

simtime_picosec calculate_rtt(FatTreeTopologyCfg* t_cfg, linkspeed_bps host_linkspeed) { 
    /*
    Using the host linkspeed here is not very accurate, but hopefully good enough for this usecase.
    */
    simtime_picosec rtt = 2 * t_cfg->get_diameter_latency() 
                + (Packet::data_packet_size() * 8 / speedAsGbps(host_linkspeed) * t_cfg->get_diameter() * 1000) 
                + (UecBasePacket::get_ack_size() * 8 / speedAsGbps(host_linkspeed) * t_cfg->get_diameter() * 1000);
    
    return rtt;
};

uint32_t calculate_bdp_pkt(FatTreeTopologyCfg* t_cfg, linkspeed_bps host_linkspeed) {
    simtime_picosec rtt = calculate_rtt(t_cfg, host_linkspeed);
    uint32_t bdp_pkt = ceil((timeAsSec(rtt) * (host_linkspeed/8)) / (double)Packet::data_packet_size()); 

    return bdp_pkt;
}

int main(int argc, char **argv) {
    DebugOutputFilterBuf debug_filter(std::cout.rdbuf());
    std::cout.rdbuf(&debug_filter);
    Clock c(timeFromSec(5 / 100.), eventlist);
    bool param_queuesize_set = false;
    uint32_t queuesize_pkt = 0;
    linkspeed_bps linkspeed = speedFromMbps((double)HOST_NIC);
    int packet_size = 4150;
    uint32_t path_entropy_size = 64;
    uint32_t fg_path_entropy_size = 0;
    uint32_t bg_path_entropy_size = 0;
    uint64_t cwnd = 0;
    uint64_t pfc_only_cwnd_pkts = 0;
    uint32_t no_of_nodes = 0;
    uint32_t tiers = 3; // we support 2 and 3 tier fattrees
    uint32_t planes = 1;  // multi-plane topologies
    uint32_t ports = 1;  // ports per NIC
    bool disable_trim = false; // Disable trimming, drop instead
    uint16_t trimsize = 64; // size of a trimmed packet
    simtime_picosec logtime = timeFromMs(0.25); // ms;
    stringstream filename(ios_base::out);
    simtime_picosec hop_latency = timeFromUs((uint32_t)1);
    simtime_picosec switch_latency = timeFromUs((uint32_t)0);
    // Default to lossless input queues so PFC is enabled unless explicitly overridden.
    queue_type qt = LOSSLESS_INPUT;

    // ===== New：前后台混合拥塞控制变量 =====
    ForegroundCCType fg_cc_type = FG_MCC_IDEAL;  // 观测流量默认使用 MCC
    uint64_t bg_flowid_threshold = 1000;    // flowid > threshold 为背景流
    bool dcqcn_no_cc = false;
    bool dcqcn_single_path = false;
    // ===================================

    enum LoadBalancing_Algo { BITMAP, REPS, REPS_LEGACY, OBLIVIOUS, MIXED};
    LoadBalancing_Algo load_balancing_algo = MIXED;
    bool dcqcn_ar_override = false;
    LoadBalancing_Algo dcqcn_ar_algo = MIXED;

    bool log_sink = false;
    bool log_nic = false;
    bool log_flow_events = true;

    bool mprdma_enable_pathselection = true;
    bool mprdma_enable_cc = true;
    bool mprdma_enable_recovery = true;
    uint32_t mprdma_L = 64;
    uint32_t mprdma_delta = 32;
    double mprdma_alpha = 1.0;
    double mprdma_probe = 0.01;
    uint32_t mprdma_iw = 1;
    double mprdma_burst_us = 0.5;
    double mprdma_idle_us = 3.0;
    double mprdma_timeout_sec = 0.0;
    uint32_t mprdma_min_rto_us = 0;
    uint64_t mprdma_msg_bytes = 0;
    string mprdma_verb = "write";
    uint32_t mprdma_recv_wq = 0;
    bool mprdma_vp_seed_set = false;
    uint32_t mprdma_vp_seed = 0;

    bool log_tor_downqueue = false;
    bool log_tor_upqueue = false;
    bool log_traffic = false;
    bool log_switches = false;
    bool log_queue_usage = false;
    const double ecn_thresh = 0.5; // default marking threshold for ECN load balancing
    simtime_picosec target_Qdelay = 0;

    bool param_ecn_set = false;
    bool ecn = true;
    uint32_t ecn_low = 0;
    uint32_t ecn_high = 0;
    uint32_t queue_size_bdp_factor = 0;
    uint32_t topo_num_failed = 0;

    bool receiver_driven = false;
    bool sender_driven = true;
    // Default foreground congestion control to MCC unless overridden.
    UecSrc::_sender_cc_algo = UecSrc::MCC_IDEAL;
    UecSrc::_sender_based_cc = true;
    uint64_t high_pfc = 150, low_pfc = 120;
    bool param_pfc_set = false;
    bool force_lossless_ecn = false;

    RouteStrategy route_strategy = NOT_SET;
    
    int seed = 13;
    int i = 1;
    double pcie_rate = 1.1;

    filename << "logout.dat";
    int end_time = 0;  // in microseconds, 0 means no end limit
    bool end_time_set = false;
    bool force_disable_oversubscribed_cc = false;
    bool enable_accurate_base_rtt = false;

    //unsure how to set this. 
    queue_type snd_type = FAIR_PRIO;

    float ar_sticky_delta = 10;
    FatTreeSwitch::sticky_choices ar_sticky = FatTreeSwitch::PER_PACKET;
    FatTreeSwitch::sticky_choices fg_ar_sticky = FatTreeSwitch::PER_PACKET;
    FatTreeSwitch::sticky_choices bg_ar_sticky = FatTreeSwitch::PER_PACKET;
    bool separate_ar_granularity = false;

    char* tm_file = NULL;
    char* topo_file = NULL;
    int8_t qa_gate = -1;
    bool conn_reuse = false;
    bool force_verbose = false;
    simtime_picosec mcc_rtt_threshold = MccIdealParams::rtt_threshold;
    double mcc_r1 = MccIdealParams::R1;
    double mcc_r2 = MccIdealParams::R2;
    double mcc_r3 = MccIdealParams::R3;
    bool mcc_params_set = false;
    uint32_t user_min_rto_us = 0;  // user-specified min RTO in microseconds, 0 means auto
    // MCC-Incast parameters
    simtime_picosec mcc_incast_rtt_threshold = MccIncastParams::rtt_threshold;
    double mcc_incast_r1 = MccIncastParams::R1;
    double mcc_incast_r2 = MccIncastParams::R2;
    double mcc_incast_r3 = MccIncastParams::R3;
    bool mcc_incast_params_set = false;

    while (i<argc) {
        if (!strcmp(argv[i],"-o")) {
            filename.str(std::string());
            filename << argv[i+1];
            i++;
        } else if (!strcmp(argv[i],"-conn_reuse")){
            conn_reuse = true;
            cout << "Enabling connection reuse" << endl;
        } else if (!strcmp(argv[i],"-verbose")) {
            force_verbose = true;
            cout << "Verbose per-flow logging enabled" << endl;
        } else if (!strcmp(argv[i],"-end")) {
            end_time = atoi(argv[i+1]);
            end_time_set = true;
            cout << "endtime(us) "<< end_time << endl;
            i++;            
        } else if (!strcmp(argv[i],"-nodes")) {
            no_of_nodes = atoi(argv[i+1]);
            cout << "no_of_nodes "<<no_of_nodes << endl;
            i++;
        } else if (!strcmp(argv[i],"-tiers")) {
            tiers = atoi(argv[i+1]);
            cout << "tiers " << tiers << endl;
            assert(tiers == 2 || tiers == 3);
            i++;
        } else if (!strcmp(argv[i],"-planes")) {
            planes = atoi(argv[i+1]);
            ports = planes;
            cout << "planes " << planes << endl;
            cout << "ports per NIC " << ports << endl;
            assert(planes >= 1 && planes <= 8);
            i++;
        } else if (!strcmp(argv[i],"-receiver_cc_only")) {
            UecSrc::_sender_based_cc = false;
            UecSrc::_receiver_based_cc = true;
            UecSink::_oversubscribed_cc = false;
            sender_driven = false;
            receiver_driven = true;
            cout << "receiver based CC enabled ONLY" << endl;
//        } else if (!strcmp(argv[i],"-disable_fd")) {
//            disable_fair_decrease = true;
//            cout << "fair_decrease disabled" << endl;
        } else if (!strcmp(argv[i],"-sender_cc_only")) {
            UecSrc::_sender_based_cc = true;
            UecSrc::_receiver_based_cc = false;
            UecSink::_oversubscribed_cc = false;
            sender_driven = true;
            receiver_driven = false;
            cout << "sender based CC enabled ONLY" << endl;
        } else if (!strcmp(argv[i],"-qa_gate")) {
            qa_gate = atof(argv[i+1]);
            cout << "qa_gate 2^" << qa_gate << endl;
            i++;
        } else if (!strcmp(argv[i],"-target_q_delay")) {
            target_Qdelay = timeFromUs(atof(argv[i+1]));
            cout << "target_q_delay" << atof(argv[i+1]) << " us"<< endl;
            i++;
        } else if (!strcmp(argv[i],"-queue_size_bdp_factor")) {
            queue_size_bdp_factor = atoi(argv[i+1]);
            cout << "Setting queue size to "<< queue_size_bdp_factor << "x BDP." << endl;
            i++;
        } else if (!strcmp(argv[i],"-sender_cc_algo")) {
            UecSrc::_sender_based_cc = true;
            sender_driven = true;
            
            if (!strcmp(argv[i+1],"dctcp")) 
                UecSrc::_sender_cc_algo = UecSrc::DCTCP;
            else if (!strcmp(argv[i+1],"nscc")) 
                UecSrc::_sender_cc_algo = UecSrc::NSCC;
            else if (!strcmp(argv[i+1],"mcc") || !strcmp(argv[i+1],"mcc_ideal"))
                UecSrc::_sender_cc_algo = UecSrc::MCC_IDEAL;
            else if (!strcmp(argv[i+1],"mcc_hardware"))
                UecSrc::_sender_cc_algo = UecSrc::MCC_HARDWARE;
            else if (!strcmp(argv[i+1],"mcc_incast"))
                UecSrc::_sender_cc_algo = UecSrc::MCC_INCAST;
            else if (!strcmp(argv[i+1],"constant")) 
                UecSrc::_sender_cc_algo = UecSrc::CONSTANT;
            else {
                cout << "UNKNOWN CC ALGO " << argv[i+1] << endl;
                exit(1);
            }    
            cout << "sender based algo "<< argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_rtt_thresh")) {
            mcc_rtt_threshold = timeFromUs(atof(argv[i+1]));
            mcc_params_set = true;
            cout << "MCC rtt threshold " << atof(argv[i+1]) << " us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_r1")) {
            mcc_r1 = atof(argv[i+1]);
            mcc_params_set = true;
            cout << "MCC R1 " << mcc_r1 << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_r2")) {
            mcc_r2 = atof(argv[i+1]);
            mcc_params_set = true;
            cout << "MCC R2 " << mcc_r2 << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_r3")) {
            mcc_r3 = atof(argv[i+1]);
            mcc_params_set = true;
            cout << "MCC R3 " << mcc_r3 << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_incast_rtt_thresh")) {
            mcc_incast_rtt_threshold = timeFromUs(atof(argv[i+1]));
            mcc_incast_params_set = true;
            cout << "MCC-Incast rtt threshold " << atof(argv[i+1]) << " us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_incast_r1")) {
            mcc_incast_r1 = atof(argv[i+1]);
            mcc_incast_params_set = true;
            cout << "MCC-Incast R1 " << mcc_incast_r1 << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_incast_r2")) {
            mcc_incast_r2 = atof(argv[i+1]);
            mcc_incast_params_set = true;
            cout << "MCC-Incast R2 " << mcc_incast_r2 << endl;
            i++;
        } else if (!strcmp(argv[i],"-mcc_incast_r3")) {
            mcc_incast_r3 = atof(argv[i+1]);
            mcc_incast_params_set = true;
            cout << "MCC-Incast R3 " << mcc_incast_r3 << endl;
            i++;
        } else if (!strcmp(argv[i],"-sender_cc")) {
            UecSrc::_sender_based_cc = true;
            UecSink::_oversubscribed_cc = false;
            sender_driven = true;
            cout << "sender based CC enabled " << endl;
        } else if (!strcmp(argv[i],"-receiver_cc")) {
            UecSrc::_receiver_based_cc = true;
            receiver_driven = true;
            cout << "receiver based CC enabled " << endl;
        }
        else if (!strcmp(argv[i],"-load_balancing_algo")){
            if (!strcmp(argv[i+1], "bitmap")) {
                load_balancing_algo = BITMAP;
            }
            else if (!strcmp(argv[i+1], "reps")) {
                load_balancing_algo = REPS;
            }
            else if (!strcmp(argv[i+1], "reps_legacy")) {
                load_balancing_algo = REPS_LEGACY;
            }
            else if (!strcmp(argv[i+1], "oblivious")) {
                load_balancing_algo = OBLIVIOUS;
            }
            else if (!strcmp(argv[i+1], "mixed")) {
                load_balancing_algo = MIXED;
            }
            else {
                cout << "Unknown load balancing algorithm of type " << argv[i+1] << ", expecting bitmap, reps or reps2" << endl;
                exit_error(argv[0]);
            }
            cout << "Load balancing algorithm set to  "<< argv[i+1] << endl;
            i++;
        }
        else if (!strcmp(argv[i],"-mprdma_L")) {
            mprdma_L = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_delta")) {
            mprdma_delta = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_alpha")) {
            mprdma_alpha = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_probe")) {
            mprdma_probe = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_iw")) {
            mprdma_iw = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_burst_us")) {
            mprdma_burst_us = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_idle_us")) {
            mprdma_idle_us = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_msg_bytes")) {
            mprdma_msg_bytes = strtoull(argv[i+1], NULL, 10);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_verb")) {
            mprdma_verb = argv[i+1];
            i++;
        } else if (!strcmp(argv[i],"-mprdma_recv_wq")) {
            mprdma_recv_wq = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_enable_pathselection")) {
            mprdma_enable_pathselection = (atoi(argv[i+1]) != 0);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_enable_cc")) {
            mprdma_enable_cc = (atoi(argv[i+1]) != 0);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_enable_recovery")) {
            mprdma_enable_recovery = (atoi(argv[i+1]) != 0);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_vp_seed")) {
            mprdma_vp_seed = static_cast<uint32_t>(strtoul(argv[i+1], NULL, 10));
            mprdma_vp_seed_set = true;
            i++;
        } else if (!strcmp(argv[i],"-mprdma_timeout") || !strcmp(argv[i],"-TIMEOUT")) {
            mprdma_timeout_sec = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-mprdma_min_rto_us")) {
            mprdma_min_rto_us = static_cast<uint32_t>(strtoul(argv[i+1], NULL, 10));
            i++;
        }
        else if (!strcmp(argv[i],"-dcqcn_ar")){
            if (!strcmp(argv[i+1], "single")) {
                dcqcn_single_path = true;
            }
            else if (!strcmp(argv[i+1], "bitmap")) {
                dcqcn_ar_algo = BITMAP;
            }
            else if (!strcmp(argv[i+1], "reps")) {
                dcqcn_ar_algo = REPS;
            }
            else if (!strcmp(argv[i+1], "reps_legacy")) {
                dcqcn_ar_algo = REPS_LEGACY;
            }
            else if (!strcmp(argv[i+1], "oblivious")) {
                dcqcn_ar_algo = OBLIVIOUS;
            }
            else if (!strcmp(argv[i+1], "mixed")) {
                dcqcn_ar_algo = MIXED;
            }
            else {
                cout << "Unknown DCQCN AR algorithm " << argv[i+1]
                     << ", expecting single|bitmap|reps|reps_legacy|oblivious|mixed" << endl;
                exit_error(argv[0]);
            }
            dcqcn_ar_override = true;
            cout << "DCQCN AR algorithm set to " << argv[i+1] << endl;
            i++;
        }
        else if (!strcmp(argv[i],"-dcqcn_single_path")) {
            dcqcn_single_path = true;
            cout << "DCQCN single path enabled" << endl;
        }
        else if (!strcmp(argv[i],"-queue_type")) {
            if (!strcmp(argv[i+1], "composite")) {
                qt = COMPOSITE;
            } 
            else if (!strcmp(argv[i+1], "composite_ecn")) {
                qt = COMPOSITE_ECN;
            }
            else if (!strcmp(argv[i+1], "aeolus")){
                qt = AEOLUS;
            }
            else if (!strcmp(argv[i+1], "aeolus_ecn")){
                qt = AEOLUS_ECN;
            }
            else if (!strcmp(argv[i+1], "lossless")){
                qt = LOSSLESS;
            }
            else if (!strcmp(argv[i+1], "lossless_input")){
                qt = LOSSLESS_INPUT;
            }
            else {
                cout << "Unknown queue type " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "queue_type "<< qt << endl;
            i++;
        } else if (!strcmp(argv[i],"-debug")) {
            UecSrc::_debug = true;
            UecPdcSes::_debug = true;
        } else if (!strcmp(argv[i],"-quiet")) {
            UecSrc::_quiet = true;
            cout << "Quiet mode enabled" << endl;
        } else if (!strcmp(argv[i],"-host_queue_type")) {
            if (!strcmp(argv[i+1], "swift")) {
                snd_type = SWIFT_SCHEDULER;
            } 
            else if (!strcmp(argv[i+1], "prio")) {
                snd_type = PRIORITY;
            }
            else if (!strcmp(argv[i+1], "fair_prio")) {
                snd_type = FAIR_PRIO;
            }
            else {
                cout << "Unknown host queue type " << argv[i+1] << " expecting one of swift|prio|fair_prio" << endl;
                exit_error(argv[0]);
            }
            cout << "host queue_type "<< snd_type << endl;
            i++;
        } else if (!strcmp(argv[i],"-log")){
            if (!strcmp(argv[i+1], "flow_events") || !strcmp(argv[i+1], "flow_event")) {
                log_flow_events = true;
            } else if (!strcmp(argv[i+1], "sink")) {
                cout << "logging sinks\n";
                log_sink = true;
            } else if (!strcmp(argv[i+1], "nic")) {
                cout << "logging nics\n";
                log_nic = true;
            } else if (!strcmp(argv[i+1], "tor_downqueue")) {
                cout << "logging tor downqueues\n";
                log_tor_downqueue = true;
            } else if (!strcmp(argv[i+1], "tor_upqueue")) {
                cout << "logging tor upqueues\n";
                log_tor_upqueue = true;
            } else if (!strcmp(argv[i+1], "switch")) {
                cout << "logging total switch queues\n";
                log_switches = true;
            } else if (!strcmp(argv[i+1], "traffic")) {
                cout << "logging traffic\n";
                log_traffic = true;
            } else if (!strcmp(argv[i+1], "queue_usage")) {
                cout << "logging queue usage\n";
                log_queue_usage = true;
            } else {
                exit_error(argv[0]);
            }
            i++;
        } else if (!strcmp(argv[i],"-cwnd")) {
            cwnd = strtoull(argv[i+1], nullptr, 10);
            cout << "cwnd " << cwnd << endl;
            i++;
        } else if (!strcmp(argv[i],"-pfc_only_cwnd")) {
            pfc_only_cwnd_pkts = strtoull(argv[i+1], nullptr, 10);
            cout << "PFC-only cwnd (pkts) " << pfc_only_cwnd_pkts << endl;
            i++;
        } else if (!strcmp(argv[i],"-tm")){
            tm_file = argv[i+1];
            cout << "traffic matrix input file: "<< tm_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-topo")){
            topo_file = argv[i+1];
            cout << "FatTree topology input file: "<< topo_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-q")){
            param_queuesize_set = true;
            queuesize_pkt = atoi(argv[i+1]);
            cout << "Setting queuesize to " << queuesize_pkt << " packets " << endl;
            i++;
        }
        else if (!strcmp(argv[i],"-sack_threshold")){
            UecSink::_bytes_unacked_threshold = atoi(argv[i+1]);
            cout << "Setting receiver SACK bytes threshold to " << UecSink::_bytes_unacked_threshold  << " bytes " << endl;
            i++;            
        }
        else if (!strcmp(argv[i],"-oversubscribed_cc")){
            UecSink::_oversubscribed_cc = true;
            cout << "Using receiver oversubscribed CC " << endl;
        }
        else if (!strcmp(argv[i],"-Ai")){
            OversubscribedCC::_Ai = atof(argv[i+1]);
            cout << "Using Ai "  << OversubscribedCC::_Ai << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-Md")){
            OversubscribedCC::_Md = atof(argv[i+1]);
            cout << "Using Md "  << OversubscribedCC::_Md << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-alpha")){
            OversubscribedCC::_alpha = atof(argv[i+1]);
            cout << "Using Alpha "  << OversubscribedCC::_alpha << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-force_disable_oversubscribed_cc")){
            UecSink::_oversubscribed_cc = false;
            force_disable_oversubscribed_cc = true;
            cout << "Disabling receiver oversubscribed CC even with OS topology" << endl;
        }
        else if (!strcmp(argv[i],"-enable_accurate_base_rtt")){
            enable_accurate_base_rtt = true;
            cout << "Enable accurate base rtt configuration, each flow uses the accurate end-to-end delay for the current sender/receiver pair as rtt upper bound." << endl;
        }
        else if (!strcmp(argv[i],"-disable_base_rtt_update_on_nack")){
            UecSrc::update_base_rtt_on_nack = false;
            cout << "Disables using NACKs to update the base RTT." << endl;
        }
        else if (!strcmp(argv[i],"-sleek")){
            UecSrc::_enable_sleek = true;
            cout << "Using SLEEK, the sender-based fast loss recovery heuristic " << endl;
        }
        else if (!strcmp(argv[i],"-no_ecn")){
            ecn = false;
            cout << "ECN disabled" << endl;
        }
        else if (!strcmp(argv[i],"-lossless_ecn_enable")){
            force_lossless_ecn = true;
            ecn = true;
            cout << "Force ECN on lossless output queues" << endl;
        }
        else if (!strcmp(argv[i],"-ecn")){
            // fraction of queuesize, between 0 and 1
            param_ecn_set = true;
            ecn = true;
            ecn_low = atoi(argv[i+1]); 
            ecn_high = atoi(argv[i+2]);
            i+=2;
        } else if (!strcmp(argv[i],"-disable_trim")) {
            disable_trim = true;
            cout << "Trimming disabled, dropping instead." << endl;
        } else if (!strcmp(argv[i],"-trimsize")){
            // size of trimmed packet in bytes
            trimsize = atoi(argv[i+1]);
            cout << "trimmed packet size: " << trimsize << " bytes\n";
            i+=1;
        } else if (!strcmp(argv[i],"-logtime")){
            double log_ms = atof(argv[i+1]);            
            logtime = timeFromMs(log_ms);
            cout << "logtime "<< log_ms << " ms" << endl;
            i++;
        } else if (!strcmp(argv[i],"-logtime_us")){
            double log_us = atof(argv[i+1]);            
            logtime = timeFromUs(log_us);
            cout << "logtime "<< log_us << " us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-failed")){
            // number of failed links (failed to 25% linkspeed)
            topo_num_failed = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-linkspeed")){
            // linkspeed specified is in Mbps
            linkspeed = speedFromMbps(atof(argv[i+1]));
            i++;
        } else if (!strcmp(argv[i],"-seed")){
            seed = atoi(argv[i+1]);
            cout << "random seed "<< seed << endl;
            i++;
        } else if (!strcmp(argv[i],"-mtu")){
            packet_size = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-paths")){
            path_entropy_size = atoi(argv[i+1]);
            cout << "no of paths " << path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-fg_path")){
            fg_path_entropy_size = atoi(argv[i+1]);
            cout << "foreground paths " << fg_path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-fg_paths")){
            fg_path_entropy_size = atoi(argv[i+1]);
            cout << "foreground paths " << fg_path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-bg_path")){
            bg_path_entropy_size = atoi(argv[i+1]);
            cout << "background paths " << bg_path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-bg_paths")){
            bg_path_entropy_size = atoi(argv[i+1]);
            cout << "background paths " << bg_path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-hop_latency")){
            hop_latency = timeFromUs(atof(argv[i+1]));
            cout << "Hop latency set to " << timeAsUs(hop_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-pcie")){
            UecSink::_model_pcie = true;
            pcie_rate = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-switch_latency")){
            switch_latency = timeFromUs(atof(argv[i+1]));
            cout << "Switch latency set to " << timeAsUs(switch_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_sticky_delta")){
            ar_sticky_delta = atof(argv[i+1]);
            cout << "Adaptive routing sticky delta " << ar_sticky_delta << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_granularity")){
            if (!strcmp(argv[i+1],"packet"))
                ar_sticky = FatTreeSwitch::PER_PACKET;
            else if (!strcmp(argv[i+1],"flow"))
                ar_sticky = FatTreeSwitch::PER_FLOWLET;
            else  {
                cout << "Expecting -ar_granularity packet|flow, found " << argv[i+1] << endl;
                exit(1);
            }   
            fg_ar_sticky = ar_sticky;
            bg_ar_sticky = ar_sticky;
            separate_ar_granularity = false;
            i++;
        } else if (!strcmp(argv[i],"-fg_ar_granularity")){
            if (!strcmp(argv[i+1],"packet"))
                fg_ar_sticky = FatTreeSwitch::PER_PACKET;
            else if (!strcmp(argv[i+1],"flow"))
                fg_ar_sticky = FatTreeSwitch::PER_FLOWLET;
            else  {
                cout << "Expecting -fg_ar_granularity packet|flow, found " << argv[i+1] << endl;
                exit(1);
            }
            separate_ar_granularity = true;
            i++;
        } else if (!strcmp(argv[i],"-bg_ar_granularity")){
            if (!strcmp(argv[i+1],"packet"))
                bg_ar_sticky = FatTreeSwitch::PER_PACKET;
            else if (!strcmp(argv[i+1],"flow"))
                bg_ar_sticky = FatTreeSwitch::PER_FLOWLET;
            else  {
                cout << "Expecting -bg_ar_granularity packet|flow, found " << argv[i+1] << endl;
                exit(1);
            }
            separate_ar_granularity = true;
            i++;
        } else if (!strcmp(argv[i],"-ar_method") || !strcmp(argv[i],"-fg_ar_method")){
            if (!strcmp(argv[i+1],"pause")){
                cout << "Adaptive routing based on pause state " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pause;
            }
            else if (!strcmp(argv[i+1],"queue")){
                cout << "Adaptive routing based on queue size " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_queuesize;
            }
            else if (!strcmp(argv[i+1],"bandwidth")){
                cout << "Adaptive routing based on bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_bandwidth;
            }
            else if (!strcmp(argv[i+1],"pqb")){
                cout << "Adaptive routing based on pause, queuesize and bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pqb;
            }
            else if (!strcmp(argv[i+1],"pq")){
                cout << "Adaptive routing based on pause, queuesize" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pq;
            }
            else if (!strcmp(argv[i+1],"pb")){
                cout << "Adaptive routing based on pause, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pb;
            }
            else if (!strcmp(argv[i+1],"qb")){
                cout << "Adaptive routing based on queuesize, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_qb; 
            }
            else {
                cout << "Unknown AR method expecting one of pause, queue, bandwidth, pqb, pq, pb, qb" << endl;
                exit(1);
            }
            i++;
        //New：新增参数
        } else if (!strcmp(argv[i],"-fg_cc")) {
            if (!strcmp(argv[i+1], "nscc")) {
                fg_cc_type = FG_NSCC;
                UecSrc::_sender_cc_algo = UecSrc::NSCC;
                UecSrc::_sender_based_cc = true;
                sender_driven = true;
            } else if (!strcmp(argv[i+1], "dcqcn")) {
                fg_cc_type = FG_DCQCN;
            } else if (!strcmp(argv[i+1], "swift")) {
                fg_cc_type = FG_SWIFT;
            } else if (!strcmp(argv[i+1], "mprdma") || !strcmp(argv[i+1], "mp_rdma")) {
                fg_cc_type = FG_MPRDMA;
            } else if (!strcmp(argv[i+1], "mcc") || !strcmp(argv[i+1], "mcc_ideal")) {
                fg_cc_type = FG_MCC_IDEAL;
                UecSrc::_sender_cc_algo = UecSrc::MCC_IDEAL;
                UecSrc::_sender_based_cc = true;
                sender_driven = true;
            } else if (!strcmp(argv[i+1], "mcc_incast")) {
                fg_cc_type = FG_MCC_INCAST;
                UecSrc::_sender_cc_algo = UecSrc::MCC_INCAST;
                UecSrc::_sender_based_cc = true;
                sender_driven = true;
            } else if (!strcmp(argv[i+1], "pfc") || !strcmp(argv[i+1], "pfc_only")) {
                fg_cc_type = FG_PFC_ONLY;
            } else {
                cout << "Unknown foreground CC " << argv[i+1] << " expecting one of nscc|dcqcn|swift|mprdma|mcc|mcc_incast|pfc" << endl;
                exit(1);
            }
            cout << "Foreground CC: " << fg_cc_name(fg_cc_type) << endl;
            i++;
        } else if (!strcmp(argv[i],"-bg_threshold")) {
            bg_flowid_threshold = atoll(argv[i+1]);
            cout << "Background flowid threshold set to " << bg_flowid_threshold << endl;
            i++;
        } else if (!strcmp(argv[i],"-cc_flowid_threshold")) {
            bg_flowid_threshold = atoll(argv[i+1]);
            fg_cc_type = FG_NSCC;
            cout << "Deprecated -cc_flowid_threshold, using NSCC for foreground and threshold " << bg_flowid_threshold << " for background PFC-only flows" << endl;
            i++;
        } else if (!strcmp(argv[i],"-fg_ar")) {
            FatTreeSwitch::ArStrategy fg_ar = FatTreeSwitch::AR_ADAPTIVE;
            if (!strcmp(argv[i+1], "ecmp")) {
                fg_ar = FatTreeSwitch::AR_ECMP;
            } else if (!strcmp(argv[i+1], "adaptive")) {
                fg_ar = FatTreeSwitch::AR_ADAPTIVE;
            } else if (!strcmp(argv[i+1], "mixed")) {
                fg_ar = FatTreeSwitch::AR_ECMP_ADAPTIVE;
            } else {
                cout << "Unknown foreground AR strategy: " << argv[i+1] << endl;
                exit(1);
            }
            FatTreeSwitch::set_fg_ar_strategy(fg_ar);
            cout << "Foreground AR strategy: " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-bg_ar")) {
            FatTreeSwitch::ArStrategy bg_ar = FatTreeSwitch::AR_ECMP;
            if (!strcmp(argv[i+1], "ecmp")) {
                bg_ar = FatTreeSwitch::AR_ECMP;
            } else if (!strcmp(argv[i+1], "adaptive")) {
                bg_ar = FatTreeSwitch::AR_ADAPTIVE;
            } else {
                cout << "Unknown background AR strategy: " << argv[i+1] << endl;
                exit(1);
            }
            FatTreeSwitch::set_bg_ar_strategy(bg_ar);
            cout << "Background AR strategy: " << argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-dcqcn_no_cc")) {
            dcqcn_no_cc = true;
            cout << "DCQCN no-cc mode enabled (PFC only)" << endl;
        } else if (!strcmp(argv[i],"-pfc_thresholds")){
            low_pfc = atoi(argv[i+1]);
            high_pfc = atoi(argv[i+2]);
            param_pfc_set = true;
            cout << "PFC thresholds high " << high_pfc << " low " << low_pfc << endl;
            i += 2;
        } else if (!strcmp(argv[i],"-min_rto")){
            user_min_rto_us = atoi(argv[i+1]);
            cout << "User-specified min RTO: " << user_min_rto_us << " us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-strat") || !strcmp(argv[i],"-fg_strat")){
            if (!strcmp(argv[i+1], "ecmp_host")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
            } else if (!strcmp(argv[i+1], "rr_ecmp")) {
                //this is the host route strategy;
                route_strategy = ECMP_FIB_ECN;
                qt = COMPOSITE_ECN_LB;
                //this is the switch route strategy. 
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR_ECMP);
            } else if (!strcmp(argv[i+1], "ecmp_host_ecn")) {
                route_strategy = ECMP_FIB_ECN;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                qt = COMPOSITE_ECN_LB;
            } else if (!strcmp(argv[i+1], "reactive_ecn")) {
                // Jitu's suggestion for something really simple
                // One path at a time, but switch whenever we get a trim or ecn
                //this is the host route strategy;
                route_strategy = REACTIVE_ECN;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                qt = COMPOSITE_ECN_LB;
            } else if (!strcmp(argv[i+1], "ecmp_ar")) {
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ADAPTIVE_ROUTING);
            } else if (!strcmp(argv[i+1], "ecmp_host_ar")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP_ADAPTIVE);
                //the stuff below obsolete
                //FatTreeSwitch::set_ar_fraction(atoi(argv[i+2]));
                //cout << "AR fraction: " << atoi(argv[i+2]) << endl;
                //i++;
            } else if (!strcmp(argv[i+1], "ecmp_rr")) {
                // switch round robin
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR);
            }
            i++;
        } else {
            cout << "Unknown parameter " << argv[i] << endl;
            exit_error(argv[0]);
        }
        i++;
    }

    uint32_t fg_paths = (fg_path_entropy_size > 0) ? fg_path_entropy_size : path_entropy_size;
    uint32_t bg_paths = (bg_path_entropy_size > 0) ? bg_path_entropy_size : 1;
    if (bg_paths == 0) {
        bg_paths = 1;
    }
    LoadBalancing_Algo fg_lb_algo = load_balancing_algo;
    LoadBalancing_Algo bg_lb_algo = OBLIVIOUS;

    if (fg_cc_type == FG_SWIFT && snd_type != SWIFT_SCHEDULER) {
        cout << "Warning: foreground Swift selected but host_queue_type is not swift; "
             << "use -host_queue_type swift for Swift scheduler." << endl;
    }

    if (end_time > 0 && logtime >= timeFromUs((uint32_t)end_time)){
        cout << "Logtime set to endtime" << endl;
        logtime = timeFromUs((uint32_t)end_time) - 1;
    }

    assert(trimsize >= 64 && trimsize <= (uint32_t)packet_size);

    cout << "Packet size (MTU) is " << packet_size << endl;

    srand(seed);
    srandom(seed);
    cout << "Parsed args\n";
    Packet::set_packet_size(packet_size);
    LosslessInputQueue::_high_threshold = Packet::data_packet_size()*high_pfc;
    LosslessInputQueue::_low_threshold = Packet::data_packet_size()*low_pfc;


    UecSrc::_mtu = Packet::data_packet_size();
    UecSrc::_mss = UecSrc::_mtu - UecSrc::_hdr_size;

    if (route_strategy==NOT_SET){
        route_strategy = ECMP_FIB;
        FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
    }

    FatTreeSwitch::set_bg_ar_strategy(FatTreeSwitch::AR_ECMP);
    bg_ar_sticky = FatTreeSwitch::PER_FLOWLET;
    separate_ar_granularity = true;

    /*
    UecSink::_oversubscribed_congestion_control = oversubscribed_congestion_control;
    */

    if (separate_ar_granularity) {
        FatTreeSwitch::set_fg_ar_sticky(fg_ar_sticky);
        FatTreeSwitch::set_bg_ar_sticky(bg_ar_sticky);
        FatTreeSwitch::set_separate_ar_sticky(true);
    } else {
        FatTreeSwitch::set_ar_sticky_all(ar_sticky);
    }
    FatTreeSwitch::set_bg_paths(static_cast<uint16_t>(bg_paths));
    FatTreeSwitch::_sticky_delta = timeFromUs(ar_sticky_delta);
    FatTreeSwitch::_ecn_threshold_fraction = ecn_thresh;
    FatTreeSwitch::_disable_trim = disable_trim;
    FatTreeSwitch::_trim_size = trimsize;

    if (end_time_set && end_time > 0) {
        eventlist.setEndtime(timeFromUs((uint32_t)end_time));
    }

    switch (route_strategy) {
    case ECMP_FIB_ECN:
    case REACTIVE_ECN:
        if (qt != COMPOSITE_ECN_LB) {
            fprintf(stderr, "Route Strategy is ECMP ECN.  Must use an ECN queue\n");
            exit(1);
        }
        assert(ecn_thresh > 0 && ecn_thresh < 1);
        // no break, fall through
    case ECMP_FIB:
        if (fg_paths > 10000) {
            fprintf(stderr, "Route Strategy is ECMP.  Must specify path count using -paths\n");
            exit(1);
        }
        break;
    case NOT_SET:
        fprintf(stderr, "Route Strategy not set.  Use the -strat param.  \nValid values are perm, rand, pull, rg and single\n");
        exit(1);
    default:
        break;
    }

    // prepare the loggers

    cout << "Logging to " << filename.str() << endl;
    //Logfile 
    Logfile logfile(filename.str(), eventlist);

    cout << "Linkspeed set to " << linkspeed/1000000000 << "Gbps" << endl;
    logfile.setStartTime(timeFromSec(0));

    vector<unique_ptr<UecNIC>> nics;

    UecSinkLoggerSampling* sink_logger = NULL;
    if (log_sink) {
        sink_logger = new UecSinkLoggerSampling(logtime, eventlist);
        logfile.addLogger(*sink_logger);
    }
    NicLoggerSampling* nic_logger = NULL;
    if (log_nic) {
        nic_logger = new NicLoggerSampling(logtime, eventlist);
        logfile.addLogger(*nic_logger);
    }
    TrafficLoggerSimple* traffic_logger = NULL;
    if (log_traffic) {
        traffic_logger = new TrafficLoggerSimple();
        logfile.addLogger(*traffic_logger);
    }
    FlowEventLoggerSimple* raw_event_logger = NULL;
    FlowEventLoggerFct* event_logger = NULL;
    if (log_flow_events) {
        raw_event_logger = new FlowEventLoggerSimple();
        logfile.addLogger(*raw_event_logger);
        event_logger = new FlowEventLoggerFct(logfile, eventlist, raw_event_logger, bg_flowid_threshold);
        logfile.addLogger(*event_logger);
    }

    logfile.write("traffic matrix input file: " + string(tm_file ? tm_file : "(stdin)"));
    logfile.write("FatTree topology input file: " + string(topo_file ? topo_file : "(generated)"));
    logfile.write("queue_type " + ntoa(static_cast<int>(qt)));
    if (receiver_driven && !sender_driven) {
        logfile.write("receiver based CC enabled ONLY");
    } else if (receiver_driven) {
        logfile.write("receiver based CC enabled");
    } else if (sender_driven) {
        logfile.write("sender based CC enabled");
    }
    if (!ecn) {
        logfile.write("ECN disabled");
    }
    logfile.write("endtime(us) " + ntoa(end_time));
    logfile.write("Packet size (MTU) is " + ntoa(packet_size));
    logfile.write("Parsed args");
    logfile.write("Logging to " + filename.str());
    logfile.write("Linkspeed set to " + ntoa(speedAsGbps(linkspeed)) + "Gbps");
    logfile.write("Loading connection matrix from  " + string(tm_file ? tm_file : "standard input"));

    //UecSrc::setMinRTO(50000); //increase RTO to avoid spurious retransmits
    UecSrc* uec_src;
    UecSink* uec_snk;

    //Route* routeout, *routein;

    QueueLoggerFactory *qlf = 0;
    if (log_tor_downqueue || log_tor_upqueue) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_SAMPLING, eventlist);
        qlf->set_sample_period(logtime);
    } else if (log_queue_usage) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_EMPTY, eventlist);
        qlf->set_sample_period(logtime);
    }

    auto conns = std::make_unique<ConnectionMatrix>(no_of_nodes);

    if (tm_file){
        cout << "Loading connection matrix from  " << tm_file << endl;

        if (!conns->load(tm_file)){
            cout << "Failed to load connection matrix " << tm_file << endl;
            exit(-1);
        }
    }
    else {
        cout << "Loading connection matrix from  standard input" << endl;        
        conns->load(cin);
    }

    if (conns->N != no_of_nodes && no_of_nodes != 0){
        cout << "Connection matrix number of nodes is " << conns->N << " while I am using " << no_of_nodes << endl;
        exit(-1);
    }

    no_of_nodes = conns->N;
    logfile.write("Nodes: " + ntoa(conns->N) + " Connections: " + ntoa(conns->getConnectionCount())
                  + " Triggers: " + ntoa(conns->getTriggerCount())
                  + " Failures: " + ntoa(conns->getFailureCount()));

    if (!param_queuesize_set) {
        cout << "Automatic queue sizing enabled ";        
        if (queue_size_bdp_factor==0) {
            if (disable_trim) {
                queue_size_bdp_factor = DEFAULT_NONTRIMMING_QUEUESIZE_FACTOR;
                cout << "non-trimming";
            } else {
                queue_size_bdp_factor = DEFAULT_TRIMMING_QUEUESIZE_FACTOR;
                cout << "trimming";
            }
        }
        cout << " queue-size-to-bdp-factor is " << queue_size_bdp_factor << "xBDP"
             << endl;
    }

    unique_ptr<FatTreeTopologyCfg> topo_cfg;
    if (topo_file) {
        topo_cfg = FatTreeTopologyCfg::load(topo_file, memFromPkt(queuesize_pkt), qt, snd_type);

        if (topo_cfg->no_of_nodes() != no_of_nodes) {
            cerr << "Mismatch between connection matrix (" << no_of_nodes << " nodes) and topology ("
                    << topo_cfg->no_of_nodes() << " nodes)" << endl;
            exit(1);
        }
    } else {
        topo_cfg = make_unique<FatTreeTopologyCfg>(tiers, no_of_nodes, linkspeed, memFromPkt(queuesize_pkt),
                                                   hop_latency, switch_latency, 
                                                   qt, snd_type);
    }
    logfile.write("Topology load done");

    simtime_picosec network_max_unloaded_rtt = calculate_rtt(topo_cfg.get(), linkspeed);

    mem_b queuesize = 0;
    if (!param_queuesize_set) {
        uint32_t bdp_pkt = calculate_bdp_pkt(topo_cfg.get(), linkspeed);
        mem_b queuesize_pkt = bdp_pkt * queue_size_bdp_factor;
        queuesize = memFromPkt(queuesize_pkt);
    } else {
        queuesize = memFromPkt(queuesize_pkt);
    }
    logfile.write("Setting queuesize to " + ntoa((uint64_t)(queuesize / Packet::data_packet_size())) + " packets");
    topo_cfg->set_queue_sizes(queuesize);

    if (!param_pfc_set && (qt == LOSSLESS_INPUT || qt == LOSSLESS_INPUT_ECN)) {
        uint32_t bdp_pkt = calculate_bdp_pkt(topo_cfg.get(), linkspeed);
        uint32_t queue_size_pkt = queuesize / Packet::data_packet_size();

        // Use queue capacity percentages rather than BDP multiples.
        high_pfc = static_cast<uint64_t>(ceil(queue_size_pkt * 0.5));
        low_pfc = static_cast<uint64_t>(ceil(queue_size_pkt * 0.3));

        // Boundary protection.
        if (high_pfc < bdp_pkt) {
            high_pfc = bdp_pkt;
        }
        if (low_pfc < bdp_pkt * 0.5) {
            low_pfc = static_cast<uint64_t>(ceil(bdp_pkt * 0.5));
        }
        if (high_pfc - low_pfc < bdp_pkt * 0.5) {
            low_pfc = high_pfc - static_cast<uint64_t>(ceil(bdp_pkt * 0.5));
        }

        cout << "Auto PFC thresholds: high=" << high_pfc << " pkts ("
             << Packet::data_packet_size() * high_pfc << " bytes, "
             << (double)high_pfc / queue_size_pkt * 100 << "% queue)"
             << " low=" << low_pfc << " pkts ("
             << Packet::data_packet_size() * low_pfc << " bytes, "
             << (double)low_pfc / queue_size_pkt * 100 << "% queue)" << endl;

        LosslessInputQueue::_high_threshold = Packet::data_packet_size() * high_pfc;
        LosslessInputQueue::_low_threshold = Packet::data_packet_size() * low_pfc;
    }

    if (topo_num_failed > 0) {
        topo_cfg->set_failed_links(topo_num_failed);
    }

    if (topo_cfg->get_oversubscription_ratio() > 1 && !UecSrc::_sender_based_cc && !force_disable_oversubscribed_cc) {
        UecSink::_oversubscribed_cc = true;
        OversubscribedCC::setOversubscriptionRatio(topo_cfg->get_oversubscription_ratio());
        cout << "Using simple receiver oversubscribed CC. Oversubscription ratio is " << topo_cfg->get_oversubscription_ratio() << endl;
    } 

    //2 priority queues; 3 hops for incast
    if (user_min_rto_us > 0) {
        // User specified min RTO (useful for PFC-only mode where PFC pauses can add significant delay)
        UecSrc::_min_rto = timeFromUs(user_min_rto_us);
        cout << "Setting min RTO to " << timeAsUs(UecSrc::_min_rto)
             << " us (user-specified)" << endl;
    } else {
        UecSrc::_min_rto = timeFromUs(15 + queuesize * 6.0 * 8 * 1000000 / linkspeed);
        cout << "Setting min RTO to " << timeAsUs(UecSrc::_min_rto)
             << " us (auto)" << endl;
    }

    if (ecn){
        uint32_t bdp_pkt = calculate_bdp_pkt(topo_cfg.get(), linkspeed);
        if (!param_ecn_set) {
            ecn_low = memFromPkt(ceil(bdp_pkt * 0.2));
            ecn_high = memFromPkt(ceil(bdp_pkt * 0.8));
        } else {
            ecn_low = memFromPkt(ecn_low);
            ecn_high = memFromPkt(ecn_high);
        }
        cout << "Setting ECN to parameters low " << ecn_low << " high " << ecn_high <<  " enable on tor downlink " << !receiver_driven << endl;
        topo_cfg->set_ecn_parameters(true, !receiver_driven, ecn_low, ecn_high);
        assert(ecn_low <= ecn_high);
        assert(ecn_high <= queuesize);
    }

    if ((fg_cc_type == FG_DCQCN || fg_cc_type == FG_MCC_IDEAL || fg_cc_type == FG_MCC_INCAST || force_lossless_ecn) &&
        (qt == LOSSLESS_INPUT || qt == LOSSLESS_INPUT_ECN)) {
        if (!ecn) {
            cout << "WARNING: ECN disabled; lossless ECN marking not enabled" << endl;
        } else {
            LosslessOutputQueue::_ecn_enabled = true;
            LosslessOutputQueue::_K = ecn_low;
            cout << "ECN marking enabled on lossless output queues, K=" << LosslessOutputQueue::_K << endl;
        }
    }

    cout << *topo_cfg << endl;

    vector<unique_ptr<FatTreeTopology>> topo;
    topo.resize(planes);
    for (uint32_t p = 0; p < planes; p++) {
        topo[p] = make_unique<FatTreeTopology>(topo_cfg.get(), qlf, &eventlist, nullptr);

        if (log_switches) {
            topo[p]->add_switch_loggers(logfile, logtime);
        }
    }
    FatTreeSwitch::set_bg_flowid_threshold(bg_flowid_threshold);
    cout << "network_max_unloaded_rtt " << timeAsUs(network_max_unloaded_rtt) << endl;
    logfile.write("network_max_unloaded_rtt " + ntoa(timeAsUs(network_max_unloaded_rtt)));

    if (UecSink::_oversubscribed_cc)
        OversubscribedCC::_base_rtt = network_max_unloaded_rtt;

    
    //handle link failures specified in the connection matrix.
    for (size_t c = 0; c < conns->failures.size(); c++){
        failure* crt = conns->failures.at(c);

        cout << "Adding link failure switch type" << crt->switch_type << " Switch ID " << crt->switch_id << " link ID "  << crt->link_id << endl;
        // xxx we only support failures in plane 0 for now.
        topo[0]->add_failed_link(crt->switch_type,crt->switch_id,crt->link_id);
    }

    // Initialize congestion control algorithms
    if (receiver_driven) {
        // TBD
    }
    if (sender_driven) {
        // UecSrc::parameterScaleToTargetQ();
        bool trimming_enabled = !disable_trim;
        UecSrc::initNsccParams(network_max_unloaded_rtt, linkspeed, target_Qdelay, qa_gate, trimming_enabled);
    }
    if (UecSrc::_sender_cc_algo == UecSrc::MCC_IDEAL ||
        UecSrc::_sender_cc_algo == UecSrc::MCC_HARDWARE ||
        fg_cc_type == FG_MCC_IDEAL || mcc_params_set) {
        MccIdealParams::initParams(mcc_rtt_threshold, mcc_r1, mcc_r2, mcc_r3);
        MccHardwareParams::initParams(mcc_rtt_threshold, mcc_r1, mcc_r2, mcc_r3);
    }
    if (UecSrc::_sender_cc_algo == UecSrc::MCC_INCAST ||
        fg_cc_type == FG_MCC_INCAST || mcc_incast_params_set) {
        MccIncastParams::initParams(mcc_incast_rtt_threshold, mcc_incast_r1,
                                    mcc_incast_r2, mcc_incast_r3);
    }

    vector<unique_ptr<UecPullPacer>> pacers;
    vector<PCIeModel*> pcie_models;
    vector<OversubscribedCC*> oversubscribed_ccs;

    for (size_t ix = 0; ix < no_of_nodes; ix++){
        auto &pacer = pacers.emplace_back(make_unique<UecPullPacer>(linkspeed, 0.99,
          UecBasePacket::unquantize(UecSink::_credit_per_pull), eventlist, ports));

        if (UecSink::_model_pcie)
            pcie_models.push_back(new PCIeModel(linkspeed * pcie_rate, UecSrc::_mtu, eventlist,
              pacer.get()));

        if (UecSink::_oversubscribed_cc)
            oversubscribed_ccs.push_back(new OversubscribedCC(eventlist, pacer.get()));

        auto &nic = nics.emplace_back(make_unique<UecNIC>(ix, eventlist,
                                                          linkspeed, ports));
        if (log_nic) {
            nic_logger->monitorNic(nic.get());
        }
    }

    // used just to print out stats data at the end
    list <const Route*> routes;

    vector<connection*>* all_conns = conns->getAllConnections();
    vector <UecSrc*> uec_srcs;
    vector <UecSrc*> pfc_only_srcs;
    vector<MPRdmaSrc*> mprdma_srcs;
    vector<MPRdmaSink*> mprdma_sinks;

    // ===== New：DCQCN 容器 =====
    vector<DCQCNSrc*> dcqcn_srcs;
    vector<DCQCNSink*> dcqcn_sinks;
    uint64_t dcqcn_no_cc_flows = 0;
    // ============================
    vector<SwiftSrc*> swift_srcs;
    vector<SwiftSink*> swift_sinks;
    SwiftRtxTimerScanner swift_rtx_scanner(timeFromUs((uint32_t)100), eventlist);

    map<flowid_t, pair<UecSrc*, UecSink*>> flowmap;
    map<flowid_t, UecPdcSes*> flow_pdc_map;
    if (!UecSrc::_quiet && !force_verbose && all_conns->size() >= 100) {
        UecSrc::_quiet = true;
        cout << "Auto quiet enabled for " << all_conns->size()
             << " connections (use -verbose to keep per-flow logs)" << endl;
    }
    DCQCNSrc::set_quiet(UecSrc::_quiet);
    if(planes != 1){
        cout << "We are taking the plane 0 to calculate the network rtt; If all the planes have the same tiers, you can remove this check." << endl;
        assert(false);
    }

    mem_b cwnd_b = static_cast<mem_b>(cwnd) * Packet::data_packet_size();
    mem_b pfc_only_cwnd_b = static_cast<mem_b>(pfc_only_cwnd_pkts) * Packet::data_packet_size();
    for (size_t c = 0; c < all_conns->size(); c++){
        connection* crt = all_conns->at(c);
        int src = crt->src;
        int dest = crt->dst;

        if (!conn_reuse and crt->msgid.has_value()) {
            cout << "msg keyword can only be used when conn_reuse is enabled.\n";
            abort();
        }

        assert(planes > 0);

        // ===== New: 判断使用哪种拥塞控制算法 =====
        flowid_t current_flowid = 0;
        if (crt->flowid) {
            current_flowid = crt->flowid;
        } else {
            current_flowid = c + 1;
        }

        bool is_background = (current_flowid > bg_flowid_threshold);
        ForegroundCCType actual_cc = is_background ? FG_PFC_ONLY : fg_cc_type;
        // ==========================================

        simtime_picosec transmission_delay = (Packet::data_packet_size() * 8 / speedAsGbps(linkspeed) * topo_cfg->get_diameter() * 1000) 
                                             + (UecBasePacket::get_ack_size() * 8 / speedAsGbps(linkspeed) * topo_cfg->get_diameter() * 1000);
        simtime_picosec base_rtt_bw_two_points = 2*topo_cfg->get_two_point_diameter_latency(src, dest) + transmission_delay;

        //cout << "Connection " << crt->src << "->" <<crt->dst << " starting at " << crt->start << " size " << crt->size << endl;

        // ========== 根据 flowid 创建不同类型的流 ==========
        if (!UecSrc::_quiet && (current_flowid <= 32 || is_background)) {
            cout << (is_background ? "[BG] " : "[FG] ")
                 << "Flow " << current_flowid
                 << " (" << fg_cc_name(actual_cc) << ")"
                 << ": src=" << src << " (" << src/64 << ")"
                 << " dst=" << dest << " (" << dest/64 << ")"
                 << " ToR_src=" << topo_cfg->HOST_POD_SWITCH(src)
                 << " ToR_dst=" << topo_cfg->HOST_POD_SWITCH(dest)
                 << endl;
        }

        if (actual_cc == FG_DCQCN) {
            // ==================== 创建 DCQCN 流 ====================
            if (!UecSrc::_quiet) {
                cout << "Creating DCQCN flow " << current_flowid << " (" << src << "->" << dest << ")" << endl;
            }

            LoadBalancing_Algo dcqcn_algo = dcqcn_ar_override ? dcqcn_ar_algo : fg_lb_algo;
            unique_ptr<UecMultipath> mp = nullptr;
            if (!dcqcn_single_path) {
                if (dcqcn_algo == REPS) {
                    mp = make_unique<UecMpReps>(static_cast<uint16_t>(fg_paths), UecSrc::_debug, !disable_trim);
                } else if (dcqcn_algo == BITMAP) {
                    mp = make_unique<UecMpBitmap>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (dcqcn_algo == REPS_LEGACY) {
                    mp = make_unique<UecMpRepsLegacy>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (dcqcn_algo == OBLIVIOUS) {
                    mp = make_unique<UecMpOblivious>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (dcqcn_algo == MIXED) {
                    mp = make_unique<UecMpMixed>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                }
            }

            DCQCNSrc* dcqcn_src = new DCQCNSrc(NULL, traffic_logger, eventlist, linkspeed, std::move(mp));
            DCQCNSink* dcqcn_sink = new DCQCNSink(eventlist);

            // Set identifiers and endpoints before connect.
            dcqcn_src->set_flowid(current_flowid);
            dcqcn_src->set_dst(dest);
            dcqcn_sink->set_src(src);

            if (dcqcn_no_cc) {
                dcqcn_src->set_no_cc(true);
                dcqcn_no_cc_flows++;
            }

            if (log_flow_events && event_logger) {
                dcqcn_src->logFlowEvents(*event_logger);
            }

            mem_b dcqcn_size = crt->size;
            if (dcqcn_size > 0) {
                dcqcn_src->set_flowsize(dcqcn_size);
                if (!UecSrc::_quiet) {
                    cout << "  Set DCQCN flow size to " << dcqcn_size << " bytes" << endl;
                }
            } else {
                cout << "  WARNING: Flow size is 0 for DCQCN flow " << current_flowid << endl;
            }

            string src_name = "DCQCN_" + ntoa(src) + "_" + ntoa(dest);
            dcqcn_src->setName(src_name);
            logfile.writeName(*dcqcn_src);

            string sink_name = "DCQCN_sink_" + ntoa(src) + "_" + ntoa(dest);
            ((DataReceiver*)dcqcn_sink)->setName(sink_name);
            logfile.writeName(*(DataReceiver*)dcqcn_sink);

            HostQueue* src_hq = dynamic_cast<HostQueue*>(
                topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]
            );
            assert(src_hq != nullptr);
            src_hq->addHostSender(dcqcn_src);

            bool dcqcn_switch_ecmp = (route_strategy == ECMP_FIB ||
                                      route_strategy == ECMP_FIB_ECN ||
                                      route_strategy == REACTIVE_ECN) &&
                (dcqcn_algo == REPS || dcqcn_algo == BITMAP || dcqcn_algo == MIXED);

            Route* first_routeout = nullptr;
            Route* first_routeback = nullptr;
            if (dcqcn_switch_ecmp) {
                dcqcn_src->set_entropy_paths(static_cast<uint16_t>(fg_paths));
                Route* srctotor = new Route();
                srctotor->push_back(topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                srctotor->push_back(topo[0]->pipes_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                srctotor->push_back(topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());

                Route* dsttotor = new Route();
                dsttotor->push_back(topo[0]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                dsttotor->push_back(topo[0]->pipes_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                dsttotor->push_back(topo[0]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]->getRemoteEndpoint());

                first_routeout = srctotor;
                first_routeback = dsttotor;
            } else {
                vector<const Route*>* paths_out = topo[0]->get_paths(src, dest);
                vector<const Route*>* paths_back = topo[0]->get_paths(dest, src);
                if (!paths_out || paths_out->empty()) {
                    cout << "ERROR: no forward paths for DCQCN flow " << current_flowid
                         << " (" << src << "->" << dest << ")" << endl;
                    exit(1);
                }
                if (!paths_back || paths_back->empty()) {
                    cout << "ERROR: no reverse paths for DCQCN flow " << current_flowid
                         << " (" << dest << "->" << src << ")" << endl;
                    exit(1);
                }

                size_t max_paths = dcqcn_single_path ? 1 : paths_out->size();
                for (size_t i = 0; i < max_paths; i++) {
                    Route* routeout = new Route(*(paths_out->at(i)), *dcqcn_sink);
                    Route* routeback = new Route(*(paths_back->at(i % paths_back->size())), *dcqcn_src);
                    routeout->add_endpoints(dcqcn_src, dcqcn_sink);
                    routeback->add_endpoints(dcqcn_sink, dcqcn_src);
                    dcqcn_src->addPath(routeout, routeback);
                    if (i == 0) {
                        first_routeout = routeout;
                        first_routeback = routeback;
                    }
                }
            }

            simtime_picosec start_time = (crt->start == TRIGGER_START) ? 0 : timeFromUs((uint32_t)crt->start);
            if (!first_routeout || !first_routeback) {
                cout << "ERROR: no DCQCN routes for " << src << "->" << dest << endl;
                exit(1);
            }
            dcqcn_src->connect(first_routeout, first_routeback, *dcqcn_sink, start_time);

            FatTreeSwitch* src_switch = dynamic_cast<FatTreeSwitch*>(
                topo[0]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]
            );
            FatTreeSwitch* dst_switch = dynamic_cast<FatTreeSwitch*>(
                topo[0]->switches_lp[topo_cfg->HOST_POD_SWITCH(dest)]
            );
            if (src_switch) {
                src_switch->addHostPort(src, current_flowid, dcqcn_src);
            } else {
                cout << "ERROR: src_switch is NULL for DCQCN flow " << current_flowid << endl;
            }
            if (dst_switch) {
                dst_switch->addHostPort(dest, current_flowid, dcqcn_sink);
            } else {
                cout << "ERROR: dst_switch is NULL for DCQCN flow " << current_flowid << endl;
            }

            if (crt->trigger) {
                Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                trig->add_target(*dcqcn_src);
            }

            if (crt->send_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                dcqcn_src->set_end_trigger(*trig);
            }

            if (crt->recv_done_trigger) {
                cout << "Warning: recv_done_trigger not supported for DCQCN flows" << endl;
            }

            dcqcn_srcs.push_back(dcqcn_src);
            dcqcn_sinks.push_back(dcqcn_sink);
            continue;

        } else if (actual_cc == FG_MPRDMA) {
            if (conn_reuse) {
                cout << "ERROR: MPRDMA does not support connection reuse." << endl;
                exit(1);
            }

            MPRdmaSrc* mprdma_src = new MPRdmaSrc(nullptr, eventlist, linkspeed);
            MPRdmaSink* mprdma_sink = new MPRdmaSink();

            uint16_t mprdma_paths = static_cast<uint16_t>(fg_paths > 0 ? fg_paths : 1);
            if (!mprdma_enable_pathselection) {
                mprdma_paths = 1;
            }
            mprdma_src->set_max_path_id(mprdma_paths);
            mprdma_src->set_bitmap_len(mprdma_L);
            mprdma_src->set_delta(mprdma_delta);
            mprdma_src->set_alpha(mprdma_alpha);
            mprdma_src->set_probe_p(mprdma_probe);
            mprdma_src->set_initial_window(mprdma_iw);
            mprdma_src->set_burst_timeout(timeFromUs(mprdma_burst_us));
            mprdma_src->set_idle_timeout(timeFromUs(mprdma_idle_us));
            mprdma_src->set_message_bytes(mprdma_msg_bytes);
            mprdma_src->set_recv_wq_depth(mprdma_recv_wq);
            mprdma_src->set_enable_cc(mprdma_enable_cc);
            mprdma_src->set_enable_recovery(mprdma_enable_recovery);
            mprdma_src->set_log_fct(true);
            mprdma_src->set_is_background(is_background);
            if (mprdma_timeout_sec > 0.0) {
                mprdma_src->set_rto_timeout(timeFromSec(mprdma_timeout_sec));
            }
            if (mprdma_min_rto_us > 0) {
                mprdma_src->set_min_rto(timeFromUs(mprdma_min_rto_us));
            }
            if (mprdma_vp_seed_set) {
                mprdma_src->set_vp_seed(mprdma_vp_seed);
            }

            if (mprdma_verb == "send") {
                mprdma_src->set_verb(MPRdmaSrc::VERB_SEND);
            } else if (mprdma_verb == "read_resp") {
                mprdma_src->set_verb(MPRdmaSrc::VERB_READ_RESP);
            } else {
                mprdma_src->set_verb(MPRdmaSrc::VERB_WRITE);
            }

            mprdma_src->set_flowid(current_flowid);
            mprdma_src->set_dst(dest);
            mprdma_sink->set_src(src);

            if (crt->size > 0) {
                mprdma_src->set_flowsize(crt->size);
            }

            mprdma_src->setName("MPRdma_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*mprdma_src);
            mprdma_sink->setName("MPRdma_sink_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*mprdma_sink);

            if (crt->trigger) {
                Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                trig->add_target(*mprdma_src);
            }
            if (crt->send_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                mprdma_src->set_end_trigger(*trig);
            }
            if (crt->recv_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                mprdma_sink->set_cqe_handler(new MPRdmaCqeTrigger(trig));
            }

            HostQueue* src_hq = dynamic_cast<HostQueue*>(
                topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]
            );
            assert(src_hq != nullptr);
            src_hq->addHostSender(mprdma_src);

            Route* first_routeout = nullptr;
            Route* first_routeback = nullptr;
            if (route_strategy == ECMP_FIB || route_strategy == ECMP_FIB_ECN ||
                route_strategy == REACTIVE_ECN) {
                Route* srctotor = new Route();
                srctotor->push_back(topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                srctotor->push_back(topo[0]->pipes_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                srctotor->push_back(topo[0]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());

                Route* dsttotor = new Route();
                dsttotor->push_back(topo[0]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                dsttotor->push_back(topo[0]->pipes_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                dsttotor->push_back(topo[0]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]->getRemoteEndpoint());

                first_routeout = srctotor;
                first_routeback = dsttotor;
            } else {
                vector<const Route*>* paths_out = topo[0]->get_paths(src, dest);
                vector<const Route*>* paths_back = topo[0]->get_paths(dest, src);
                if (!paths_out || paths_out->empty()) {
                    cout << "ERROR: no forward paths for MPRDMA flow " << current_flowid
                         << " (" << src << "->" << dest << ")" << endl;
                    exit(1);
                }
                if (!paths_back || paths_back->empty()) {
                    cout << "ERROR: no reverse paths for MPRDMA flow " << current_flowid
                         << " (" << dest << "->" << src << ")" << endl;
                    exit(1);
                }
                Route* routeout = new Route(*(paths_out->at(0)), *mprdma_sink);
                Route* routeback = new Route(*(paths_back->at(0)), *mprdma_src);
                routeout->add_endpoints(mprdma_src, mprdma_sink);
                routeback->add_endpoints(mprdma_sink, mprdma_src);
                first_routeout = routeout;
                first_routeback = routeback;
            }

            simtime_picosec start_time = (crt->start == TRIGGER_START) ? 0 : timeFromUs((uint32_t)crt->start);
            if (!first_routeout || !first_routeback) {
                cout << "ERROR: no MPRDMA routes for " << src << "->" << dest << endl;
                exit(1);
            }
            mprdma_src->connect(first_routeout, first_routeback, *mprdma_sink, start_time);

            FatTreeSwitch* src_switch = dynamic_cast<FatTreeSwitch*>(
                topo[0]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]
            );
            FatTreeSwitch* dst_switch = dynamic_cast<FatTreeSwitch*>(
                topo[0]->switches_lp[topo_cfg->HOST_POD_SWITCH(dest)]
            );
            if (src_switch) {
                src_switch->addHostPort(src, mprdma_src->flow_id(), mprdma_src);
            } else {
                cout << "ERROR: src_switch is NULL for MPRDMA flow " << current_flowid << endl;
            }
            if (dst_switch) {
                dst_switch->addHostPort(dest, mprdma_src->flow_id(), mprdma_sink);
            } else {
                cout << "ERROR: dst_switch is NULL for MPRDMA flow " << current_flowid << endl;
            }

            mprdma_srcs.push_back(mprdma_src);
            mprdma_sinks.push_back(mprdma_sink);
            continue;

        } else if (actual_cc == FG_SWIFT) {
            // ==================== 创建 Swift 流 ====================
            SwiftSrc* swift_src = new SwiftSrc(swift_rtx_scanner, NULL, traffic_logger, eventlist);
            SwiftSink* swift_sink = new SwiftSink();

            swift_src->set_flowid(current_flowid);
            swift_src->set_dst(dest);
            swift_sink->set_src_id(src);
            if (crt->size > 0) {
                swift_src->set_flowsize(crt->size);
            }
            simtime_picosec base_delay = network_max_unloaded_rtt / 2;
            if (base_delay < timeFromUs((uint32_t)10)) base_delay = timeFromUs((uint32_t)10);
            if (base_delay > timeFromUs((uint32_t)50)) base_delay = timeFromUs((uint32_t)50);
            swift_src->set_base_delay(base_delay);
            if (cwnd > 0) {
                uint64_t swift_cwnd_b = cwnd * Packet::data_packet_size();
                swift_src->set_cwnd(static_cast<uint32_t>(std::min<uint64_t>(swift_cwnd_b, UINT32_MAX)));
            } else {
                mem_b bdp = static_cast<mem_b>(timeAsSec(network_max_unloaded_rtt) * (linkspeed / 8.0));
                mem_b initial_cwnd = static_cast<mem_b>(bdp * 0.5);
                if (initial_cwnd < 10 * Packet::data_packet_size()) {
                    initial_cwnd = 10 * Packet::data_packet_size();
                }
                swift_src->set_cwnd(static_cast<uint32_t>(std::min<mem_b>(initial_cwnd, UINT32_MAX)));
            }

            swift_src->setName("Swift_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*swift_src);
            swift_sink->setName("Swift_sink_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*swift_sink);

            if (log_flow_events && event_logger) {
                swift_src->logFlowEvents(*event_logger);
            }

            vector<const Route*>* paths_out = topo[0]->get_paths(src, dest);
            vector<const Route*>* paths_back = topo[0]->get_paths(dest, src);
            if (!paths_out || paths_out->empty()) {
                cout << "ERROR: no Swift forward paths for " << src << "->" << dest << endl;
                exit(1);
            }
            if (!paths_back || paths_back->empty()) {
                cout << "ERROR: no Swift reverse paths for " << dest << "->" << src << endl;
                exit(1);
            }

            Route* first_routeout = nullptr;
            Route* first_routeback = nullptr;
            for (size_t i = 0; i < paths_out->size(); i++) {
                Route* routeout = new Route(*(paths_out->at(i)));
                Route* routeback = new Route(*(paths_back->at(i % paths_back->size())));
                swift_src->addPath(routeout, routeback);
                if (i == 0) {
                    first_routeout = routeout;
                    first_routeback = routeback;
                }
            }

            simtime_picosec start_time = (crt->start == TRIGGER_START) ? 0 : timeFromUs((uint32_t)crt->start);

            if (crt->trigger) {
                Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                swift_src->set_start_trigger(*trig);
                trig->add_target(*swift_src);
            }
            if (crt->send_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                swift_src->set_end_trigger(*trig);
            }
            if (!first_routeout || !first_routeback) {
                cout << "ERROR: no Swift routes for " << src << "->" << dest << endl;
                exit(1);
            }
            swift_src->connect(*first_routeout, *first_routeback, *swift_sink, start_time);

            SwiftSubflowSrc* subflow = swift_src->get_subflow();
            if (subflow) {
                unique_ptr<UecMultipath> mp = nullptr;
                if (fg_lb_algo == BITMAP) {
                    mp = make_unique<UecMpBitmap>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (fg_lb_algo == REPS) {
                    mp = make_unique<UecMpReps>(static_cast<uint16_t>(fg_paths), UecSrc::_debug, !disable_trim);
                } else if (fg_lb_algo == REPS_LEGACY) {
                    mp = make_unique<UecMpRepsLegacy>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (fg_lb_algo == OBLIVIOUS) {
                    mp = make_unique<UecMpOblivious>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                } else if (fg_lb_algo == MIXED) {
                    mp = make_unique<UecMpMixed>(static_cast<uint16_t>(fg_paths), UecSrc::_debug);
                }
                if (mp) {
                    subflow->set_multipath(std::move(mp), swift_src->get_no_of_paths());
                }
            }

            swift_srcs.push_back(swift_src);
            swift_sinks.push_back(swift_sink);

        } else if (!conn_reuse 
            || (crt->flowid and flowmap.find(crt->flowid) == flowmap.end())) {
            LoadBalancing_Algo flow_lb_algo = is_background ? bg_lb_algo : fg_lb_algo;
            uint32_t flow_paths = is_background ? bg_paths : fg_paths;
            unique_ptr<UecMultipath> mp = nullptr;
            if (flow_lb_algo == BITMAP){
                mp = make_unique<UecMpBitmap>(static_cast<uint16_t>(flow_paths), UecSrc::_debug);
            } else if (flow_lb_algo == REPS){
                mp = make_unique<UecMpReps>(static_cast<uint16_t>(flow_paths), UecSrc::_debug, !disable_trim);
            } else if (flow_lb_algo == REPS_LEGACY){
                mp = make_unique<UecMpRepsLegacy>(static_cast<uint16_t>(flow_paths), UecSrc::_debug);
            } else if (flow_lb_algo == OBLIVIOUS){
                mp = make_unique<UecMpOblivious>(static_cast<uint16_t>(flow_paths), UecSrc::_debug);
            } else if (flow_lb_algo == MIXED){
                mp = make_unique<UecMpMixed>(static_cast<uint16_t>(flow_paths), UecSrc::_debug);
            } else {
                cout << "ERROR: Failed to set multipath algorithm, abort." << endl;
                abort();
            }

            uec_src = new UecSrc(traffic_logger, eventlist, move(mp), *nics.at(src), ports);

            if (crt->flowid) {
                uec_src->setFlowId(crt->flowid);
                assert(flowmap.find(crt->flowid) == flowmap.end()); // don't have dups
            }

            

            if (conn_reuse) {
                stringstream uec_src_dbg_tag;
                uec_src_dbg_tag << "flow_id " << uec_src->flowId();
                UecPdcSes* pdc = new UecPdcSes(uec_src, EventList::getTheEventList(), UecSrc::_mss, UecSrc::_hdr_size, uec_src_dbg_tag.str());
                uec_src->makeReusable(pdc);
                flow_pdc_map[uec_src->flowId()] = pdc;
            }

            if (receiver_driven)
                uec_snk = new UecSink(NULL, pacers[dest].get(), *nics.at(dest),
                                      ports);
            else //each connection has its own pacer, so receiver driven mode does not kick in! 
                uec_snk = new UecSink(NULL,linkspeed,1.1,UecBasePacket::unquantize(UecSink::_credit_per_pull),eventlist,*nics.at(dest), ports);

            flowmap[uec_src->flowId()] = { uec_src, uec_snk };

            if (crt->flowid) {
                uec_snk->setFlowId(crt->flowid);
            }

            // If cwnd is 0 initXXcc will set a sensible default value 
            if (receiver_driven) {
                // uec_src->setCwnd(cwnd*Packet::data_packet_size());
                // uec_src->setMaxWnd(cwnd*Packet::data_packet_size());

                if (enable_accurate_base_rtt) {
                    uec_src->initRccc(cwnd_b, base_rtt_bw_two_points);
                } else {
                    uec_src->initRccc(cwnd_b, network_max_unloaded_rtt);
                }
            }

            if (sender_driven) {
                if (enable_accurate_base_rtt) {
                    uec_src->initNscc(cwnd_b, base_rtt_bw_two_points);
                } else {
                    uec_src->initNscc(cwnd_b, network_max_unloaded_rtt);
                }
            }
            if (actual_cc == FG_PFC_ONLY) {
                mem_b pfc_only_cwnd = pfc_only_cwnd_b;
                if (pfc_only_cwnd == 0) {
                    if (cwnd_b > 0) {
                        pfc_only_cwnd = cwnd_b;
                    } else {
                        // Keep PFC-only flows bounded by BDP to avoid runaway event counts.
                        pfc_only_cwnd = static_cast<mem_b>(timeAsSec(network_max_unloaded_rtt) * (linkspeed / 8.0));
                        if (pfc_only_cwnd < Packet::data_packet_size()) {
                            pfc_only_cwnd = Packet::data_packet_size();
                        }
                    }
                }
                uec_src->setPfcOnlyMode(true);
                uec_src->setCwnd(pfc_only_cwnd);
                uec_src->setMaxWnd(pfc_only_cwnd);
                uec_src->setConfiguredMaxWnd(pfc_only_cwnd);
                pfc_only_srcs.push_back(uec_src);
            } else {
                uec_srcs.push_back(uec_src);
            }
            uec_src->setDst(dest);

            if (log_flow_events) {
                uec_src->logFlowEvents(*event_logger);
            }
            

            uec_src->setName("Uec_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*uec_src);
            uec_snk->setSrc(src);

            if (UecSink::_model_pcie){
                uec_snk->setPCIeModel(pcie_models[dest]);
            }
                            
            if (UecSink::_oversubscribed_cc){
                uec_snk->setOversubscribedCC(oversubscribed_ccs[dest]);
            }

            ((DataReceiver*)uec_snk)->setName("Uec_sink_" + ntoa(src) + "_" + ntoa(dest));
            logfile.writeName(*(DataReceiver*)uec_snk);

            if (!conn_reuse) {
                if (crt->size>0){
                    uec_src->setFlowsize(crt->size);
                }

                if (crt->trigger) {
                    Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                    trig->add_target(*uec_src);
                }

                if (crt->send_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                    uec_src->setEndTrigger(*trig);
                }

                if (crt->recv_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                    uec_snk->setEndTrigger(*trig);
                }
            } else {
                assert(crt->size > 0);

                optional<simtime_picosec> start_ts = {};
                if (crt->start != TRIGGER_START) {
                    start_ts.emplace(timeFromUs((uint32_t)crt->start));
                } 

                UecPdcSes* pdc = flow_pdc_map.find(crt->flowid)->second;
                UecMsg* msg = pdc->enque(crt->size, start_ts, true);

                if (crt->trigger) {
                    Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                    trig->add_target(*msg);
                }

                if (crt->send_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                    msg->setTrigger(UecMsg::MsgStatus::SentLast, trig);
                }

                if (crt->recv_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                    uec_snk->setEndTrigger(*trig);
                    msg->setTrigger(UecMsg::MsgStatus::RecvdLast, trig);
                }
            }

            //uec_snk->set_priority(crt->priority);
                            
            for (uint32_t p = 0; p < planes; p++) {
                switch (route_strategy) {
                case ECMP_FIB:
                case ECMP_FIB_ECN:
                case REACTIVE_ECN:
                    {
                        Route* srctotor = new Route();
                        srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                        srctotor->push_back(topo[p]->pipes_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                        srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());

                        Route* dsttotor = new Route();
                        dsttotor->push_back(topo[p]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                        dsttotor->push_back(topo[p]->pipes_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                        dsttotor->push_back(topo[p]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]->getRemoteEndpoint());

                        uec_src->connectPort(p, *srctotor, *dsttotor, *uec_snk, crt->start);
                        //uec_src->setPaths(path_entropy_size);
                        //uec_snk->setPaths(path_entropy_size);

                        //register src and snk to receive packets from their respective TORs. 
                        assert(topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]);
                        assert(topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]);
                        topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]->addHostPort(src,uec_snk->flowId(),uec_src->getPort(p));
                        topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(dest)]->addHostPort(dest,uec_src->flowId(),uec_snk->getPort(p));
                        break;
                    }
                default:
                    abort();
                }
            }

            // set up the triggers
            // xxx

            if (log_sink) {
                sink_logger->monitorSink(uec_snk);
            }
        } else {
            // Use existing connection for this message
            assert(crt->msgid.has_value());

            UecPdcSes* pdc = flow_pdc_map.find(crt->flowid)->second;
            uec_src = nullptr;
            uec_snk = nullptr;

            optional<simtime_picosec> start_ts = {};
            if (crt->start != TRIGGER_START) {
                start_ts.emplace(timeFromUs((uint32_t)crt->start));
            } 

            UecMsg* msg = pdc->enque(crt->size, start_ts, true);

            if (crt->trigger) {
                Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                trig->add_target(*msg);
            }

            if (crt->send_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                msg->setTrigger(UecMsg::MsgStatus::SentLast, trig);
            }

            if (crt->recv_done_trigger) {
                Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                msg->setTrigger(UecMsg::MsgStatus::RecvdLast, trig);
            }
        }
    }

    Logged::dump_idmap();
    // Record the setup
    int pktsize = Packet::data_packet_size();
    logfile.write("# pktsize=" + ntoa(pktsize) + " bytes");
    logfile.write("# hostnicrate = " + ntoa(linkspeed/1000000) + " Mbps");
    //logfile.write("# corelinkrate = " + ntoa(HOST_NIC*CORE_TO_HOST) + " pkt/sec");
    //logfile.write("# buffer = " + ntoa((double) (queues_na_ni[0][1]->_maxsize) / ((double) pktsize)) + " pkt");
    
    // GO!
    cout << "Starting simulation" << endl;
    simtime_picosec last_progress_time = 0;
    simtime_picosec progress_interval = timeFromUs((uint32_t)1000000);  // 1 s
    simtime_picosec last_completion_check = 0;
    simtime_picosec completion_check_interval = timeFromUs((uint32_t)10);  // 10 us
    while (eventlist.doNextEvent()) {
        simtime_picosec now = eventlist.now();
        if (now - last_completion_check >= completion_check_interval) {
            bool all_done = true;
            for (auto* src : uec_srcs) {
                if (!src->isTotallyFinished()) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) {
                for (auto* src : pfc_only_srcs) {
                    if (!src->isTotallyFinished()) {
                        all_done = false;
                        break;
                    }
                }
            }
            if (all_done) {
                for (auto* src : dcqcn_srcs) {
                    if (!src->isDone()) {
                        all_done = false;
                        break;
                    }
                }
            }
            if (all_done) {
                for (auto* src : swift_srcs) {
                    if (!src->is_finished()) {
                        all_done = false;
                        break;
                    }
                }
            }
            if (all_done) {
                for (auto* src : mprdma_srcs) {
                    if (!src->is_done()) {
                        all_done = false;
                        break;
                    }
                }
            }
            if (all_done) {
                break;
            }
            last_completion_check = now;
        }
        if (!UecSrc::_quiet && now - last_progress_time >= progress_interval) {
            cout << "Progress: " << timeAsSec(now) << " seconds" << endl;
            last_progress_time = now;
        }
    }

    cout << "Done" << endl;

    if (!mprdma_srcs.empty()) {
        size_t incomplete = 0;
        for (auto* src : mprdma_srcs) {
            if (!src->is_done()) {
                incomplete++;
            }
        }
        if (incomplete > 0) {
            cout << "MPRDMA incomplete flows: " << incomplete
                 << " of " << mprdma_srcs.size() << endl;
        }
    }

    cout << "\n========== Flow Statistics ==========" << endl;
    cout << "Configuration:" << endl;
    cout << "  Foreground CC: " << fg_cc_name(fg_cc_type) << endl;
    cout << "  Background: PFC-only (threshold > " << bg_flowid_threshold << ")" << endl;
    auto ar_name = [](FatTreeSwitch::ArStrategy s) {
        switch (s) {
        case FatTreeSwitch::AR_ECMP:
            return "ECMP";
        case FatTreeSwitch::AR_ADAPTIVE:
            return "Adaptive";
        case FatTreeSwitch::AR_ECMP_ADAPTIVE:
            return "Mixed";
        default:
            return "Unknown";
        }
    };
    cout << "  Foreground AR: " << ar_name(FatTreeSwitch::fg_ar_strategy()) << endl;
    cout << "  Background AR: " << ar_name(FatTreeSwitch::bg_ar_strategy()) << endl;

    uint64_t fg_count = 0;
    uint64_t bg_count = 0;
    auto count_flow = [&](flowid_t flowid) {
        if (flowid <= bg_flowid_threshold) {
            fg_count++;
        } else {
            bg_count++;
        }
    };

    for (auto* src : uec_srcs) {
        count_flow(src->flowId());
    }
    for (auto* src : pfc_only_srcs) {
        count_flow(src->flowId());
    }
    for (auto* src : dcqcn_srcs) {
        count_flow(src->flow_id());
    }
    for (auto* src : swift_srcs) {
        count_flow(src->get_flowid());
    }

    cout << "Flow counts:" << endl;
    cout << "  Foreground: " << fg_count << endl;
    cout << "  Background: " << bg_count << endl;
    if (UecSrc::_sender_cc_algo == UecSrc::MCC_IDEAL) {
        cout << "  MCC-Ideal: " << uec_srcs.size() << endl;
    } else if (UecSrc::_sender_cc_algo == UecSrc::MCC_INCAST) {
        cout << "  MCC-Incast: " << uec_srcs.size() << endl;
    } else {
        cout << "  NSCC: " << uec_srcs.size() << endl;
    }
    cout << "  DCQCN: " << dcqcn_srcs.size() << endl;
    cout << "  Swift: " << swift_srcs.size() << endl;
    cout << "  PFC-only: " << pfc_only_srcs.size() << endl;
    if (dcqcn_no_cc_flows > 0) {
        cout << "  DCQCN no-cc flows: " << dcqcn_no_cc_flows << endl;
    }
    cout << "\nAR Strategy Statistics:" << endl;
    cout << "  Foreground AR: " << ar_name(FatTreeSwitch::fg_ar_strategy()) << endl;
    cout << "  Background AR: " << ar_name(FatTreeSwitch::bg_ar_strategy()) << endl;

    cout << "\nToR 0 Uplink Packet Distribution:" << endl;
    for (int i = 0; i < 32; i++) {
        uint64_t fg_pkts = 0;
        uint64_t bg_pkts = 0;
        auto it_fg = FatTreeSwitch::_fg_uplink_packets.find(i);
        if (it_fg != FatTreeSwitch::_fg_uplink_packets.end()) {
            fg_pkts = it_fg->second;
        }
        auto it_bg = FatTreeSwitch::_bg_uplink_packets.find(i);
        if (it_bg != FatTreeSwitch::_bg_uplink_packets.end()) {
            bg_pkts = it_bg->second;
        }
        cout << "Uplink " << i << ": FG=" << fg_pkts << " pkts, BG=" << bg_pkts << " pkts" << endl;
    }
    if (log_switches || log_tor_upqueue) {
        auto* ft = dynamic_cast<FatTreeTopology*>(topo[0].get());
        if (!ft) {
            cout << "\nQueue Statistics: topology is not FatTreeTopology" << endl;
        } else {
            struct QueueStats {
                mem_b max_q = 0;
                double max_ratio = 0.0;
                uint64_t nonzero = 0;
                uint64_t total = 0;
            };
            auto scan_queues = [](const vector<vector<vector<BaseQueue*>>>& qs) {
                QueueStats s;
                for (const auto& v1 : qs) {
                    for (const auto& v2 : v1) {
                        for (const auto* q : v2) {
                            if (!q) continue;
                            s.total++;
                            mem_b qsize = q->queuesize();
                            if (qsize > 0) s.nonzero++;
                            if (qsize > s.max_q) s.max_q = qsize;
                            mem_b qmax = q->maxsize();
                            if (qmax > 0) {
                                double ratio = (double)qsize / (double)qmax;
                                if (ratio > s.max_ratio) s.max_ratio = ratio;
                            }
                        }
                    }
                }
                return s;
            };
            auto print_stats = [](const char* label, const QueueStats& s) {
                cout << "  " << label << ": max_q " << s.max_q
                     << " max_ratio " << s.max_ratio
                     << " nonzero " << s.nonzero
                     << " total " << s.total << endl;
            };
            cout << "\nQueue Statistics:" << endl;
            print_stats("queues_nlp_ns", scan_queues(ft->queues_nlp_ns));
            print_stats("queues_nup_nlp", scan_queues(ft->queues_nup_nlp));
            print_stats("queues_nlp_nup", scan_queues(ft->queues_nlp_nup));
            print_stats("queues_nup_nc", scan_queues(ft->queues_nup_nc));
            print_stats("queues_nc_nup", scan_queues(ft->queues_nc_nup));
            print_stats("queues_ns_nlp", scan_queues(ft->queues_ns_nlp));
        }
    }
    if (qt == LOSSLESS_INPUT || qt == LOSSLESS_INPUT_ECN) {
        cout << "PFC Statistics:" << endl;
        cout << "  pause_sent: " << LosslessInputQueue::_pause_sent << endl;
        cout << "  pause_cleared: " << LosslessInputQueue::_pause_cleared << endl;
        cout << "  pfc_thresholds: low " << LosslessInputQueue::_low_threshold
             << " high " << LosslessInputQueue::_high_threshold << endl;
    }

    if (auto* ft = dynamic_cast<FatTreeTopology*>(topo[0].get())) {
        cout << "\nToR 0 Uplink Queue Statistics:" << endl;
        int uplink = 0;
        auto& uplinks = ft->queues_nlp_nup[0];
        for (size_t agg = 0; agg < uplinks.size(); agg++) {
            for (size_t b = 0; b < uplinks[agg].size(); b++) {
                if (uplink >= 32) break;
                auto* q = dynamic_cast<Queue*>(uplinks[agg][b]);
                if (q) {
                    cout << "Uplink " << uplink
                         << ": packets_served=" << q->_packets_served
                         << " max_size=" << q->_max_recorded_size
                         << endl;
                } else {
                    cout << "Uplink " << uplink
                         << ": packets_served=NA max_size=NA" << endl;
                }
                uplink++;
            }
            if (uplink >= 32) break;
        }

        cout << "\nToR 0 Uplink Background Flow Counts:" << endl;
        for (int i = 0; i < 32; i++) {
            uint32_t count = 0;
            auto it = FatTreeSwitch::_bg_uplink_counts.find(i);
            if (it != FatTreeSwitch::_bg_uplink_counts.end()) {
                count = it->second;
            }
            cout << "Uplink " << i << ": bg_flows=" << count << endl;
        }

        cout << "\nToR 0 Uplink PFC Pause Counts:" << endl;
        uplink = 0;
        for (size_t agg = 0; agg < uplinks.size(); agg++) {
            for (size_t b = 0; b < uplinks[agg].size(); b++) {
                if (uplink >= 32) break;
                auto* uplink_q = uplinks[agg][b];
                uint64_t sent = 0;
                uint64_t cleared = 0;
                if (uplink_q) {
                    auto* endpoint = uplink_q->getRemoteEndpoint();
                    auto* liq = endpoint ? dynamic_cast<LosslessInputQueue*>(endpoint) : nullptr;
                    if (liq) {
                        auto it_s = LosslessInputQueue::_pause_sent_by_q.find(liq);
                        if (it_s != LosslessInputQueue::_pause_sent_by_q.end()) {
                            sent = it_s->second;
                        }
                        auto it_c = LosslessInputQueue::_pause_cleared_by_q.find(liq);
                        if (it_c != LosslessInputQueue::_pause_cleared_by_q.end()) {
                            cleared = it_c->second;
                        }
                    }
                }
                cout << "Uplink " << uplink << ": pause_sent=" << sent
                     << " pause_cleared=" << cleared << endl;
                uplink++;
            }
            if (uplink >= 32) break;
        }

        if (!ft->switches_c.empty()) {
            cout << "\nSpine PFC Pause Counts:" << endl;
            for (size_t core = 0; core < ft->switches_c.size(); core++) {
                uint64_t sent = 0;
                uint64_t cleared = 0;
                for (size_t agg = 0; agg < ft->queues_nup_nc.size(); agg++) {
                    if (core >= ft->queues_nup_nc[agg].size()) {
                        continue;
                    }
                    for (size_t b = 0; b < ft->queues_nup_nc[agg][core].size(); b++) {
                        auto* q = ft->queues_nup_nc[agg][core][b];
                        if (!q) {
                            continue;
                        }
                        auto* liq = dynamic_cast<LosslessInputQueue*>(q->getRemoteEndpoint());
                        if (!liq) {
                            continue;
                        }
                        auto it_s = LosslessInputQueue::_pause_sent_by_q.find(liq);
                        if (it_s != LosslessInputQueue::_pause_sent_by_q.end()) {
                            sent += it_s->second;
                        }
                        auto it_c = LosslessInputQueue::_pause_cleared_by_q.find(liq);
                        if (it_c != LosslessInputQueue::_pause_cleared_by_q.end()) {
                            cleared += it_c->second;
                        }
                    }
                }
                cout << "Spine " << core << ": pause_sent=" << sent
                     << " pause_cleared=" << cleared << endl;
            }
        }
    }
    int new_pkts = 0, rtx_pkts = 0, bounce_pkts = 0, rts_pkts = 0, ack_pkts = 0, nack_pkts = 0, pull_pkts = 0, sleek_pkts = 0;
    for (size_t ix = 0; ix < uec_srcs.size(); ix++) {
        const struct UecSrc::Stats& s = uec_srcs[ix]->stats();
        new_pkts += s.new_pkts_sent;
        rtx_pkts += s.rtx_pkts_sent;
        rts_pkts += s.rts_pkts_sent;
        bounce_pkts += s.bounces_received;
        ack_pkts += s.acks_received;
        nack_pkts += s.nacks_received;
        pull_pkts += s.pulls_received;
        sleek_pkts += s._sleek_counter;
    }
    
    if (uec_srcs.size() > 0) {
        if (UecSrc::_sender_cc_algo == UecSrc::MCC_IDEAL) {
            cout << "\nUEC/MCC-Ideal Statistics:" << endl;
        } else if (UecSrc::_sender_cc_algo == UecSrc::MCC_INCAST) {
            cout << "\nUEC/MCC-Incast Statistics:" << endl;
        } else {
            cout << "\nUEC/NSCC Statistics:" << endl;
        }
        cout << "  New: " << new_pkts << " Rtx: " << rtx_pkts << " RTS: " << rts_pkts 
             << " Bounced: " << bounce_pkts << " ACKs: " << ack_pkts << " NACKs: " << nack_pkts 
             << " Pulls: " << pull_pkts << " SLEEK: " << sleek_pkts << endl;
    }
    
    // ===== New：DCQCN 统计 =====
    if (dcqcn_srcs.size() > 0) {
        uint32_t total_cnps = 0;
        uint32_t total_acks = 0;
        uint32_t total_nacks = 0;
        uint64_t total_packets_sent = 0;
        
        for (size_t ix = 0; ix < dcqcn_srcs.size(); ix++) {
            total_cnps += dcqcn_srcs[ix]->_cnps_received;
            total_acks += dcqcn_srcs[ix]->_acks_received;
            total_nacks += dcqcn_srcs[ix]->_nacks_received;
            total_packets_sent += dcqcn_srcs[ix]->_packets_sent;
        }
        
        cout << "\nDCQCN Statistics:" << endl;
        cout << "  Packets sent: " << total_packets_sent << endl;
        cout << "  CNPs received: " << total_cnps << endl;
        cout << "  ACKs received: " << total_acks << endl;
        cout << "  NACKs received: " << total_nacks << endl;
    }
    // ============================
    if (swift_srcs.size() > 0) {
        uint64_t total_packets_sent = 0;
        uint64_t total_acks = 0;
        uint64_t total_retransmits = 0;
        uint64_t total_timeouts = 0;
        uint64_t total_rtt_samples = 0;
        simtime_picosec total_rtt = 0;
        mem_b total_cwnd = 0;
        uint64_t fg_swift = 0;
        uint64_t bg_swift = 0;

        for (auto* src : swift_srcs) {
            SwiftSubflowSrc* sub = src->get_subflow();
            if (sub) {
                const auto& stats = sub->get_stats();
                total_packets_sent += stats.packets_sent;
                total_acks += stats.acks_received;
                total_retransmits += stats.retransmits;
                total_timeouts += stats.timeouts;
                total_rtt += stats.total_rtt;
                total_rtt_samples += stats.rtt_samples;
                total_cwnd += sub->get_cwnd();
            }
            if (src->get_flowid() <= bg_flowid_threshold) {
                fg_swift++;
            } else {
                bg_swift++;
            }
        }

        cout << "\nSwift Statistics:" << endl;
        cout << "  Foreground: " << fg_swift << endl;
        cout << "  Background: " << bg_swift << endl;
        cout << "  Total: " << swift_srcs.size() << endl;
        cout << "  Packets sent: " << total_packets_sent << endl;
        cout << "  ACKs received: " << total_acks << endl;
        cout << "  Retransmits: " << total_retransmits << endl;
        cout << "  Timeouts: " << total_timeouts << endl;
        if (total_packets_sent > 0) {
            cout << "  Retransmit rate: "
                 << (double)total_retransmits / total_packets_sent * 100
                 << "%" << endl;
        }
        if (total_rtt_samples > 0) {
            cout << "  Average RTT: " << timeAsUs(total_rtt / total_rtt_samples) << " us" << endl;
        }
        if (!swift_srcs.empty()) {
            cout << "  Average CWND: "
                 << (total_cwnd / swift_srcs.size()) / Packet::data_packet_size()
                 << " packets" << endl;
        }
    }
    if (pfc_only_srcs.size() > 0) {
        cout << "\nPFC-only flows: " << pfc_only_srcs.size() << endl;
    }
    {
        cout << "\n========================================" << endl;
        cout << "      Mixed Traffic Summary" << endl;
        cout << "========================================" << endl;

        const char* cc_names[] = {"NSCC", "DCQCN", "Swift", "MCC-Ideal", "MCC-Incast", "PFC-only"};
        cout << "Configuration:" << endl;
        cout << "  Foreground CC: " << cc_names[fg_cc_type] << endl;
        cout << "  Background CC: PFC-only" << endl;
        cout << "  Threshold: flowid " << bg_flowid_threshold << endl;

        uint64_t fg_nscc = 0;
        uint64_t fg_dcqcn = 0;
        uint64_t fg_swift = 0;
        uint64_t fg_mcc = 0;
        uint64_t fg_mcc_incast = 0;
        uint64_t fg_pfc = 0;
        uint64_t bg_pfc = 0;

        for (auto* src : uec_srcs) {
            if (src->flowId() <= bg_flowid_threshold) {
                if (UecSrc::_sender_cc_algo == UecSrc::MCC_IDEAL) {
                    fg_mcc++;
                } else if (UecSrc::_sender_cc_algo == UecSrc::MCC_INCAST) {
                    fg_mcc_incast++;
                } else {
                    fg_nscc++;
                }
            } else {
                bg_pfc++;
            }
        }
        for (auto* src : pfc_only_srcs) {
            if (src->flowId() <= bg_flowid_threshold) {
                fg_pfc++;
            } else {
                bg_pfc++;
            }
        }
        for (auto* src : dcqcn_srcs) {
            if (src->flow_id() <= bg_flowid_threshold) {
                fg_dcqcn++;
            } else {
                bg_pfc++;
            }
        }
        for (auto* src : swift_srcs) {
            if (src->get_flowid() <= bg_flowid_threshold) {
                fg_swift++;
            } else {
                bg_pfc++;
            }
        }

        cout << "\nFlow Distribution:" << endl;
        cout << "  Foreground flows:" << endl;
        cout << "    NSCC:      " << fg_nscc << endl;
        cout << "    DCQCN:     " << fg_dcqcn << endl;
        cout << "    Swift:     " << fg_swift << endl;
        cout << "    MCC-Ideal: " << fg_mcc << endl;
        cout << "    MCC-Incast:" << fg_mcc_incast << endl;
        cout << "    PFC-only:  " << fg_pfc << endl;
        cout << "    Subtotal:  " << (fg_nscc + fg_dcqcn + fg_swift + fg_mcc + fg_mcc_incast + fg_pfc) << endl;
        cout << "  Background flows:" << endl;
        cout << "    PFC-only: " << bg_pfc << endl;
        cout << "  Total:      " << (fg_nscc + fg_dcqcn + fg_swift + fg_mcc + fg_mcc_incast + fg_pfc + bg_pfc) << endl;

        if ((fg_nscc > 0) + (fg_dcqcn > 0) + (fg_swift > 0) + (fg_mcc > 0) + (fg_mcc_incast > 0) + (fg_pfc > 0) > 1) {
            cout << "\nPerformance Comparison:" << endl;
            cout << "  (Analyze FCT, throughout, and fairness)" << endl;
        }

        cout << "========================================\n" << endl;
    }
    /*
    int new_pkts = 0, rtx_pkts = 0, bounce_pkts = 0, rts_pkts = 0, ack_pkts = 0, nack_pkts = 0, pull_pkts = 0, sleek_pkts = 0;
    for (size_t ix = 0; ix < uec_srcs.size(); ix++) {
        const struct UecSrc::Stats& s = uec_srcs[ix]->stats();
        new_pkts += s.new_pkts_sent;
        rtx_pkts += s.rtx_pkts_sent;
        rts_pkts += s.rts_pkts_sent;
        bounce_pkts += s.bounces_received;
        ack_pkts += s.acks_received;
        nack_pkts += s.nacks_received;
        pull_pkts += s.pulls_received;
        sleek_pkts += s._sleek_counter;
    }
    cout << "New: " << new_pkts << " Rtx: " << rtx_pkts << " RTS: " << rts_pkts << " Bounced: " << bounce_pkts << " ACKs: " << ack_pkts << " NACKs: " << nack_pkts << " Pulls: " << pull_pkts << " sleek_pkts: " << sleek_pkts << endl;
    
    list <const Route*>::iterator rt_i;
    int counts[10]; int hop;
    for (int i = 0; i < 10; i++)
        counts[i] = 0;
    cout << "route count: " << routes.size() << endl;
    for (rt_i = routes.begin(); rt_i != routes.end(); rt_i++) {
        const Route* r = (*rt_i);
        //print_route(*r);
#ifdef PRINTPATHS
        cout << "Path:" << endl;
#endif
        hop = 0;
        for (int i = 0; i < r->size(); i++) {
            PacketSink *ps = r->at(i); 
            CompositeQueue *q = dynamic_cast<CompositeQueue*>(ps);
            if (q == 0) {
#ifdef PRINTPATHS
                cout << ps->nodename() << endl;
#endif
            } else {
#ifdef PRINTPATHS
                cout << q->nodename() << " " << q->num_packets() << "pkts " 
                     << q->num_headers() << "hdrs " << q->num_acks() << "acks " << q->num_nacks() << "nacks " << q->num_stripped() << "stripped"
                     << endl;
#endif
                counts[hop] += q->num_stripped();
                hop++;
            }
        } 
#ifdef PRINTPATHS
        cout << endl;
#endif
    }
    for (int i = 0; i < 10; i++)
        cout << "Hop " << i << " Count " << counts[i] << endl;
    */  

    return EXIT_SUCCESS;
}
