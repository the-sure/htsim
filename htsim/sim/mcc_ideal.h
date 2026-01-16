// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef MCC_IDEAL_H
#define MCC_IDEAL_H

#include <cstdint>

#include "config.h"

// MCC (Message-level Congestion Control) configuration.
struct MccIdealParams {
    static const uint32_t kMessageLevelPkts = 64;

    // RTT threshold to separate mild/free states.
    static simtime_picosec rtt_threshold;

    // Rate adjustment coefficients (in packets).
    static double R1;
    static double R2;
    static double R3;

    static void initParams(simtime_picosec rtt_thresh, double r1, double r2, double r3);
};

// Per-period counters (message-level window).
struct MccIdealPeriod {
    uint32_t congested_cnt = 0;
    uint32_t mild_cnt = 0;
    uint32_t free_cnt = 0;
    uint32_t total_samples = 0;

    void reset() {
        congested_cnt = 0;
        mild_cnt = 0;
        free_cnt = 0;
        total_samples = 0;
    }
};

// Lifetime stats.
struct MccIdealStats {
    uint32_t cwnd_updates = 0;
    mem_b total_increase = 0;
    mem_b total_decrease = 0;
};

// MCC-Ideal controller (message-level update on ACK samples).
class MccIdealController {
public:
    MccIdealController();

    bool onAck(bool ecn_marked,
               simtime_picosec rtt,
               uint32_t acked_pkts,
               mem_b cwnd_bytes,
               mem_b min_cwnd_bytes,
               mem_b max_cwnd_bytes,
               mem_b mss_bytes,
               mem_b* new_cwnd_bytes);

    const MccIdealPeriod& period() const { return _period; }
    const MccIdealStats& stats() const { return _stats; }

    void resetPeriod() { _period.reset(); }

private:
    enum MccState {
        MCC_STATE_FREE = 0,
        MCC_STATE_MILD = 1,
        MCC_STATE_CONGESTED = 2
    };

    static const char* stateName(MccState state);

    MccIdealPeriod _period;
    MccIdealStats _stats;
    MccState _last_state;
    bool _has_last_state;
};

#endif  // MCC_IDEAL_H
