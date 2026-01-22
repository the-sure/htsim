// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "mcc_incast.h"

#include <algorithm>
#include <cmath>
#include <iostream>

using namespace std;

MccIncastController::MccIncastController()
    : _last_state(MCC_STATE_FREE),
      _has_last_state(false) {}

const char* MccIncastController::stateName(MccState state) {
    switch (state) {
    case MCC_STATE_FREE:
        return "free";
    case MCC_STATE_MILD:
        return "mild";
    case MCC_STATE_CONGESTED:
        return "congested";
    }
    return "unknown";
}

// Default parameters: R1=0.01, R2=0.1, R3=0.001
simtime_picosec MccIncastParams::rtt_threshold = timeFromUs(10u);
double MccIncastParams::R1 = 0.01;
double MccIncastParams::R2 = 0.1;
double MccIncastParams::R3 = 0.001;

void MccIncastParams::initParams(simtime_picosec rtt_thresh,
                                 double r1,
                                 double r2,
                                 double r3) {
    rtt_threshold = rtt_thresh;
    R1 = r1;
    R2 = r2;
    R3 = r3;

    cout << "MCC-Incast parameters:"
         << " rtt_threshold_us=" << timeAsUs(rtt_threshold)
         << " R1=" << R1
         << " R2=" << R2
         << " R3=" << R3
         << endl;
}

bool MccIncastController::onAck(bool ecn_marked,
                                simtime_picosec rtt,
                                uint32_t acked_pkts,
                                mem_b cwnd_bytes,
                                mem_b min_cwnd_bytes,
                                mem_b max_cwnd_bytes,
                                mem_b mss_bytes,
                                mem_b* new_cwnd_bytes) {
    if (acked_pkts == 0 || mss_bytes == 0 || new_cwnd_bytes == nullptr) {
        return false;
    }

    static uint64_t ack_count = 0;
    ack_count += acked_pkts;

    // Classify packets into congested/mild/free states
    if (ecn_marked) {
        _period.congested_cnt += acked_pkts;
    } else if (rtt > MccIncastParams::rtt_threshold) {
        _period.mild_cnt += acked_pkts;
    } else {
        _period.free_cnt += acked_pkts;
    }
    _period.total_samples += acked_pkts;

    if (ecn_marked) {
        cout << "MCC_INCAST_ECN_MARKED ack_num=" << ack_count
             << " rtt_us=" << timeAsUs(rtt)
             << " total_samples=" << _period.total_samples
             << endl;
    } else if (rtt > MccIncastParams::rtt_threshold) {
        cout << "MCC_INCAST_MILD rtt_us=" << timeAsUs(rtt)
             << " threshold_us=" << timeAsUs(MccIncastParams::rtt_threshold)
             << " total_samples=" << _period.total_samples
             << endl;
    } else {
        if (_period.total_samples % 100 == 0) {
            cout << "MCC_INCAST_FREE_SAMPLE total=" << _period.total_samples
                 << " rtt_us=" << timeAsUs(rtt)
                 << endl;
        }
    }

    // Wait for enough samples before updating window
    if (_period.total_samples < MccIncastParams::kMessageLevelPkts) {
        return false;
    }

    cout << "MCC_INCAST_WINDOW_UPDATE"
         << " total_samples=" << _period.total_samples
         << " congested=" << _period.congested_cnt
         << " mild=" << _period.mild_cnt
         << " free=" << _period.free_cnt
         << " ecn_ratio=" << (double)_period.congested_cnt / _period.total_samples
         << " mild_ratio=" << (double)_period.mild_cnt / _period.total_samples
         << endl;

    double cwnd_pkts = static_cast<double>(cwnd_bytes) / static_cast<double>(mss_bytes);
    double min_pkts = static_cast<double>(min_cwnd_bytes) / static_cast<double>(mss_bytes);
    double max_pkts = static_cast<double>(max_cwnd_bytes) / static_cast<double>(mss_bytes);
    if (min_pkts < 1.0) {
        min_pkts = 1.0;
    }

    // MCC-Incast formula:
    // ΔW = W · R1 · free_cnt - W · R2 · congested_cnt + R3 · mild_cnt
    // W_new = min(W + ΔW, BDP)
    double delta_W = cwnd_pkts * MccIncastParams::R1 * _period.free_cnt
                   - cwnd_pkts * MccIncastParams::R2 * _period.congested_cnt
                   + MccIncastParams::R3 * _period.mild_cnt;

    double new_pkts = cwnd_pkts + delta_W;

    // Clamp to [min, max]
    if (new_pkts < min_pkts) {
        new_pkts = min_pkts;
    }
    if (max_pkts > 0.0 && new_pkts > max_pkts) {
        new_pkts = max_pkts;
    }

    mem_b updated_cwnd = static_cast<mem_b>(llround(new_pkts * static_cast<double>(mss_bytes)));
    if (updated_cwnd < min_cwnd_bytes) {
        updated_cwnd = min_cwnd_bytes;
    }
    if (max_cwnd_bytes > 0 && updated_cwnd > max_cwnd_bytes) {
        updated_cwnd = max_cwnd_bytes;
    }

    // Update stats
    if (updated_cwnd > cwnd_bytes) {
        _stats.total_increase += (updated_cwnd - cwnd_bytes);
    } else if (updated_cwnd < cwnd_bytes) {
        _stats.total_decrease += (cwnd_bytes - updated_cwnd);
    }
    _stats.cwnd_updates++;

    // Track state transitions
    MccState state = MCC_STATE_FREE;
    if (_period.congested_cnt > 0) {
        state = MCC_STATE_CONGESTED;
    } else if (_period.mild_cnt > 0) {
        state = MCC_STATE_MILD;
    }
    if (!_has_last_state || state != _last_state) {
        cout << "MCC_INCAST_CONGESTION_STATE"
             << " prev_state " << (_has_last_state ? stateName(_last_state) : "none")
             << " state " << stateName(state)
             << " congested_cnt " << _period.congested_cnt
             << " mild_cnt " << _period.mild_cnt
             << " free_cnt " << _period.free_cnt
             << " total_samples " << _period.total_samples
             << endl;
        _last_state = state;
        _has_last_state = true;
    }

    cout << "MCC_INCAST_CONGESTION_WINDOW"
         << " congested_cnt " << _period.congested_cnt
         << " mild_cnt " << _period.mild_cnt
         << " free_cnt " << _period.free_cnt
         << " total_samples " << _period.total_samples
         << " delta_W " << delta_W
         << " old_cwnd_pkts " << cwnd_pkts
         << " new_cwnd_pkts " << new_pkts
         << " rtt_thresh_us " << timeAsUs(MccIncastParams::rtt_threshold)
         << endl;

    *new_cwnd_bytes = updated_cwnd;
    _period.reset();
    return true;
}
