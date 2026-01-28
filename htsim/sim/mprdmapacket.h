// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef MPRDMAPACKET_H
#define MPRDMAPACKET_H

#include <list>
#include "network.h"

class MPRdmaPacket : public Packet {
 public:
    typedef uint32_t psn_t;

    inline static MPRdmaPacket* newpkt(PacketFlow& flow, const Route& route,
                                       psn_t psn, int size, bool retransmitted,
                                       bool synchronise,
                                       uint32_t destination = UINT32_MAX) {
        MPRdmaPacket* p = _packetdb.allocPacket();
        p->set_route(flow, route, size + ACKSIZE, psn);
        p->_type = MPRDMA;
        p->_is_header = false;
        p->_psn = psn;
        p->_msn = 0;
        p->_ipsn = psn;
        p->_is_tail = false;
        p->_needs_completion = false;
        p->_retransmitted = retransmitted;
        p->_synchronise = synchronise;
        p->_path_len = route.size();
        p->_direction = NONE;
        p->set_dst(destination);
        return p;
    }

    inline static MPRdmaPacket* newpkt(PacketFlow& flow,
                                       psn_t psn, int size,
                                       bool retransmitted, bool synchronise,
                                       uint32_t destination = UINT32_MAX) {
        MPRdmaPacket* p = _packetdb.allocPacket();
        p->set_attrs(flow, size + ACKSIZE, psn);
        p->_type = MPRDMA;
        p->_is_header = false;
        p->_psn = psn;
        p->_msn = 0;
        p->_ipsn = psn;
        p->_is_tail = false;
        p->_needs_completion = false;
        p->_retransmitted = retransmitted;
        p->_synchronise = synchronise;
        p->_path_len = 0;
        p->_direction = NONE;
        p->set_dst(destination);
        return p;
    }

    void free() {set_pathid(UINT32_MAX); _packetdb.freePacket(this);}
    virtual ~MPRdmaPacket() {}

    inline psn_t psn() const {return _psn;}
    inline psn_t msn() const {return _msn;}
    inline psn_t ipsn() const {return _ipsn;}
    inline bool is_tail() const {return _is_tail;}
    inline bool needs_completion() const {return _needs_completion;}
    inline bool retransmitted() const {return _retransmitted;}
    inline bool synchronise() const {return _synchronise;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    inline void set_msn(psn_t msn) {_msn = msn;}
    inline void set_ipsn(psn_t ipsn) {_ipsn = ipsn;}
    inline void set_tail(bool is_tail, bool needs_completion) {
        _is_tail = is_tail;
        _needs_completion = needs_completion;
    }
    inline uint32_t path_id() const {if (_pathid != UINT32_MAX) return _pathid; else return _route->path_id();}
    virtual PktPriority priority() const {return Packet::PRIO_LO;}

    const static int ACKSIZE = 64;

 protected:
    psn_t _psn;
    psn_t _msn;
    psn_t _ipsn;
    bool _is_tail;
    bool _needs_completion;
    simtime_picosec _ts;
    bool _retransmitted;
    bool _synchronise;
    static PacketDB<MPRdmaPacket> _packetdb;
};

class MPRdmaAck : public Packet {
 public:
    typedef MPRdmaPacket::psn_t psn_t;

    inline static MPRdmaAck* newpkt(PacketFlow& flow, const Route& route,
                                    psn_t sack_psn, psn_t aack_psn,
                                    uint16_t echo_vp_id, bool ecn_echo,
                                    bool retx_echo, bool sync_echo,
                                    uint32_t destination = UINT32_MAX) {
        MPRdmaAck* p = _packetdb.allocPacket();
        p->set_route(flow, route, MPRdmaPacket::ACKSIZE, sack_psn);
        p->_type = MPRDMAACK;
        p->_is_header = true;
        p->_sack_psn = sack_psn;
        p->_aack_psn = aack_psn;
        p->_ecn_echo = ecn_echo;
        p->_retx_echo = retx_echo;
        p->_sync_echo = sync_echo;
        p->_path_len = 0;
        p->_direction = NONE;
        p->set_dst(destination);
        p->set_pathid(echo_vp_id);
        return p;
    }

    void free() {set_pathid(UINT32_MAX); _packetdb.freePacket(this);}
    virtual ~MPRdmaAck() {}

    inline psn_t sack_psn() const {return _sack_psn;}
    inline psn_t aack_psn() const {return _aack_psn;}
    inline bool ecn_echo() const {return _ecn_echo;}
    inline bool retx_echo() const {return _retx_echo;}
    inline bool sync_echo() const {return _sync_echo;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    virtual PktPriority priority() const {return Packet::PRIO_HI;}

 protected:
    psn_t _sack_psn;
    psn_t _aack_psn;
    bool _ecn_echo;
    bool _retx_echo;
    bool _sync_echo;
    simtime_picosec _ts;
    static PacketDB<MPRdmaAck> _packetdb;
};

class MPRdmaNack : public Packet {
 public:
    typedef MPRdmaPacket::psn_t psn_t;

    inline static MPRdmaNack* newpkt(PacketFlow& flow, const Route& route,
                                     psn_t nack_psn, uint16_t echo_vp_id,
                                     uint32_t destination = UINT32_MAX) {
        MPRdmaNack* p = _packetdb.allocPacket();
        p->set_route(flow, route, MPRdmaPacket::ACKSIZE, nack_psn);
        p->_type = MPRDMANACK;
        p->_is_header = true;
        p->_nack_psn = nack_psn;
        p->_path_len = 0;
        p->_direction = NONE;
        p->set_dst(destination);
        p->set_pathid(echo_vp_id);
        return p;
    }

    void free() {set_pathid(UINT32_MAX); _packetdb.freePacket(this);}
    virtual ~MPRdmaNack() {}

    inline psn_t nack_psn() const {return _nack_psn;}
    inline simtime_picosec ts() const {return _ts;}
    inline void set_ts(simtime_picosec ts) {_ts = ts;}
    virtual PktPriority priority() const {return Packet::PRIO_HI;}

 protected:
    psn_t _nack_psn;
    simtime_picosec _ts;
    static PacketDB<MPRdmaNack> _packetdb;
};

#endif
