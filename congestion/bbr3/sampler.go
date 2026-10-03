package bbr3

import (
	"time"

	"github.com/quic-go/quic-go/congestion"
)

type sentPacket struct {
	size          congestion.ByteCount
	sendTime      congestion.Time
	delivered     congestion.ByteCount
	deliveredTime congestion.Time
	firstSendTime congestion.Time
	lost          congestion.ByteCount
	txInFlight    congestion.ByteCount
	isAppLimited  bool
}

type rateSample struct {
	hasData        bool
	deliveryRate   float64
	isAppLimited   bool
	interval       time.Duration
	delivered      congestion.ByteCount
	priorDelivered congestion.ByteCount
	sendElapsed    time.Duration
	ackElapsed     time.Duration
	lastAckedPN    congestion.PacketNumber
	newestSendTime congestion.Time
	rtt            time.Duration
	newlyAcked     congestion.ByteCount
	newlyLost      congestion.ByteCount
	txInFlight     congestion.ByteCount
	priorLost      congestion.ByteCount
	lost           congestion.ByteCount
}

type sampler struct {
	delivered     congestion.ByteCount
	deliveredTime congestion.Time
	firstSendTime congestion.Time
	appLimited    congestion.ByteCount
	lost          congestion.ByteCount
	minRTT        time.Duration
	packets       map[congestion.PacketNumber]sentPacket
}

func newSampler() sampler {
	return sampler{minRTT: infiniteRTT, packets: make(map[congestion.PacketNumber]sentPacket)}
}

func (s *sampler) onPacketSent(t congestion.Time, inflightBefore, inflight congestion.ByteCount, pn congestion.PacketNumber, size congestion.ByteCount) {
	if inflightBefore == 0 {
		s.firstSendTime, s.deliveredTime = t, t
	}
	s.packets[pn] = sentPacket{
		size:          size,
		sendTime:      t,
		delivered:     s.delivered,
		deliveredTime: s.deliveredTime,
		firstSendTime: s.firstSendTime,
		lost:          s.lost,
		txInFlight:    inflight,
		isAppLimited:  s.appLimited != 0,
	}
}

func (s *sampler) onAcked(rs *rateSample, now congestion.Time, pn congestion.PacketNumber) bool {
	p, ok := s.packets[pn]
	if !ok {
		return false
	}
	delete(s.packets, pn)

	s.delivered += p.size
	s.deliveredTime = now
	rs.newlyAcked += p.size

	if !rs.hasData || p.sendTime.After(rs.newestSendTime) || (p.sendTime == rs.newestSendTime && pn > rs.lastAckedPN) {
		rs.hasData = true
		rs.priorDelivered = p.delivered
		rs.isAppLimited = p.isAppLimited
		rs.sendElapsed = p.sendTime.Sub(p.firstSendTime)
		rs.ackElapsed = s.deliveredTime.Sub(p.deliveredTime)
		rs.lastAckedPN = pn
		rs.newestSendTime = p.sendTime
		rs.txInFlight = p.txInFlight
		rs.priorLost = p.lost
		rs.rtt = now.Sub(p.sendTime)
		s.firstSendTime = p.sendTime
	}
	return true
}

func (s *sampler) onLost(pn congestion.PacketNumber) (sentPacket, bool) {
	p, ok := s.packets[pn]
	if !ok {
		return sentPacket{}, false
	}
	delete(s.packets, pn)
	s.lost += p.size
	return p, true
}

func (s *sampler) generate(rs *rateSample) {
	if s.appLimited != 0 && s.delivered > s.appLimited {
		s.appLimited = 0
	}
	if !rs.hasData {
		return
	}
	s.minRTT = min(s.minRTT, rs.rtt)
	rs.lost = s.lost - rs.priorLost
	rs.interval = max(rs.sendElapsed, rs.ackElapsed)
	rs.delivered = s.delivered - rs.priorDelivered
	if rs.interval < s.minRTT || rs.interval <= 0 {
		return
	}
	rs.deliveryRate = float64(rs.delivered) / rs.interval.Seconds()
}

func (s *sampler) markAppLimited(inflight congestion.ByteCount) {
	s.appLimited = max(s.delivered+inflight, 1)
}

func (s *sampler) forgetOlderThan(cutoff congestion.Time) {
	for pn, p := range s.packets {
		if p.sendTime.Before(cutoff) {
			delete(s.packets, pn)
		}
	}
}
