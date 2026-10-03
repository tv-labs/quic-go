package bbr3

import (
	"math"
	"time"

	"github.com/quic-go/quic-go/congestion"
)

const minPacingDelay = time.Millisecond

type pacer struct {
	rate     float64
	burst    congestion.ByteCount
	budget   congestion.ByteCount
	lastSent congestion.Time
	packet   congestion.ByteCount
}

func (p *pacer) budgetAt(now congestion.Time) congestion.ByteCount {
	if p.lastSent.IsZero() {
		return p.burst
	}
	refill := p.rate * now.Sub(p.lastSent).Seconds()
	return congestion.ByteCount(min(float64(p.budget)+refill, float64(p.burst)))
}

func (p *pacer) onSent(t congestion.Time, bytes congestion.ByteCount) {
	p.budget = max(p.budgetAt(t)-bytes, 0)
	p.lastSent = t
}

func (p *pacer) hasBudget(now congestion.Time) bool {
	return p.budgetAt(now) >= p.packet
}

func (p *pacer) nextSendTime() congestion.Time {
	if p.lastSent.IsZero() || p.budget >= p.packet || p.rate <= 0 {
		return 0
	}
	wait := time.Duration(math.Ceil(float64(p.packet-p.budget) / p.rate * float64(time.Second)))
	return p.lastSent.Add(max(wait, minPacingDelay))
}
