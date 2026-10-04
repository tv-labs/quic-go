package congestion

import (
	"math"
	"sync/atomic"
	"time"
)

const rateCapBurst = 10 * time.Millisecond

// RateCap bounds the send rate of the controllers it wraps. It can be changed
// at any time, from any goroutine.
type RateCap struct {
	bytesPerSecond atomic.Uint64
}

// Set caps the send rate at bytesPerSecond, or lifts the cap if it is 0.
func (r *RateCap) Set(bytesPerSecond uint64) { r.bytesPerSecond.Store(bytesPerSecond) }

func (r *RateCap) Wrap(newController NewController) NewController {
	return func(p Params) Controller {
		return &rateCapped{Controller: newController(p), limit: r, packet: p.InitialMaxDatagramSize}
	}
}

type rateCapped struct {
	Controller
	limit    *RateCap
	packet   ByteCount
	budget   float64
	lastSent Time
}

func (c *rateCapped) rate() float64 { return float64(c.limit.bytesPerSecond.Load()) }

func (c *rateCapped) budgetAt(now Time, rate float64) float64 {
	burst := max(rate*rateCapBurst.Seconds(), float64(2*c.packet))
	if c.lastSent.IsZero() {
		return burst
	}
	return min(c.budget+rate*now.Sub(c.lastSent).Seconds(), burst)
}

func (c *rateCapped) OnPacketSent(t Time, inflight ByteCount, pn PacketNumber, bytes ByteCount, retransmittable bool) {
	if rate := c.rate(); rate > 0 {
		c.budget = c.budgetAt(t, rate) - float64(bytes)
	}
	c.lastSent = t
	c.Controller.OnPacketSent(t, inflight, pn, bytes, retransmittable)
}

func (c *rateCapped) HasPacingBudget(now Time) bool {
	if rate := c.rate(); rate > 0 && c.budgetAt(now, rate) < float64(c.packet) {
		return false
	}
	return c.Controller.HasPacingBudget(now)
}

func (c *rateCapped) TimeUntilSend(inflight ByteCount) Time {
	next := c.Controller.TimeUntilSend(inflight)
	rate := c.rate()
	if rate == 0 || c.lastSent.IsZero() || c.budget >= float64(c.packet) {
		return next
	}
	wait := time.Duration(math.Ceil((float64(c.packet) - c.budget) / rate * float64(time.Second)))
	if capped := c.lastSent.Add(max(wait, time.Millisecond)); capped.After(next) {
		return capped
	}
	return next
}

func (c *rateCapped) SetMaxDatagramSize(size ByteCount) {
	c.packet = size
	c.Controller.SetMaxDatagramSize(size)
}
