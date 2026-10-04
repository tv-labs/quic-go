package congestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type unlimited struct{ Controller }

func (unlimited) OnPacketSent(Time, ByteCount, PacketNumber, ByteCount, bool) {}
func (unlimited) HasPacingBudget(Time) bool                                   { return true }
func (unlimited) TimeUntilSend(ByteCount) Time                                { return 0 }
func (unlimited) SetMaxDatagramSize(ByteCount)                                {}

func sendForOneSecond(c Controller) ByteCount {
	start := Now()
	var sent ByteCount
	for now := start; now.Sub(start) < time.Second; now = now.Add(100 * time.Microsecond) {
		for range 1000 {
			if !c.HasPacingBudget(now) {
				break
			}
			c.OnPacketSent(now, 0, 0, 1000, true)
			sent += 1000
		}
	}
	return sent
}

func TestRateCap(t *testing.T) {
	var limit RateCap
	newController := limit.Wrap(func(Params) Controller { return unlimited{} })

	limit.Set(1_000_000)
	require.InDelta(t, 1_000_000, int64(sendForOneSecond(newController(Params{InitialMaxDatagramSize: 1000}))), 20_000)

	limit.Set(0)
	require.Greater(t, sendForOneSecond(newController(Params{InitialMaxDatagramSize: 1000})), ByteCount(100_000_000))
}
