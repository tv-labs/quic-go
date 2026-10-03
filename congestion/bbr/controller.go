package bbr

import "github.com/quic-go/quic-go/congestion"

type controller struct {
	sender *bbrSender
	acked  []AckedPacketInfo
	lost   []LostPacketInfo
}

var _ congestion.Controller = &controller{}

// New is a congestion.NewController for BBRv1.
func New(p congestion.Params) congestion.Controller {
	sender := NewBbrSender(DefaultClock{}, p.InitialMaxDatagramSize, ProfileStandard)
	sender.SetRTTStatsProvider(p.RTTStats)
	return &controller{sender: sender}
}

func (c *controller) OnPacketSent(t congestion.Time, inflight congestion.ByteCount, pn congestion.PacketNumber, bytes congestion.ByteCount, retransmittable bool) {
	c.sender.OnPacketSent(t, inflight, pn, bytes, retransmittable)
}

func (c *controller) OnEvent(e congestion.Event) {
	c.acked, c.lost = c.acked[:0], c.lost[:0]
	for _, p := range e.Acked {
		c.acked = append(c.acked, AckedPacketInfo{PacketNumber: p.PacketNumber, BytesAcked: p.Bytes, ReceivedTime: e.Time})
	}
	for _, p := range e.Lost {
		c.lost = append(c.lost, LostPacketInfo{PacketNumber: p.PacketNumber, BytesLost: p.Bytes})
	}
	c.sender.OnCongestionEventEx(e.PriorInFlight, e.Time, c.acked, c.lost)
}

func (c *controller) OnAppLimited(congestion.ByteCount) {}

func (c *controller) OnRetransmissionTimeout(retransmitted bool) {
	c.sender.OnRetransmissionTimeout(retransmitted)
}

func (c *controller) CanSend(inflight congestion.ByteCount) bool { return c.sender.CanSend(inflight) }
func (c *controller) HasPacingBudget(now congestion.Time) bool   { return c.sender.HasPacingBudget(now) }
func (c *controller) TimeUntilSend(inflight congestion.ByteCount) congestion.Time {
	return c.sender.TimeUntilSend(inflight)
}
func (c *controller) SetMaxDatagramSize(s congestion.ByteCount) { c.sender.SetMaxDatagramSize(s) }
func (c *controller) CongestionWindow() congestion.ByteCount    { return c.sender.GetCongestionWindow() }
func (c *controller) InSlowStart() bool                         { return c.sender.InSlowStart() }
func (c *controller) InRecovery() bool                          { return c.sender.InRecovery() }
