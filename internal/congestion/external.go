package congestion

import (
	"github.com/quic-go/quic-go/congestion"
	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
)

type External struct {
	controller congestion.Controller
	connStats  *utils.ConnectionStats

	event   congestion.Event
	pending bool
}

var _ SendAlgorithmWithDebugInfos = &External{}

func NewExternal(controller congestion.Controller, connStats *utils.ConnectionStats) *External {
	return &External{controller: controller, connStats: connStats}
}

func (e *External) OnPacketSent(sentTime monotime.Time, bytesInFlight protocol.ByteCount, pn protocol.PacketNumber, bytes protocol.ByteCount, isRetransmittable bool) {
	e.controller.OnPacketSent(sentTime, bytesInFlight, pn, bytes, isRetransmittable)
}

func (e *External) OnPacketAcked(pn protocol.PacketNumber, ackedBytes, priorInFlight protocol.ByteCount, _ monotime.Time) {
	e.begin(priorInFlight)
	e.event.Acked = append(e.event.Acked, congestion.AckedPacket{PacketNumber: pn, Bytes: ackedBytes})
}

func (e *External) OnCongestionEvent(pn protocol.PacketNumber, lostBytes, priorInFlight protocol.ByteCount) {
	e.connStats.PacketsLost.Add(1)
	e.connStats.BytesLost.Add(uint64(lostBytes))
	e.begin(priorInFlight)
	e.event.Lost = append(e.event.Lost, congestion.LostPacket{PacketNumber: pn, Bytes: lostBytes})
}

func (e *External) OnECNCongestion(priorInFlight protocol.ByteCount) {
	e.begin(priorInFlight)
	e.event.ECNCongestion = true
}

func (e *External) begin(priorInFlight protocol.ByteCount) {
	if !e.pending {
		e.pending = true
		e.event.PriorInFlight = priorInFlight
	}
}

func (e *External) FinishEvent(now monotime.Time) {
	if !e.pending {
		return
	}
	e.event.Time = now
	e.controller.OnEvent(e.event)
	e.event = congestion.Event{Acked: e.event.Acked[:0], Lost: e.event.Lost[:0]}
	e.pending = false
}

func (e *External) OnAppLimited(bytesInFlight protocol.ByteCount) {
	e.controller.OnAppLimited(bytesInFlight)
}

func (e *External) MaybeExitSlowStart() {}

func (e *External) OnRetransmissionTimeout(packetsRetransmitted bool) {
	e.controller.OnRetransmissionTimeout(packetsRetransmitted)
}

func (e *External) CanSend(bytesInFlight protocol.ByteCount) bool {
	return e.controller.CanSend(bytesInFlight)
}

func (e *External) HasPacingBudget(now monotime.Time) bool {
	return e.controller.HasPacingBudget(now)
}

func (e *External) TimeUntilSend(bytesInFlight protocol.ByteCount) monotime.Time {
	return e.controller.TimeUntilSend(bytesInFlight)
}

func (e *External) SetMaxDatagramSize(size protocol.ByteCount) {
	e.controller.SetMaxDatagramSize(size)
}

func (e *External) GetCongestionWindow() protocol.ByteCount { return e.controller.CongestionWindow() }
func (e *External) InSlowStart() bool                       { return e.controller.InSlowStart() }
func (e *External) InRecovery() bool                        { return e.controller.InRecovery() }
