package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

type (
	ByteCount    = protocol.ByteCount
	PacketNumber = protocol.PacketNumber
	Time         = monotime.Time
)

func Now() Time { return monotime.Now() }

type RTTStats interface {
	MinRTT() time.Duration
	SmoothedRTT() time.Duration
	LatestRTT() time.Duration
	MeanDeviation() time.Duration
	HasMeasurement() bool
}

type AckedPacket struct {
	PacketNumber PacketNumber
	Bytes        ByteCount
}

type LostPacket struct {
	PacketNumber PacketNumber
	Bytes        ByteCount
}

// Event covers one ACK frame or one loss detection timeout. Its slices are
// reused once OnEvent returns.
type Event struct {
	Time          Time
	PriorInFlight ByteCount
	Acked         []AckedPacket
	Lost          []LostPacket
	ECNCongestion bool
}

type Controller interface {
	OnPacketSent(sentTime Time, bytesInFlight ByteCount, pn PacketNumber, bytes ByteCount, isRetransmittable bool)
	OnEvent(Event)
	OnAppLimited(bytesInFlight ByteCount)
	OnRetransmissionTimeout(packetsRetransmitted bool)

	CanSend(bytesInFlight ByteCount) bool
	HasPacingBudget(now Time) bool
	TimeUntilSend(bytesInFlight ByteCount) Time
	SetMaxDatagramSize(ByteCount)

	CongestionWindow() ByteCount
	InSlowStart() bool
	InRecovery() bool
}

type Params struct {
	RTTStats               RTTStats
	InitialMaxDatagramSize ByteCount
}

type NewController func(Params) Controller
