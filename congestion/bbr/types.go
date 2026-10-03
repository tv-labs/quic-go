package bbr

import (
	"time"

	"github.com/quic-go/quic-go/congestion"
)

const (
	InitialPacketSize          = 1280
	MinPacingDelay             = time.Millisecond
	MaxPacketBufferSize        = 1452
	MinInitialPacketSize       = 1200
	MaxCongestionWindowPackets = 10000
	PacketsPerConnectionID     = 10000
)

type AckedPacketInfo struct {
	PacketNumber congestion.PacketNumber
	BytesAcked   congestion.ByteCount
	ReceivedTime congestion.Time
}

type LostPacketInfo struct {
	PacketNumber congestion.PacketNumber
	BytesLost    congestion.ByteCount
}

type RTTStatsProvider = congestion.RTTStats

type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}
