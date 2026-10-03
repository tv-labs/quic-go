package ackhandler

import (
	"slices"
	"testing"
	"time"

	cc "github.com/quic-go/quic-go/congestion"
	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/quic-go/quic-go/internal/wire"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type recordingController struct {
	params     cc.Params
	events     []cc.Event
	appLimited []protocol.ByteCount
}

func (c *recordingController) OnPacketSent(monotime.Time, protocol.ByteCount, protocol.PacketNumber, protocol.ByteCount, bool) {
}

func (c *recordingController) OnEvent(e cc.Event) {
	e.Acked = slices.Clone(e.Acked)
	e.Lost = slices.Clone(e.Lost)
	c.events = append(c.events, e)
}

func (c *recordingController) OnAppLimited(bytesInFlight protocol.ByteCount) {
	c.appLimited = append(c.appLimited, bytesInFlight)
}

func (c *recordingController) OnRetransmissionTimeout(bool)                   {}
func (c *recordingController) CanSend(protocol.ByteCount) bool                { return true }
func (c *recordingController) HasPacingBudget(monotime.Time) bool             { return true }
func (c *recordingController) TimeUntilSend(protocol.ByteCount) monotime.Time { return 0 }
func (c *recordingController) SetMaxDatagramSize(protocol.ByteCount)          {}
func (c *recordingController) CongestionWindow() protocol.ByteCount           { return 100_000 }
func (c *recordingController) InSlowStart() bool                              { return false }
func (c *recordingController) InRecovery() bool                               { return false }

func newExternallyControlledHandler(t *testing.T, enc protocol.EncryptionLevel) (SentPacketHandler, *[]*recordingController, func(monotime.Time) protocol.PacketNumber) {
	t.Helper()
	var controllers []*recordingController
	sph := NewSentPacketHandler(
		0,
		1200,
		utils.NewRTTStats(),
		&utils.ConnectionStats{},
		true,
		false,
		nil,
		protocol.PerspectiveServer,
		func(p cc.Params) cc.Controller {
			c := &recordingController{params: p}
			controllers = append(controllers, c)
			return c
		},
		nil,
		utils.DefaultLogger,
	)
	var packets packetTracker
	send := func(ti monotime.Time) protocol.PacketNumber {
		pn := sph.PopPacketNumber(enc)
		sph.SentPacket(ti, pn, protocol.InvalidPacketNumber, nil, []Frame{packets.NewPingFrame(pn)}, enc, protocol.ECNNon, 1000, false, false)
		return pn
	}
	return sph, &controllers, send
}

func TestSentPacketHandlerExternalCongestionOneEventPerAck(t *testing.T) {
	sph, controllers, send := newExternallyControlledHandler(t, protocol.EncryptionInitial)
	require.Len(t, *controllers, 1)
	require.EqualValues(t, 1200, (*controllers)[0].params.InitialMaxDatagramSize)
	require.NotNil(t, (*controllers)[0].params.RTTStats)

	now := monotime.Now()
	var pns []protocol.PacketNumber
	for range 5 {
		pns = append(pns, send(now))
	}

	ackTime := now.Add(time.Second)
	_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns[3], pns[4])}, protocol.EncryptionInitial, ackTime)
	require.NoError(t, err)

	require.Equal(t, []cc.Event{{
		Time:          ackTime,
		PriorInFlight: 5000,
		Acked:         []cc.AckedPacket{{PacketNumber: pns[3], Bytes: 1000}, {PacketNumber: pns[4], Bytes: 1000}},
		Lost:          []cc.LostPacket{{PacketNumber: pns[0], Bytes: 1000}, {PacketNumber: pns[1], Bytes: 1000}},
	}}, (*controllers)[0].events)
}

func TestSentPacketHandlerExternalCongestionLossTimeout(t *testing.T) {
	sph, controllers, send := newExternallyControlledHandler(t, protocol.EncryptionInitial)

	now := monotime.Now()
	pn1 := send(now.Add(-time.Second))
	pn2 := send(now.Add(-10 * time.Millisecond))
	pn3 := send(now)

	_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pn3)}, protocol.EncryptionInitial, now.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, (*controllers)[0].events, 1)

	timeout := sph.GetLossDetectionTimeout()
	require.NoError(t, sph.OnLossDetectionTimeout(timeout))

	events := (*controllers)[0].events
	require.Len(t, events, 2)
	require.Equal(t, []cc.LostPacket{{PacketNumber: pn1, Bytes: 1000}}, events[0].Lost)
	require.Equal(t, timeout, events[1].Time)
	require.EqualValues(t, 1000, events[1].PriorInFlight)
	require.Empty(t, events[1].Acked)
	require.Equal(t, []cc.LostPacket{{PacketNumber: pn2, Bytes: 1000}}, events[1].Lost)
}

func TestSentPacketHandlerExternalCongestionECN(t *testing.T) {
	sph, controllers, send := newExternallyControlledHandler(t, protocol.Encryption1RTT)
	ecnHandler := NewMockECNHandler(gomock.NewController(t))
	ecnHandler.EXPECT().SentPacket(gomock.Any(), gomock.Any()).AnyTimes()
	ecnHandler.EXPECT().HandleNewlyAcked(gomock.Any(), int64(0), int64(0), int64(1)).Return(true)
	sph.(*sentPacketHandler).ecnTracker = ecnHandler

	now := monotime.Now()
	pn := send(now)
	_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pn), ECNCE: 1}, protocol.Encryption1RTT, now.Add(10*time.Millisecond))
	require.NoError(t, err)

	events := (*controllers)[0].events
	require.Len(t, events, 1)
	require.True(t, events[0].ECNCongestion)
	require.Empty(t, events[0].Lost)
	require.Equal(t, []cc.AckedPacket{{PacketNumber: pn, Bytes: 1000}}, events[0].Acked)
}

func TestSentPacketHandlerExternalCongestionAppLimited(t *testing.T) {
	sph, controllers, send := newExternallyControlledHandler(t, protocol.EncryptionInitial)
	send(monotime.Now())
	send(monotime.Now())

	sph.OnAppLimited()

	require.Equal(t, []protocol.ByteCount{2000}, (*controllers)[0].appLimited)
}

func TestSentPacketHandlerExternalCongestionPathMigration(t *testing.T) {
	sph, controllers, _ := newExternallyControlledHandler(t, protocol.Encryption1RTT)

	sph.MigratedPath(monotime.Now(), 1300)

	require.Len(t, *controllers, 2)
	require.EqualValues(t, 1300, (*controllers)[1].params.InitialMaxDatagramSize)
}

func TestSentPacketHandlerExternalCongestionCountsLosses(t *testing.T) {
	sph, _, send := newExternallyControlledHandler(t, protocol.EncryptionInitial)
	now := monotime.Now()
	var pns []protocol.PacketNumber
	for range 5 {
		pns = append(pns, send(now))
	}

	_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns[3], pns[4])}, protocol.EncryptionInitial, now.Add(time.Second))
	require.NoError(t, err)

	stats := sph.(*sentPacketHandler).connStats
	require.EqualValues(t, 2, stats.PacketsLost.Load())
	require.EqualValues(t, 2000, stats.BytesLost.Load())
}
