package self_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/congestion"
	"github.com/quic-go/quic-go/congestion/bbr"

	"github.com/stretchr/testify/require"
)

type fixedWindowController struct {
	mx          sync.Mutex
	sent        int
	ackedBytes  congestion.ByteCount
	appLimited  int
	controllers int
}

func (c *fixedWindowController) new(congestion.Params) congestion.Controller {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.controllers++
	return c
}

func (c *fixedWindowController) OnPacketSent(congestion.Time, congestion.ByteCount, congestion.PacketNumber, congestion.ByteCount, bool) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.sent++
}

func (c *fixedWindowController) OnEvent(e congestion.Event) {
	c.mx.Lock()
	defer c.mx.Unlock()
	for _, p := range e.Acked {
		c.ackedBytes += p.Bytes
	}
}

func (c *fixedWindowController) OnAppLimited(congestion.ByteCount) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.appLimited++
}

func (c *fixedWindowController) OnRetransmissionTimeout(bool) {}
func (c *fixedWindowController) CanSend(bytesInFlight congestion.ByteCount) bool {
	return bytesInFlight < c.CongestionWindow()
}
func (c *fixedWindowController) HasPacingBudget(congestion.Time) bool               { return true }
func (c *fixedWindowController) TimeUntilSend(congestion.ByteCount) congestion.Time { return 0 }
func (c *fixedWindowController) SetMaxDatagramSize(congestion.ByteCount)            {}
func (c *fixedWindowController) CongestionWindow() congestion.ByteCount             { return 64 << 10 }
func (c *fixedWindowController) InSlowStart() bool                                  { return false }
func (c *fixedWindowController) InRecovery() bool                                   { return false }

func TestExternalCongestionController(t *testing.T) {
	ln, err := quic.Listen(newUDPConnLocalhost(t), getTLSConfig(), getQuicConfig(nil))
	require.NoError(t, err)
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var controller fixedWindowController
	client, err := quic.Dial(ctx, newUDPConnLocalhost(t), ln.Addr(), getTLSClientConfig(), getQuicConfig(&quic.Config{Congestion: controller.new}))
	require.NoError(t, err)
	defer client.CloseWithError(0, "")

	server, err := ln.Accept(ctx)
	require.NoError(t, err)

	data := GeneratePRData(1 << 20)
	str, err := client.OpenUniStreamSync(ctx)
	require.NoError(t, err)
	writeErr := make(chan error, 1)
	go func() {
		_, err := str.Write(data)
		if err == nil {
			err = str.Close()
		}
		writeErr <- err
	}()

	rstr, err := server.AcceptUniStream(ctx)
	require.NoError(t, err)
	received, err := io.ReadAll(rstr)
	require.NoError(t, err)
	require.Equal(t, data, received)
	require.NoError(t, <-writeErr)

	require.Eventually(t, func() bool {
		controller.mx.Lock()
		defer controller.mx.Unlock()
		return controller.ackedBytes >= 1<<20
	}, time.Second, 10*time.Millisecond)

	controller.mx.Lock()
	defer controller.mx.Unlock()
	require.Equal(t, 1, controller.controllers)
	require.Greater(t, controller.sent, (1<<20)/1500)
	require.NotZero(t, controller.appLimited)
}

func TestBBRController(t *testing.T) {
	ln, err := quic.Listen(newUDPConnLocalhost(t), getTLSConfig(), getQuicConfig(nil))
	require.NoError(t, err)
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := quic.Dial(ctx, newUDPConnLocalhost(t), ln.Addr(), getTLSClientConfig(), getQuicConfig(&quic.Config{Congestion: bbr.New}))
	require.NoError(t, err)
	defer client.CloseWithError(0, "")

	server, err := ln.Accept(ctx)
	require.NoError(t, err)

	data := GeneratePRData(4 << 20)
	str, err := client.OpenUniStreamSync(ctx)
	require.NoError(t, err)
	writeErr := make(chan error, 1)
	go func() {
		_, err := str.Write(data)
		if err == nil {
			err = str.Close()
		}
		writeErr <- err
	}()

	rstr, err := server.AcceptUniStream(ctx)
	require.NoError(t, err)
	received, err := io.ReadAll(rstr)
	require.NoError(t, err)
	require.Equal(t, data, received)
	require.NoError(t, <-writeErr)
}
