package bbr3

import (
	"fmt"
	"os"
	"strconv"
)

var debugOn = os.Getenv("BBR3_DEBUG") != ""

func (b *bbr) debugRound(rs *rateSample, inflight int64) {
	if !debugOn || !b.roundStart {
		return
	}
	mb := func(v float64) float64 { return v / (1 << 20) }
	lt, st := int64(b.inflightLongterm), int64(b.inflightShortterm)
	if b.inflightLongterm == infiniteBytes {
		lt = -1
	}
	if b.inflightShortterm == infiniteBytes {
		st = -1
	}
	fmt.Fprintf(os.Stderr, "bbr3 r=%d mode=%d maxBw=%.2f bw=%.2f bwST=%.2f pace=%.2f cwnd=%d inflight=%d lt=%d st=%d rate=%.2f lost=%d tx=%d app=%v probe=%v lossRound=%v full=%v minrtt=%s rec=%v\n",
		b.roundCount, b.mode, mb(b.maxBw), mb(b.bw), mb(b.bwShortterm), mb(b.pacingRate), b.cwnd, inflight, lt, st, mb(rs.deliveryRate), rs.lost, rs.txInFlight, rs.isAppLimited, b.isBwProbeSample, b.isLossInRound, b.fullBwReached, b.minRTT, b.inRecovery)
}

var lossThresh = func() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("BBR3_LOSS_THRESH"), 64); err == nil {
		return v
	}
	return lossThreshDefault
}()
