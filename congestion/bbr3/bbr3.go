// Package bbr3 is BBRv3 congestion control, after draft-ietf-ccwg-bbr-06.
package bbr3

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/quic-go/quic-go/congestion"
)

type mode uint8

const (
	startup mode = iota
	drain
	probeBWDown
	probeBWCruise
	probeBWRefill
	probeBWUp
	probeRTT
)

type ackPhase uint8

const (
	acksInit ackPhase = iota
	acksRefilling
	acksProbeStarting
	acksProbeFeedback
	acksProbeStopping
)

const (
	startupPacingGain   = 2.77
	drainPacingGain     = 0.5
	probeDownPacingGain = 0.9
	probeUpPacingGain   = 1.25
	defaultCwndGain     = 2.0
	probeUpCwndGain     = 2.25
	probeRTTCwndGain    = 0.5
	pacingMargin        = 0.99

	lossThreshDefault  = 0.02
	beta               = 0.7
	headroom           = 0.15
	fullBwGrowth       = 1.25
	fullBwRounds       = 3
	startupFullLossCnt = 6

	extraAckedFilterLen = 10
	minRTTFilterLen     = 10 * time.Second
	probeRTTDuration    = 200 * time.Millisecond
	probeRTTInterval    = 5 * time.Second
	maxRenoProbeRounds  = 63
	maxProbeUpRounds    = 30
	minQueueDelay       = 10 * time.Millisecond
	policedLossThresh   = 0.15

	initialCwndPackets = 32
	minPipeCwndPackets = 4
	maxSendQuantum     = 64 << 10
	forgetUnackedAfter = 3 * minRTTFilterLen
)

const (
	infiniteBytes = congestion.ByteCount(math.MaxInt64)
	infiniteRTT   = time.Duration(math.MaxInt64)
)

var infiniteBw = math.Inf(1)

type bbr struct {
	rtt     congestion.RTTStats
	rng     *rand.Rand
	sampler sampler
	pacer   pacer

	smss        congestion.ByteCount
	initialCwnd congestion.ByteCount
	cwnd        congestion.ByteCount
	pacingRate  float64
	sendQuantum congestion.ByteCount
	started     bool
	largestSent congestion.PacketNumber

	isCwndLimited        bool
	cwndLimitedThisRound bool

	inRecovery  bool
	recoveryEnd congestion.PacketNumber

	mode       mode
	pacingGain float64
	cwndGain   float64

	roundCount         uint64
	roundStart         bool
	nextRoundDelivered congestion.ByteCount
	idleRestart        bool
	drainStartRound    uint64

	bwHi        [2]float64
	maxBw       float64
	bwShortterm float64
	bw          float64

	minRTT              time.Duration
	minRTTStamp         congestion.Time
	bdp                 congestion.ByteCount
	extraAcked          congestion.ByteCount
	maxInflight         congestion.ByteCount
	inflightLongterm    congestion.ByteCount
	inflightShortterm   congestion.ByteCount
	extraAckedFilter    maxFilter
	extraAckedStart     congestion.Time
	extraAckedDelivered congestion.ByteCount

	bwLatest           float64
	inflightLatest     congestion.ByteCount
	isLossInRound      bool
	lossRoundStart     bool
	lossRoundDelivered congestion.ByteCount
	lossEventsInRound  int

	fullBw        float64
	fullBwCount   int
	fullBwNow     bool
	fullBwReached bool

	ackPhase               ackPhase
	isBwProbeSample        bool
	bwProbeUpAcked         congestion.ByteCount
	probeUpAckedPerInc     congestion.ByteCount
	bwProbeUpRounds        uint
	roundsSinceProbeUp     uint64
	bwProbeWait            time.Duration
	cycleStamp             congestion.Time
	prevProbeTooHigh       bool
	prevProbePrecautionary bool

	priorCwnd             congestion.ByteCount
	undoMode              mode
	undoValid             bool
	undoBwShortterm       float64
	undoInflightShortterm congestion.ByteCount
	undoInflightLongterm  congestion.ByteCount

	roundMinRTT     time.Duration
	prevRoundMinRTT time.Duration

	probeRTTMinDelay  time.Duration
	probeRTTMinStamp  congestion.Time
	probeRTTExpired   bool
	probeRTTDoneStamp congestion.Time
	probeRTTRoundDone bool
}

var _ congestion.Controller = &bbr{}

func New(p congestion.Params) congestion.Controller {
	b := &bbr{
		rtt:                p.RTTStats,
		rng:                rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		sampler:            newSampler(),
		smss:               p.InitialMaxDatagramSize,
		initialCwnd:        initialCwndPackets * p.InitialMaxDatagramSize,
		minRTT:             infiniteRTT,
		probeRTTMinDelay:   infiniteRTT,
		roundMinRTT:        infiniteRTT,
		prevRoundMinRTT:    infiniteRTT,
		inflightLongterm:   infiniteBytes,
		probeUpAckedPerInc: infiniteBytes,
		largestSent:        -1,
	}
	b.cwnd = b.initialCwnd
	if p.RTTStats.HasMeasurement() {
		b.minRTT = p.RTTStats.SmoothedRTT()
	}
	b.resetCongestionSignals()
	b.resetShortTermModel()
	b.resetFullBw()
	b.initPacingRate()
	b.enterStartup()
	b.setSendQuantum()
	return b
}

func (b *bbr) minPipeCwnd() congestion.ByteCount { return minPipeCwndPackets * b.smss }

func (b *bbr) OnPacketSent(t congestion.Time, inflight congestion.ByteCount, pn congestion.PacketNumber, size congestion.ByteCount, retransmittable bool) {
	if !retransmittable {
		return
	}
	if !b.started {
		b.started = true
		b.minRTTStamp, b.probeRTTMinStamp, b.extraAckedStart, b.cycleStamp = t, t, t, t
	}
	inflightBefore := inflight - size
	b.handleRestartFromIdle(t, inflightBefore)
	b.sampler.onPacketSent(t, inflightBefore, inflight, pn, size)
	b.pacer.onSent(t, size)
	b.largestSent = max(b.largestSent, pn)
	if inflight+b.smss > b.cwnd {
		b.cwndLimitedThisRound = true
		b.isCwndLimited = true
	}
}

func (b *bbr) OnAppLimited(inflight congestion.ByteCount) {
	if inflight < b.cwnd {
		b.sampler.markAppLimited(inflight)
	}
}

func (b *bbr) OnEvent(e congestion.Event) {
	rs := rateSample{rtt: -1}
	for _, a := range e.Acked {
		b.sampler.onAcked(&rs, e.Time, a.PacketNumber)
	}
	if rs.hasData {
		b.roundMinRTT = min(b.roundMinRTT, rs.rtt)
	}
	if b.inRecovery && rs.hasData && rs.lastAckedPN > b.recoveryEnd {
		b.inRecovery = false
		b.restoreCwnd()
	}
	for _, l := range e.Lost {
		if p, ok := b.sampler.onLost(l.PacketNumber); ok {
			rs.newlyLost += p.size
			b.handleLostPacket(p)
		}
		if !b.inRecovery || l.PacketNumber > b.recoveryEnd {
			b.inRecovery = true
			b.recoveryEnd = b.largestSent
		}
	}
	if e.ECNCongestion {
		b.noteLoss()
	}
	if rs.newlyAcked == 0 {
		return
	}

	inflight := max(e.PriorInFlight-rs.newlyAcked-rs.newlyLost, 0)
	firstRTTSample := rs.hasData && b.sampler.minRTT == infiniteRTT
	b.sampler.generate(&rs)
	if firstRTTSample {
		b.pacingRate = startupPacingGain * float64(b.initialCwnd) / rs.rtt.Seconds()
		b.pacer.rate = b.pacingRate
	}
	b.updateModelAndState(e.Time, inflight, &rs)
	b.updateControlParameters(&rs)
	b.debugRound(&rs, int64(inflight))
}

func (b *bbr) OnRetransmissionTimeout(packetsRetransmitted bool) {
	if !packetsRetransmitted {
		return
	}
	b.saveCwnd()
	b.saveStateUponLoss()
	b.cwnd = b.minPipeCwnd()
}

func (b *bbr) CanSend(inflight congestion.ByteCount) bool { return inflight < b.cwnd }

func (b *bbr) HasPacingBudget(now congestion.Time) bool { return b.pacer.hasBudget(now) }

func (b *bbr) TimeUntilSend(congestion.ByteCount) congestion.Time { return b.pacer.nextSendTime() }

func (b *bbr) SetMaxDatagramSize(size congestion.ByteCount) {
	b.smss = size
	b.pacer.packet = size
	b.cwnd = max(b.cwnd, b.minPipeCwnd())
	b.setSendQuantum()
}

func (b *bbr) CongestionWindow() congestion.ByteCount { return b.cwnd }
func (b *bbr) InSlowStart() bool                      { return b.mode == startup }
func (b *bbr) InRecovery() bool                       { return b.inRecovery }

func (b *bbr) updateModelAndState(now congestion.Time, inflight congestion.ByteCount, rs *rateSample) {
	b.updateLatestDeliverySignals(rs)
	b.updateCongestionSignals(now, rs)
	b.updateACKAggregation(now, rs)
	b.checkFullBwReached(rs)
	b.checkStartupDone(rs)
	b.checkDrainDone(now, inflight)
	b.updateProbeBWCyclePhase(now, inflight, rs)
	b.updateMinRTT(now, rs)
	b.checkProbeRTT(now, inflight, rs)
	b.advanceLatestDeliverySignals(rs)
	b.boundBwForModel()
}

func (b *bbr) updateControlParameters(rs *rateSample) {
	b.setPacingRate()
	b.setSendQuantum()
	b.setCwnd(rs)
}

func (b *bbr) enterStartup() {
	b.mode = startup
	b.pacingGain = startupPacingGain
	b.cwndGain = defaultCwndGain
}

func (b *bbr) resetFullBw() {
	b.fullBw = 0
	b.fullBwCount = 0
	b.fullBwNow = false
}

func (b *bbr) checkFullBwReached(rs *rateSample) {
	if b.fullBwNow || !b.roundStart || rs.isAppLimited {
		return
	}
	if rs.deliveryRate >= b.fullBw*fullBwGrowth {
		b.resetFullBw()
		b.fullBw = rs.deliveryRate
		return
	}
	b.fullBwCount++
	b.fullBwNow = b.fullBwCount >= fullBwRounds
	if b.fullBwNow {
		b.fullBwReached = true
	}
}

func (b *bbr) checkStartupDone(rs *rateSample) {
	b.checkStartupHighLoss(rs)
	if b.mode == startup && b.fullBwReached {
		b.enterDrain()
	}
}

func (b *bbr) checkStartupHighLoss(rs *rateSample) {
	if b.fullBwReached {
		return
	}
	if rs.newlyLost > 0 {
		b.lossEventsInRound++
	}
	if b.lossRoundStart && b.lossEventsInRound >= startupFullLossCnt && b.isCongestiveLoss(rs.lost, rs.txInFlight) {
		b.undoMode, b.undoValid = startup, true
		b.fullBwReached, b.fullBwNow = true, true
		b.inflightLongterm = max(b.bdpMultiple(b.maxBw, 1), b.inflightLatest)
		b.enterDrain()
	}
	if b.lossRoundStart {
		b.lossEventsInRound = 0
	}
}

func (b *bbr) enterDrain() {
	b.mode = drain
	b.pacingGain = drainPacingGain
	b.cwndGain = defaultCwndGain
	b.drainStartRound = b.roundCount
}

func (b *bbr) checkDrainDone(now congestion.Time, inflight congestion.ByteCount) {
	if b.mode == drain && (inflight <= b.inflight(b.bw, 1) || b.roundCount > b.drainStartRound+3) {
		b.enterProbeBW(now)
	}
}

func (b *bbr) enterProbeBW(now congestion.Time) {
	b.cwndGain = defaultCwndGain
	b.startProbeBWDown(now)
}

func (b *bbr) startProbeBWDown(now congestion.Time) {
	b.resetCongestionSignals()
	b.probeUpAckedPerInc = infiniteBytes
	b.pickProbeWait()
	b.cycleStamp = now
	b.ackPhase = acksProbeStopping
	b.startRound()
	b.mode = probeBWDown
	b.pacingGain = probeDownPacingGain
	b.cwndGain = defaultCwndGain
}

func (b *bbr) startProbeBWCruise() {
	b.mode = probeBWCruise
	b.pacingGain = 1
	b.cwndGain = defaultCwndGain
}

func (b *bbr) startProbeBWRefill() {
	b.resetShortTermModel()
	b.bwProbeUpRounds = 0
	b.bwProbeUpAcked = 0
	b.prevProbePrecautionary = false
	b.ackPhase = acksRefilling
	b.startRound()
	b.mode = probeBWRefill
	b.pacingGain = 1
	b.cwndGain = defaultCwndGain
}

func (b *bbr) startProbeBWUp(rs *rateSample) {
	b.ackPhase = acksProbeStarting
	b.startRound()
	b.resetFullBw()
	b.fullBw = rs.deliveryRate
	b.mode = probeBWUp
	b.pacingGain = probeUpPacingGain
	b.cwndGain = probeUpCwndGain
	b.raiseInflightLongtermSlope()
}

func (b *bbr) updateProbeBWCyclePhase(now congestion.Time, inflight congestion.ByteCount, rs *rateSample) {
	if !b.fullBwReached || b.adaptLongTermModel(rs) || !b.inProbeBW() {
		return
	}
	switch b.mode {
	case probeBWDown:
		if b.isTimeToProbeBW(now) {
			return
		}
		if b.isTimeToCruise(inflight) {
			b.startProbeBWCruise()
		}
	case probeBWCruise:
		b.isTimeToProbeBW(now)
	case probeBWRefill:
		if b.roundStart {
			b.isBwProbeSample = true
			b.startProbeBWUp(rs)
		}
	case probeBWUp:
		if b.isTimeToGoDown(inflight, rs) {
			b.prevProbeTooHigh = false
			b.startProbeBWDown(now)
		}
	}
}

func (b *bbr) inProbeBW() bool {
	return b.mode >= probeBWDown && b.mode <= probeBWUp
}

func (b *bbr) isProbingBw() bool {
	return b.mode == startup || b.mode == probeBWRefill || b.mode == probeBWUp
}

func (b *bbr) isTimeToProbeBW(now congestion.Time) bool {
	if now.Sub(b.cycleStamp) > b.bwProbeWait || b.isRenoCoexistenceProbeTime() {
		b.startProbeBWRefill()
		return true
	}
	return false
}

func (b *bbr) pickProbeWait() {
	b.roundsSinceProbeUp = uint64(b.rng.IntN(2))
	b.bwProbeWait = 2*time.Second + time.Duration(b.rng.Float64()*float64(time.Second))
}

func (b *bbr) isRenoCoexistenceProbeTime() bool {
	renoRounds := uint64(min(b.bdp, b.cwnd) / b.smss)
	return b.roundsSinceProbeUp >= min(renoRounds, maxRenoProbeRounds)
}

func (b *bbr) isTimeToCruise(inflight congestion.ByteCount) bool {
	return inflight <= b.inflightWithHeadroom() && inflight <= b.inflight(b.maxBw, 1)
}

func (b *bbr) isTimeToGoDown(inflight congestion.ByteCount, rs *rateSample) bool {
	if b.prevProbeTooHigh && inflight >= b.inflightLongterm {
		b.prevProbePrecautionary = true
		return true
	}
	if b.isCwndLimited && b.cwnd >= b.inflightLongterm {
		b.resetFullBw()
		b.fullBw = rs.deliveryRate
		return false
	}
	return b.fullBwNow
}

func (b *bbr) inflightWithHeadroom() congestion.ByteCount {
	if b.inflightLongterm == infiniteBytes {
		return infiniteBytes
	}
	room := max(b.smss, congestion.ByteCount(headroom*float64(b.inflightLongterm)))
	return max(b.inflightLongterm-room, b.minPipeCwnd())
}

func (b *bbr) raiseInflightLongtermSlope() {
	growth := congestion.ByteCount(1) << b.bwProbeUpRounds
	b.bwProbeUpRounds = min(b.bwProbeUpRounds+1, maxProbeUpRounds)
	b.probeUpAckedPerInc = max(b.cwnd/growth, b.smss)
}

func (b *bbr) probeInflightLongtermUpward(rs *rateSample) {
	if !b.isCwndLimited || b.cwnd < b.inflightLongterm {
		return
	}
	b.bwProbeUpAcked += rs.newlyAcked
	if b.bwProbeUpAcked >= b.probeUpAckedPerInc {
		delta := b.bwProbeUpAcked / b.probeUpAckedPerInc
		b.bwProbeUpAcked -= delta * b.probeUpAckedPerInc
		b.inflightLongterm += delta * b.smss
	}
	if b.roundStart {
		b.raiseInflightLongtermSlope()
	}
}

func (b *bbr) adaptLongTermModel(rs *rateSample) bool {
	if b.ackPhase == acksProbeStarting && b.roundStart {
		b.ackPhase = acksProbeFeedback
	}
	if b.ackPhase == acksProbeStopping && b.roundStart {
		b.isBwProbeSample = false
		b.ackPhase = acksInit
		if b.inProbeBW() && !rs.isAppLimited {
			b.advanceMaxBwFilter()
		}
		if b.inProbeBW() && b.prevProbePrecautionary && !b.prevProbeTooHigh {
			b.startProbeBWRefill()
			return true
		}
	}
	if b.isCongestiveLoss(rs.lost, rs.txInFlight) || b.inflightLongterm == infiniteBytes {
		return false
	}
	b.inflightLongterm = max(b.inflightLongterm, rs.txInFlight)
	if b.mode == probeBWUp {
		b.probeInflightLongtermUpward(rs)
	}
	return false
}

// isCongestiveLoss tells loss from a filling bottleneck queue, which raises the
// RTT, or from a policer, which drops heavily without queueing, apart from
// random loss, which BBR would otherwise mistake for congestion.
func (b *bbr) isCongestiveLoss(lost, txInFlight congestion.ByteCount) bool {
	rate := float64(lost) / float64(max(txInFlight, 1))
	return rate > policedLossThresh || (rate > lossThresh && b.isQueueBuilding())
}

func (b *bbr) isQueueBuilding() bool {
	recent := b.roundMinRTT
	if recent == infiniteRTT {
		recent = b.prevRoundMinRTT
	}
	if recent == infiniteRTT || b.minRTT == infiniteRTT {
		return false
	}
	return recent > b.minRTT+max(minQueueDelay, b.minRTT/10)
}

func (b *bbr) noteLoss() {
	if !b.isLossInRound {
		b.lossRoundDelivered = b.sampler.delivered
		b.saveStateUponLoss()
	}
	b.isLossInRound = true
}

func (b *bbr) handleLostPacket(p sentPacket) {
	lost := b.sampler.lost - p.lost
	if !b.isCongestiveLoss(lost, p.txInFlight) {
		return
	}
	b.noteLoss()
	if b.isBwProbeSample {
		b.handleInflightTooHigh(inflightAtLoss(p, lost), p.isAppLimited)
	}
}

func inflightAtLoss(p sentPacket, lost congestion.ByteCount) congestion.ByteCount {
	inflightPrev := float64(p.txInFlight - p.size)
	lostPrev := float64(lost - p.size)
	lostPrefix := (lossThresh*inflightPrev - lostPrev) / (1 - lossThresh)
	return congestion.ByteCount(max(inflightPrev+lostPrefix, 0))
}

func (b *bbr) handleInflightTooHigh(txInFlight congestion.ByteCount, isAppLimited bool) {
	b.prevProbeTooHigh = true
	b.isBwProbeSample = false
	if !isAppLimited {
		target := congestion.ByteCount(float64(min(b.bdp, b.cwnd)) * beta)
		b.inflightLongterm = max(txInFlight, target)
	}
	if b.mode == probeBWUp {
		b.undoMode, b.undoValid = probeBWUp, true
		b.startProbeBWDown(b.sampler.deliveredTime)
	}
}

func (b *bbr) updateLatestDeliverySignals(rs *rateSample) {
	b.lossRoundStart = false
	b.bwLatest = max(b.bwLatest, rs.deliveryRate)
	b.inflightLatest = max(b.inflightLatest, rs.delivered)
	if rs.priorDelivered >= b.lossRoundDelivered {
		b.lossRoundDelivered = b.sampler.delivered
		b.lossRoundStart = true
	}
}

func (b *bbr) advanceLatestDeliverySignals(rs *rateSample) {
	if b.lossRoundStart {
		b.bwLatest = rs.deliveryRate
		b.inflightLatest = rs.delivered
	}
}

func (b *bbr) resetCongestionSignals() {
	b.isLossInRound = false
	b.bwLatest = 0
	b.inflightLatest = 0
}

func (b *bbr) updateCongestionSignals(now congestion.Time, rs *rateSample) {
	b.updateMaxBw(now, rs)
	if !b.lossRoundStart {
		return
	}
	b.adaptLowerBoundsFromCongestion()
	b.isLossInRound = false
}

func (b *bbr) adaptLowerBoundsFromCongestion() {
	if b.isProbingBw() || !b.isLossInRound {
		return
	}
	if math.IsInf(b.bwShortterm, 1) {
		b.bwShortterm = b.maxBw
	}
	if b.inflightShortterm == infiniteBytes {
		b.inflightShortterm = b.cwnd
	}
	b.bwShortterm = max(b.bwLatest, beta*b.bwShortterm)
	b.inflightShortterm = max(b.inflightLatest, congestion.ByteCount(beta*float64(b.inflightShortterm)))
}

func (b *bbr) resetShortTermModel() {
	b.bwShortterm = infiniteBw
	b.inflightShortterm = infiniteBytes
}

func (b *bbr) boundBwForModel() {
	b.bw = min(b.maxBw, b.bwShortterm)
}

func (b *bbr) startRound() {
	b.nextRoundDelivered = b.sampler.delivered
}

func (b *bbr) updateRound(now congestion.Time, rs *rateSample) {
	if rs.priorDelivered < b.nextRoundDelivered {
		b.roundStart = false
		return
	}
	b.startRound()
	b.roundCount++
	b.roundsSinceProbeUp++
	b.roundStart = true
	b.isCwndLimited = b.cwndLimitedThisRound
	b.cwndLimitedThisRound = false
	b.prevRoundMinRTT, b.roundMinRTT = b.roundMinRTT, infiniteRTT
	b.sampler.forgetOlderThan(now.Add(-forgetUnackedAfter))
}

func (b *bbr) updateMaxBw(now congestion.Time, rs *rateSample) {
	b.updateRound(now, rs)
	if rs.deliveryRate > 0 && (rs.deliveryRate >= b.maxBw || !rs.isAppLimited) {
		b.bwHi[1] = max(b.bwHi[1], rs.deliveryRate)
		b.maxBw = max(b.bwHi[0], b.bwHi[1])
	}
}

func (b *bbr) advanceMaxBwFilter() {
	b.bwHi[0], b.bwHi[1] = b.bwHi[1], 0
	b.maxBw = b.bwHi[0]
}

func (b *bbr) updateACKAggregation(now congestion.Time, rs *rateSample) {
	expected := congestion.ByteCount(b.bw * now.Sub(b.extraAckedStart).Seconds())
	if b.extraAckedDelivered <= expected {
		b.extraAckedDelivered = 0
		b.extraAckedStart = now
		expected = 0
	}
	b.extraAckedDelivered += rs.newlyAcked
	extra := min(b.extraAckedDelivered-expected, b.cwnd)
	window := uint64(1)
	if b.fullBwReached {
		window = extraAckedFilterLen
	}
	b.extraAcked = congestion.ByteCount(b.extraAckedFilter.update(window, b.roundCount, float64(extra)))
}

func (b *bbr) updateMinRTT(now congestion.Time, rs *rateSample) {
	b.probeRTTExpired = now.Sub(b.probeRTTMinStamp) > probeRTTInterval
	if rs.rtt >= 0 && (rs.rtt < b.probeRTTMinDelay || b.probeRTTExpired) {
		b.probeRTTMinDelay = rs.rtt
		b.probeRTTMinStamp = now
	}
	if b.probeRTTMinDelay < b.minRTT || now.Sub(b.minRTTStamp) > minRTTFilterLen {
		b.minRTT = b.probeRTTMinDelay
		b.minRTTStamp = b.probeRTTMinStamp
	}
}

func (b *bbr) checkProbeRTT(now congestion.Time, inflight congestion.ByteCount, rs *rateSample) {
	if b.mode != probeRTT && b.probeRTTExpired && !b.idleRestart {
		b.enterProbeRTT()
		b.saveCwnd()
		b.probeRTTDoneStamp = 0
		b.ackPhase = acksProbeStopping
		b.startRound()
	}
	if b.mode == probeRTT {
		b.handleProbeRTT(now, inflight)
	}
	if rs.delivered > 0 {
		b.idleRestart = false
	}
}

func (b *bbr) enterProbeRTT() {
	b.mode = probeRTT
	b.pacingGain = 1
	b.cwndGain = probeRTTCwndGain
}

func (b *bbr) handleProbeRTT(now congestion.Time, inflight congestion.ByteCount) {
	b.sampler.markAppLimited(inflight)
	if b.probeRTTDoneStamp.IsZero() && inflight <= b.probeRTTCwnd() {
		b.probeRTTDoneStamp = now.Add(probeRTTDuration)
		b.probeRTTRoundDone = false
		b.startRound()
		return
	}
	if !b.probeRTTDoneStamp.IsZero() {
		if b.roundStart {
			b.probeRTTRoundDone = true
		}
		if b.probeRTTRoundDone {
			b.checkProbeRTTDone(now)
		}
	}
}

func (b *bbr) checkProbeRTTDone(now congestion.Time) {
	if !b.probeRTTDoneStamp.IsZero() && now.After(b.probeRTTDoneStamp) {
		b.probeRTTMinStamp = now
		b.restoreCwnd()
		b.exitProbeRTT(now)
	}
}

func (b *bbr) exitProbeRTT(now congestion.Time) {
	b.resetShortTermModel()
	if b.fullBwReached {
		b.startProbeBWDown(now)
		b.startProbeBWCruise()
		return
	}
	b.enterStartup()
}

func (b *bbr) handleRestartFromIdle(now congestion.Time, inflightBefore congestion.ByteCount) {
	if inflightBefore != 0 || b.sampler.appLimited == 0 {
		return
	}
	b.idleRestart = true
	b.extraAckedStart = now
	if b.inProbeBW() {
		b.setPacingRateWithGain(1)
	} else if b.mode == probeRTT {
		b.checkProbeRTTDone(now)
	}
}

func (b *bbr) saveStateUponLoss() {
	b.saveCwnd()
	b.undoValid = false
	b.undoBwShortterm = b.bwShortterm
	b.undoInflightShortterm = b.inflightShortterm
	b.undoInflightLongterm = b.inflightLongterm
}

func (b *bbr) saveCwnd() {
	if !b.inRecovery && b.mode != probeRTT {
		b.priorCwnd = b.cwnd
		return
	}
	b.priorCwnd = max(b.priorCwnd, b.cwnd)
}

func (b *bbr) restoreCwnd() {
	b.cwnd = max(b.cwnd, b.priorCwnd)
}

func (b *bbr) initPacingRate() {
	srtt := time.Millisecond
	if b.rtt.HasMeasurement() {
		srtt = b.rtt.SmoothedRTT()
	}
	b.pacingRate = startupPacingGain * float64(b.initialCwnd) / srtt.Seconds()
	b.pacer.rate = b.pacingRate
	b.pacer.packet = b.smss
}

func (b *bbr) setPacingRateWithGain(gain float64) {
	rate := gain * b.bw * pacingMargin
	if b.fullBwReached || rate > b.pacingRate {
		b.pacingRate = rate
		b.pacer.rate = rate
	}
}

func (b *bbr) setPacingRate() { b.setPacingRateWithGain(b.pacingGain) }

func (b *bbr) setSendQuantum() {
	q := congestion.ByteCount(b.pacingRate * float64(time.Millisecond) / float64(time.Second))
	b.sendQuantum = max(min(q, maxSendQuantum), 2*b.smss)
	b.pacer.burst = b.sendQuantum
}

func (b *bbr) bdpMultiple(bw, gain float64) congestion.ByteCount {
	if b.minRTT == infiniteRTT {
		return b.initialCwnd
	}
	return congestion.ByteCount(gain * bw * b.minRTT.Seconds())
}

func (b *bbr) quantizationBudget(inflight congestion.ByteCount) congestion.ByteCount {
	inflight = max(inflight, b.sendQuantum, b.minPipeCwnd())
	if b.mode == probeBWUp {
		inflight += 2 * b.smss
	}
	return inflight
}

func (b *bbr) inflight(bw, gain float64) congestion.ByteCount {
	return b.quantizationBudget(b.bdpMultiple(bw, gain))
}

func (b *bbr) updateMaxInflight() {
	b.bdp = b.bdpMultiple(b.bw, 1)
	b.maxInflight = b.quantizationBudget(b.bdpMultiple(b.bw, b.cwndGain) + b.extraAcked)
}

func (b *bbr) setCwnd(rs *rateSample) {
	b.updateMaxInflight()
	switch {
	case b.fullBwReached:
		b.cwnd = min(b.cwnd+rs.newlyAcked, b.maxInflight)
	case b.cwnd < b.maxInflight || b.sampler.delivered < b.initialCwnd:
		b.cwnd += rs.newlyAcked
	}
	b.cwnd = max(b.cwnd, b.minPipeCwnd())
	if b.mode == probeRTT {
		b.cwnd = min(b.cwnd, b.probeRTTCwnd())
	}
	b.boundCwndForModel()
}

func (b *bbr) probeRTTCwnd() congestion.ByteCount {
	return max(b.bdpMultiple(b.bw, probeRTTCwndGain), b.minPipeCwnd())
}

func (b *bbr) boundCwndForModel() {
	limit := infiniteBytes
	switch {
	case b.inProbeBW() && b.mode != probeBWCruise:
		limit = b.inflightLongterm
	case b.mode == probeRTT || b.mode == probeBWCruise:
		limit = b.inflightWithHeadroom()
	}
	limit = max(min(limit, b.inflightShortterm), b.minPipeCwnd())
	b.cwnd = min(b.cwnd, limit)
}
