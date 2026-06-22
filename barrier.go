package disruptor

import (
	"context"
	"math"
	"runtime"
	"sync/atomic"
)

// Barrier aggregates a set of upstream Sequences and exposes their minimum as a
// single gating point. Two uses:
//
//   - A consumer waits on a Barrier built from the ring's writer cursor (and any
//     dependency consumers' read cursors) to learn what's safe to process.
//   - A producer waits on a Barrier built from the sink consumers' read cursors
//     to apply back-pressure (don't overwrite a slot still being read).
//
// Alert breaks waiters out of their spin loop for graceful shutdown. Register
// and setStrategy are wiring-time only and are not safe to call concurrently
// with WaitFor.
type Barrier struct {
	seqs        []*Sequence
	strategy    WaitStrategy
	alerted     atomic.Bool
	stalls      atomic.Int64 // times a waiter had to wait (slow path); for metrics
	countStalls bool         // only the producer gate counts (back-pressure events)
}

// Register adds a sequence to the set this barrier gates on.
func (b *Barrier) Register(seq *Sequence) {
	b.seqs = append(b.seqs, seq)
}

// setStrategy installs the wait strategy; nil falls back to YieldingWait.
func (b *Barrier) setStrategy(s WaitStrategy) {
	if s == nil {
		s = YieldingWait{}
	}
	b.strategy = s
}

// Alert wakes any goroutine spinning in WaitFor so it can observe shutdown.
func (b *Barrier) Alert() { b.alerted.Store(true) }

// ClearAlert resets the alert flag (e.g. to reuse a barrier).
func (b *Barrier) ClearAlert() { b.alerted.Store(false) }

// minimum returns the smallest registered sequence, or MaxInt64 if none.
func (b *Barrier) minimum() int64 {
	minSeq := int64(math.MaxInt64)
	for _, s := range b.seqs {
		if v := s.Load(); v < minSeq {
			minSeq = v
		}
	}
	return minSeq
}

// WaitFor blocks until the aggregated minimum sequence is >= expected, or the
// barrier is alerted. It always prefers returning available data: if the
// minimum already satisfies expected it returns (min, false) even when alerted,
// so a consumer can fully drain published events before honouring the alert.
//
// Returns (min, false) when data is ready (min >= expected); (min, true) when
// alerted with no new data (min < expected).
func (b *Barrier) WaitFor(expected int64) (available int64, alerted bool) {
	spin := 0
	counted := false
	for {
		m := b.minimum()
		if m >= expected {
			return m, false
		}
		if b.alerted.Load() {
			return m, true
		}
		if b.countStalls && !counted {
			b.stalls.Add(1) // count once per waiting call, not per spin
			counted = true
		}
		if b.strategy != nil {
			b.strategy.Idle(spin)
		} else {
			runtime.Gosched()
		}
		spin++
	}
}

// waitForContext behaves like WaitFor but also honours ctx: if ctx is cancelled
// (or its deadline passes) while spinning, it returns (min, false, ctx.Err()).
// Cancellation is checked once per failed poll. ctx must be non-nil; callers
// with no context should use WaitFor instead.
//
// Returns (min, false, nil) when data is ready, (min, true, nil) when alerted,
// and (min, false, err) when the context ended first.
func (b *Barrier) waitForContext(ctx context.Context, expected int64) (available int64, alerted bool, err error) {
	spin := 0
	counted := false
	for {
		m := b.minimum()
		if m >= expected {
			return m, false, nil
		}
		if b.alerted.Load() {
			return m, true, nil
		}
		if e := ctx.Err(); e != nil {
			return m, false, e
		}
		if b.countStalls && !counted {
			b.stalls.Add(1)
			counted = true
		}
		if b.strategy != nil {
			b.strategy.Idle(spin)
		} else {
			runtime.Gosched()
		}
		spin++
	}
}

// stallCount reports how many times a waiter on this barrier had to wait. On the
// producer gate this is the cumulative back-pressure event count.
func (b *Barrier) stallCount() int64 { return b.stalls.Load() }
