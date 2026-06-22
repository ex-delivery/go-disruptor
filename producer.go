package disruptor

import (
	"context"
	"math/bits"
	"sync/atomic"
)

// Producer claims and commits slots in the ring.
//
//   - Next claims count contiguous slots and returns the upper sequence of the
//     claimed range; the lower bound is upper-count+1. It blocks (per the wait
//     strategy) until the slowest sink consumer has freed enough room, applying
//     back-pressure.
//   - TryNext is the non-blocking variant: it claims count slots only if room is
//     already available, returning (upper, true) on success or (0, false) when
//     the ring is full. It never blocks, so it is the safe way to publish from a
//     goroutine that must also be able to shut down (Stop cannot unblock a
//     producer parked in Next).
//   - RemainingCapacity reports how many slots are currently free (an estimate
//     under concurrency). Zero means the next Next would block / TryNext fail.
//   - Publish makes [lower, upper] visible to consumers.
//
// Typical use:
//
//	seq := p.Next(1)
//	*ring.Get(seq) = value
//	p.Publish(seq, seq)
//
// Non-blocking use:
//
//	if seq, ok := p.TryNext(1); ok {
//	    *ring.Get(seq) = value
//	    p.Publish(seq, seq)
//	}
type Producer interface {
	Next(count int64) int64
	NextContext(ctx context.Context, count int64) (upper int64, err error)
	TryNext(count int64) (upper int64, ok bool)
	RemainingCapacity() int64
	Publish(lower, upper int64)
}

// ---------------------------------------------------------------------------
// Single producer
// ---------------------------------------------------------------------------

// singleProducer is the fast path for exactly one writer goroutine. Because no
// other goroutine touches current/cachedPoint, they are plain ints — no atomics
// needed. This mirrors the LMAX SingleProducerSequencer.
type singleProducer struct {
	writeIndex   *Sequence
	blockBarrier *Barrier
	capacity     int64
	cachedPoint  int64 // cached gating minimum; -1 initially
	current      int64 // last claimed sequence; -1 initially
}

// NewSingleProducer builds a single-writer producer. Safe for use by ONE
// goroutine only.
func NewSingleProducer(writeIndex *Sequence, barrier *Barrier, capacity int64) Producer {
	return &singleProducer{
		writeIndex:   writeIndex,
		blockBarrier: barrier,
		capacity:     capacity,
		cachedPoint:  -1,
		current:      -1,
	}
}

func (s *singleProducer) Next(count int64) int64 {
	if count < 1 {
		count = 1
	}
	previous := s.current
	next := previous + count
	wrapPoint := next - s.capacity

	// Fast path: the cached gate already proves there's room AND is not stale
	// (not ahead of our own position). Otherwise spin until consumers catch up.
	if wrapPoint > s.cachedPoint || s.cachedPoint > previous {
		gate, _ := s.blockBarrier.WaitFor(wrapPoint)
		s.cachedPoint = gate
	}
	s.current = next
	return next
}

// NextContext is the context-aware blocking claim: like Next it waits for room,
// but returns ctx.Err() if ctx is cancelled (or its deadline passes) first, and
// ErrClosed if the disruptor is shutting down. On error nothing is claimed and
// the writer position is left unchanged, so the call can be safely retried.
func (s *singleProducer) NextContext(ctx context.Context, count int64) (int64, error) {
	if count < 1 {
		count = 1
	}
	previous := s.current
	next := previous + count
	wrapPoint := next - s.capacity
	if wrapPoint > s.cachedPoint || s.cachedPoint > previous {
		gate, alerted, err := s.blockBarrier.waitForContext(ctx, wrapPoint)
		if err != nil {
			return 0, err
		}
		if alerted {
			return 0, ErrClosed
		}
		s.cachedPoint = gate
	}
	s.current = next
	return next, nil
}

// TryNext claims count slots without blocking. The gating minimum is monotonic
// (consumers only advance), so a cached value is always a lower bound on the
// true gate: wrapPoint <= cachedPoint already proves there is room. Only when
// the cache is too low do we refresh it with a single read and re-check.
func (s *singleProducer) TryNext(count int64) (int64, bool) {
	if count < 1 {
		count = 1
	}
	next := s.current + count
	wrapPoint := next - s.capacity
	if wrapPoint > s.cachedPoint {
		gate := s.blockBarrier.minimum()
		s.cachedPoint = gate
		if wrapPoint > gate {
			return 0, false // not enough room right now
		}
	}
	s.current = next
	return next, true
}

// RemainingCapacity reports the free slot count: capacity minus the in-flight
// span (claimed but not yet consumed), clamped to [0, capacity].
func (s *singleProducer) RemainingCapacity() int64 {
	consumed := s.blockBarrier.minimum()
	return clampCapacity(s.capacity-(s.current-consumed), s.capacity)
}

func (s *singleProducer) Publish(lower, upper int64) {
	// Single writer: a plain atomic store of the upper bound is enough; there is
	// no gap to fill because sequences are claimed strictly in order.
	_ = lower
	s.writeIndex.Store(upper)
}

// ---------------------------------------------------------------------------
// Multi producer
// ---------------------------------------------------------------------------

// multiProducer supports many concurrent writer goroutines. Claiming is a CAS
// on claimIdx; publishing marks each slot's "availability" with the lap number
// (seq >> shift) so a stale value from a previous lap can never be mistaken for
// a fresh publish. writeIdx is then advanced cooperatively to the highest
// contiguous published sequence. Mirrors the LMAX MultiProducerSequencer.
type multiProducer struct {
	writeIdx     *Sequence
	claimIdx     Sequence
	capacity     int64
	blockBarrier *Barrier
	available    []int64 // per-slot lap number; -1 = never published
	mask         int64
	cachedPoint  atomic.Int64
	shift        int
}

// NewMultiProducer builds a producer safe for concurrent use by many
// goroutines.
func NewMultiProducer(writeIndex *Sequence, barrier *Barrier, capacity int64) Producer {
	m := &multiProducer{
		writeIdx:     writeIndex,
		capacity:     capacity,
		blockBarrier: barrier,
		available:    make([]int64, capacity),
		mask:         capacity - 1,
		shift:        bits.Len64(uint64(capacity)) - 1,
	}
	for i := range m.available {
		m.available[i] = -1
	}
	m.claimIdx.Store(-1)
	m.cachedPoint.Store(-1)
	return m
}

func (m *multiProducer) Next(count int64) int64 {
	if count < 1 {
		count = 1
	}
	for {
		current := m.claimIdx.Load()
		next := current + count
		wrapPoint := next - m.capacity

		if cached := m.cachedPoint.Load(); wrapPoint > cached || cached > current {
			gate, _ := m.blockBarrier.WaitFor(wrapPoint)
			m.cachedPoint.Store(gate)
		}
		if m.claimIdx.CompareAndSwap(current, next) {
			return next
		}
		// Lost the race; retry with a fresh claim.
	}
}

// NextContext is the context-aware blocking claim for multiple producers. It
// waits for room like Next, but returns ctx.Err() if ctx ends first, or
// ErrClosed on shutdown. On error nothing is claimed and the claim cursor is
// left untouched.
func (m *multiProducer) NextContext(ctx context.Context, count int64) (int64, error) {
	if count < 1 {
		count = 1
	}
	for {
		current := m.claimIdx.Load()
		next := current + count
		wrapPoint := next - m.capacity
		if cached := m.cachedPoint.Load(); wrapPoint > cached || cached > current {
			gate, alerted, err := m.blockBarrier.waitForContext(ctx, wrapPoint)
			if err != nil {
				return 0, err
			}
			if alerted {
				return 0, ErrClosed
			}
			m.cachedPoint.Store(gate)
		}
		if m.claimIdx.CompareAndSwap(current, next) {
			return next, nil
		}
	}
}

// TryNext claims count slots without blocking. Like the single-writer variant it
// trusts the cached gate as a lower bound and only refreshes when that cache is
// too low; on a full ring it returns (0, false) instead of spinning. The CAS may
// still retry under contention from other producers — that is claim contention,
// not back-pressure, so it stays non-blocking.
func (m *multiProducer) TryNext(count int64) (int64, bool) {
	if count < 1 {
		count = 1
	}
	for {
		current := m.claimIdx.Load()
		next := current + count
		wrapPoint := next - m.capacity
		if wrapPoint > m.cachedPoint.Load() {
			gate := m.blockBarrier.minimum()
			m.cachedPoint.Store(gate)
			if wrapPoint > gate {
				return 0, false // not enough room right now
			}
		}
		if m.claimIdx.CompareAndSwap(current, next) {
			return next, true
		}
		// Lost the race; retry with a fresh claim.
	}
}

// RemainingCapacity reports the free slot count, clamped to [0, capacity]. Under
// concurrent producers the claim cursor and the gate are sampled separately, so
// the result is a momentary estimate rather than an exact count.
func (m *multiProducer) RemainingCapacity() int64 {
	consumed := m.blockBarrier.minimum()
	produced := m.claimIdx.Load()
	return clampCapacity(m.capacity-(produced-consumed), m.capacity)
}

func (m *multiProducer) Publish(lower, upper int64) {
	// 1) Mark each claimed slot available, tagged with its lap number. The store
	//    is atomic so consumers (reading writeIdx, which is advanced below) get a
	//    correct happens-before edge to the slot payload written before Publish.
	for seq := lower; seq <= upper; seq++ {
		atomic.StoreInt64(&m.available[seq&m.mask], seq>>m.shift)
	}
	// 2) Advance the published cursor as far as contiguous availability allows.
	//    Multiple producers help each other here, so the cursor never stalls on
	//    an out-of-order publish.
	for {
		currentWrite := m.writeIdx.Load()
		nextToWrite := currentWrite + 1
		if atomic.LoadInt64(&m.available[nextToWrite&m.mask]) != nextToWrite>>m.shift {
			return
		}
		m.writeIdx.CompareAndSwap(currentWrite, nextToWrite)
	}
}

// clampCapacity bounds a computed free-slot count to [0, capacity]. With no
// gating consumers the gate is MaxInt64 and the raw figure overflows high, so
// the upper clamp reports the ring as fully free rather than a nonsense value.
func clampCapacity(remaining, capacity int64) int64 {
	return min(max(remaining, 0), capacity)
}
