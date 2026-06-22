package test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestEndOfBatch verifies the endOfBatch flag fires once per batch and every
// event is delivered.
func TestEndOfBatch(t *testing.T) {
	const N = 500
	d := disruptor.NewDisruptor(64, newEvent)

	var events, ends int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		events++
		if eob {
			ends++
		}
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if events != N {
		t.Fatalf("events=%d want %d", events, N)
	}
	if ends < 1 || ends > N {
		t.Fatalf("endOfBatch count=%d, want in [1,%d] (once per batch)", ends, N)
	}
}

// TestBatchRewind verifies ErrRewind reprocesses the whole batch via the
// BatchRewindStrategy, and every event is eventually handled.
func TestBatchRewind(t *testing.T) {
	const N = 50
	d := disruptor.NewDisruptor(64, newEvent)

	seen := make([]int32, N)
	var calls, rewinds int64
	var didRewind atomic.Bool
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		calls++
		if eob && didRewind.CompareAndSwap(false, true) {
			rewinds++
			return disruptor.ErrRewind // rewind the first complete batch once
		}
		seen[e.Value] = 1
		return nil
	})).WithRewindStrategy(disruptor.AlwaysRewind{})
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if rewinds != 1 {
		t.Fatalf("rewinds=%d want 1", rewinds)
	}
	if calls <= N {
		t.Fatalf("calls=%d, expected > %d (a rewind must reprocess a batch)", calls, N)
	}
	for i, s := range seen {
		if s != 1 {
			t.Fatalf("event %d was never processed after the rewind", i)
		}
	}
}

// batchAwareHandler implements EventHandler + BatchStartAware, recording the
// largest batch size it is told about.
type batchAwareHandler struct {
	events      int64
	batches     int64
	maxObserved int64
	gate        chan struct{}
	gated       atomic.Bool
}

func (h *batchAwareHandler) OnEvent(e *Event, seq int64, eob bool) error {
	if h.gate != nil && h.gated.CompareAndSwap(false, true) {
		<-h.gate // block on the very first event so a backlog can build
	}
	h.events++
	return nil
}

func (h *batchAwareHandler) OnBatchStart(batchSize, queueDepth int64) {
	h.batches++
	if batchSize > h.maxObserved {
		h.maxObserved = batchSize
	}
}

// TestOnBatchStart verifies OnBatchStart fires with positive sizes and the
// per-event count is exact.
func TestOnBatchStart(t *testing.T) {
	const N = 1000
	d := disruptor.NewDisruptor(64, newEvent)

	h := &batchAwareHandler{}
	c := d.Consumer(h)
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if h.events != N {
		t.Fatalf("events=%d want %d", h.events, N)
	}
	if h.batches == 0 {
		t.Fatal("OnBatchStart was never called")
	}
	if h.maxObserved < 1 {
		t.Fatal("OnBatchStart never reported a positive batch size")
	}
}

// TestMaxBatchSize parks the consumer to build a backlog, then verifies no batch
// exceeds the configured cap.
func TestMaxBatchSize(t *testing.T) {
	const N = 1000
	const cap = 16
	d := disruptor.NewDisruptor(2048, newEvent)

	h := &batchAwareHandler{gate: make(chan struct{})}
	c := d.Consumer(h).MaxBatchSize(cap)
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) { // fills a backlog while the consumer is gated on event 0
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	close(h.gate) // release; the backlog drains in capped batches
	d.Stop()

	if h.events != N {
		t.Fatalf("events=%d want %d", h.events, N)
	}
	if h.maxObserved > cap {
		t.Fatalf("observed batch size %d exceeds MaxBatchSize %d", h.maxObserved, cap)
	}
	if h.maxObserved != cap {
		t.Fatalf("max observed batch %d, want %d (cap should bind given the backlog)", h.maxObserved, cap)
	}
}

// timeoutAwareHandler implements EventHandler + TimeoutAware.
type timeoutAwareHandler struct {
	events   int64
	timeouts atomic.Int64
}

func (h *timeoutAwareHandler) OnEvent(e *Event, seq int64, eob bool) error { h.events++; return nil }
func (h *timeoutAwareHandler) OnTimeout(sequence int64)                    { h.timeouts.Add(1) }

// TestOnTimeout verifies OnTimeout fires during idle periods and normal events
// still flow.
func TestOnTimeout(t *testing.T) {
	d := disruptor.NewDisruptor(64, newEvent)

	h := &timeoutAwareHandler{}
	c := d.Consumer(h).Timeout(10 * time.Millisecond)
	d.RegisterConsumer(c)
	d.Start()

	time.Sleep(60 * time.Millisecond) // idle: OnTimeout should fire repeatedly

	seq := d.Next(1)
	d.Get(seq).Value = 1
	d.Publish(seq, seq)
	d.Stop()

	if h.timeouts.Load() == 0 {
		t.Fatal("OnTimeout never fired during the idle period")
	}
	if h.events != 1 {
		t.Fatalf("events=%d want 1", h.events)
	}
}
