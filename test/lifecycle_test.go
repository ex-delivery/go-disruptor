package test

import (
	"sync/atomic"
	"testing"

	"github.com/ex-delivery/go-disruptor"
)

// lifecycleHandler implements EventHandler plus the optional LifecycleAware
// interface, mirroring how Disruptor v4 folds onStart/onShutdown into the handler.
type lifecycleHandler struct {
	started   atomic.Int64
	shutdown  atomic.Int64
	processed atomic.Int64
	ordering  atomic.Int64 // counts events seen before OnStart (must stay 0)
}

func (h *lifecycleHandler) OnEvent(e *Event, seq int64, eob bool) error {
	if h.started.Load() == 0 {
		h.ordering.Add(1)
	}
	h.processed.Add(1)
	return nil
}

func (h *lifecycleHandler) OnStart()    { h.started.Add(1) }
func (h *lifecycleHandler) OnShutdown() { h.shutdown.Add(1) }

// TestConsumerLifecycle verifies OnStart fires once before processing and
// OnShutdown fires once after draining (completing before Stop returns).
func TestConsumerLifecycle(t *testing.T) {
	const N = 10
	d := disruptor.NewDisruptor(64, newEvent)

	h := &lifecycleHandler{}
	c := d.Consumer(h)
	d.RegisterConsumer(c)
	d.Start()

	for i := int64(1); i <= N; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if got := h.started.Load(); got != 1 {
		t.Fatalf("OnStart called %d times, want 1", got)
	}
	if got := h.shutdown.Load(); got != 1 {
		t.Fatalf("OnShutdown called %d times, want 1 (must complete before Stop returns)", got)
	}
	if got := h.ordering.Load(); got != 0 {
		t.Fatalf("%d events processed before OnStart ran", got)
	}
	if got := h.processed.Load(); got != N {
		t.Fatalf("processed=%d want %d", got, N)
	}
}
