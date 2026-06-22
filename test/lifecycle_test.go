package test

import (
	"sync/atomic"
	"testing"

	"github.com/ex-delivery/go-disruptor"
)

// TestConsumerLifecycle verifies OnStart fires once before processing and
// OnShutdown fires once after draining (and completes before Stop returns).
func TestConsumerLifecycle(t *testing.T) {
	var started, shutdown, ordered atomic.Int64
	d := disruptor.NewDisruptor(64, newEvent)

	var processed int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		// onStart must have run before any event is processed.
		if started.Load() == 0 {
			ordered.Store(1) // flag an ordering violation
		}
		for s := lo; s <= hi; s++ {
			processed++
		}
	}).
		OnStart(func() { started.Add(1) }).
		OnShutdown(func() { shutdown.Add(1) })
	d.RegisterConsumer(c)
	d.Start()

	for i := int64(1); i <= 10; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if got := started.Load(); got != 1 {
		t.Fatalf("OnStart called %d times, want 1", got)
	}
	if got := shutdown.Load(); got != 1 {
		t.Fatalf("OnShutdown called %d times, want 1 (must complete before Stop returns)", got)
	}
	if ordered.Load() != 0 {
		t.Fatal("an event was processed before OnStart ran")
	}
	if processed != 10 {
		t.Fatalf("processed=%d want 10", processed)
	}
}

// TestWorkerPoolLifecycle verifies the pool's OnStart/OnShutdown fire once per
// worker goroutine.
func TestWorkerPoolLifecycle(t *testing.T) {
	const workers = 4
	var started, shutdown atomic.Int64
	d := disruptor.NewDisruptor(64, newEvent)

	pool := d.WorkerPool(workers, func(buf []Event, mask, seq int64) {}).
		OnStart(func() { started.Add(1) }).
		OnShutdown(func() { shutdown.Add(1) })
	d.RegisterWorkerPool(pool)
	d.Start()

	for i := int64(1); i <= 50; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if got := started.Load(); got != workers {
		t.Fatalf("OnStart called %d times, want %d (once per worker)", got, workers)
	}
	if got := shutdown.Load(); got != workers {
		t.Fatalf("OnShutdown called %d times, want %d (once per worker)", got, workers)
	}
}
