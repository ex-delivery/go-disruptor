package test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestStats checks the pull-based snapshot tracks publishing, occupancy, and
// consumer lag, and recovers after draining.
func TestStats(t *testing.T) {
	const capacity = 8
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-proceed // park so occupancy and lag are observable deterministically
	})
	d.RegisterConsumer(c)
	d.Start()

	if s := d.Stats(); s.Published != 0 || s.Free != capacity || s.Capacity != capacity {
		t.Fatalf("fresh ring stats = %+v", s)
	}

	for range capacity {
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}

	s := d.Stats()
	if s.Published != capacity {
		t.Fatalf("Published=%d want %d", s.Published, capacity)
	}
	if s.Free != 0 {
		t.Fatalf("Free=%d want 0 (ring full)", s.Free)
	}
	if len(s.ConsumerLag) != 1 || s.ConsumerLag[0] != capacity {
		t.Fatalf("ConsumerLag=%v want [%d]", s.ConsumerLag, capacity)
	}

	close(proceed)
	d.Stop()

	s = d.Stats()
	if s.ConsumerLag[0] != 0 {
		t.Fatalf("drained ConsumerLag=%v want [0]", s.ConsumerLag)
	}
	if s.Free != capacity {
		t.Fatalf("drained Free=%d want %d", s.Free, capacity)
	}
}

// TestStatsBackpressure verifies the back-pressure counter rises only when a
// producer actually has to wait for room.
func TestStatsBackpressure(t *testing.T) {
	const capacity = 4
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) { <-proceed })
	d.RegisterConsumer(c)
	d.Start()

	for range capacity { // exactly fills the ring — no claim has to wait yet
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}
	if bp := d.Stats().Backpressure; bp != 0 {
		t.Fatalf("Backpressure=%d before any blocking claim, want 0", bp)
	}

	// The ring is full: this claim must wait, which is one back-pressure event.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := d.NextContext(ctx, 1); err == nil {
		t.Fatal("NextContext should have timed out on a full ring")
	}
	if bp := d.Stats().Backpressure; bp < 1 {
		t.Fatalf("Backpressure=%d after a blocked claim, want >= 1", bp)
	}

	close(proceed)
	d.Stop()
}

// TestWithMetrics checks the WithMetrics sampler fires periodically and is shut
// down cleanly (a final snapshot is taken on Stop).
func TestWithMetrics(t *testing.T) {
	var samples int64
	d := disruptor.NewDisruptor(64, newEvent,
		disruptor.WithMetrics(5*time.Millisecond, func(s disruptor.Stats) {
			atomic.AddInt64(&samples, 1)
		}))

	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {})
	d.RegisterConsumer(c)
	d.Start()

	for i := int64(1); i <= 100; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	time.Sleep(30 * time.Millisecond) // let several samples fire
	d.Stop()                          // waits for the sampler's final snapshot

	if got := atomic.LoadInt64(&samples); got == 0 {
		t.Fatal("WithMetrics sink was never called")
	}
}
