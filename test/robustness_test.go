package test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestNextContextCancel fills the ring with a parked consumer, then checks that
// a producer blocked in NextContext is released with ctx.Err() when its context
// is cancelled (the interruptible-claim guarantee that plain Next lacks).
func TestNextContextCancel(t *testing.T) {
	const capacity = 4
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-proceed // park so the ring fills and never drains
	})
	d.RegisterConsumer(c)
	d.Start()

	// Fill every slot; the parked consumer frees nothing, so the next claim blocks.
	for range capacity {
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := d.NextContext(ctx, 1) // blocks: ring is full
		errCh <- err
	}()

	// It must still be blocked before we cancel.
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("NextContext returned (%v) while the ring was full; should have blocked", err)
	default:
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("NextContext err=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("NextContext did not return after cancel (blocked claim was not interruptible)")
	}

	close(proceed)
	d.Stop()
}

// TestStopContextTimeout wedges a consumer's handler and verifies StopContext
// returns a deadline error promptly instead of blocking forever — the fix for
// Stop being hostage to a stuck consumer.
func TestStopContextTimeout(t *testing.T) {
	d := disruptor.NewDisruptor(8, newEvent)

	block := make(chan struct{})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-block // wedged: never returns until the test releases it
	})
	d.RegisterConsumer(c)
	d.Start()

	seq := d.Next(1) // one event so the consumer enters (and wedges in) the handler
	d.Get(seq).Value = 1
	d.Publish(seq, seq)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := d.StopContext(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("StopContext returned nil with a wedged consumer, want a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopContext err=%v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("StopContext blocked %v; should have returned near the 50ms deadline", elapsed)
	}

	close(block) // release the wedged goroutine so the test doesn't leak it
}

// TestStopContextClean verifies the happy path: with a context that never ends,
// StopContext drains every published event and returns nil.
func TestStopContextClean(t *testing.T) {
	const N = 1000
	d := disruptor.NewDisruptor(64, newEvent)

	var sum int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			sum += buf[s&mask].Value
		}
	})
	d.RegisterConsumer(c)
	d.Start()

	var want int64
	for i := int64(1); i <= N; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
		want += i
	}

	if err := d.StopContext(context.Background()); err != nil {
		t.Fatalf("clean StopContext returned %v", err)
	}
	if sum != want {
		t.Fatalf("sum=%d want=%d (clean StopContext must drain everything)", sum, want)
	}
}

// TestDefaultPanicHandler checks that WithPanicHandler installs a default that
// every consumer inherits, so a panicking handler is recovered and the pipeline
// keeps running. StopContext with a deadline turns a regression (stalled
// pipeline) into a failure instead of a hang.
func TestDefaultPanicHandler(t *testing.T) {
	const N = 100
	var panics int64
	d := disruptor.NewDisruptor(64, newEvent,
		disruptor.WithPanicHandler(func(recovered any, lo, hi int64) {
			atomic.AddInt64(&panics, 1)
		}))

	var processed int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			if buf[s&mask].Value == 50 {
				panic("boom")
			}
			atomic.AddInt64(&processed, 1)
		}
	})
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.StopContext(ctx); err != nil {
		t.Fatalf("pipeline stalled, default panic handler not applied? %v", err)
	}
	if panics == 0 {
		t.Fatal("default panic handler was never invoked")
	}
	if processed == 0 || processed >= N {
		t.Fatalf("processed=%d, expected to skip the poisoned batch but keep going", processed)
	}
}

// TestPanicHandlerOverride verifies a consumer's own OnPanic takes precedence
// over the disruptor-wide default.
func TestPanicHandlerOverride(t *testing.T) {
	var defaultHits, ownHits int64
	d := disruptor.NewDisruptor(32, newEvent,
		disruptor.WithPanicHandler(func(any, int64, int64) {
			atomic.AddInt64(&defaultHits, 1)
		}))

	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		panic("boom")
	}).OnPanic(func(any, int64, int64) {
		atomic.AddInt64(&ownHits, 1)
	})
	d.RegisterConsumer(c)
	d.Start()

	seq := d.Next(1)
	d.Get(seq).Value = 1
	d.Publish(seq, seq)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.StopContext(ctx); err != nil {
		t.Fatalf("pipeline stalled: %v", err)
	}
	if ownHits == 0 {
		t.Fatal("consumer's own OnPanic was not used")
	}
	if defaultHits != 0 {
		t.Fatalf("default handler fired %d times; OnPanic should have overridden it", defaultHits)
	}
}
