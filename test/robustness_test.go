package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestNextContextCancel: a producer blocked in NextContext on a full ring is
// released with ctx.Err() when its context is cancelled.
func TestNextContextCancel(t *testing.T) {
	const capacity = 4
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		<-proceed
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	for range capacity {
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := d.NextContext(ctx, 1) // blocks: ring full
		errCh <- err
	}()

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
		t.Fatal("NextContext did not return after cancel (blocked claim not interruptible)")
	}

	close(proceed)
	d.Stop()
}

// TestStopContextTimeout: a wedged handler makes StopContext return a deadline
// error promptly instead of blocking forever.
func TestStopContextTimeout(t *testing.T) {
	d := disruptor.NewDisruptor(8, newEvent)

	block := make(chan struct{})
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		<-block // wedged
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	seq := d.Next(1)
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
	close(block)
}

// TestStopContextClean: with a context that never ends, StopContext drains every
// event and returns nil.
func TestStopContextClean(t *testing.T) {
	const N = 1000
	d := disruptor.NewDisruptor(64, newEvent)

	var sum int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		sum += e.Value
		return nil
	}))
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

// TestDefaultExceptionHandler verifies the disruptor-wide default ExceptionHandler
// is inherited by consumers, and a per-consumer handler overrides it.
func TestDefaultExceptionHandler(t *testing.T) {
	d := disruptor.NewDisruptor(64, newEvent)
	def := &countingExceptions{}
	d.HandleExceptionsWith(def)

	a := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		panic("boom-a") // inherits the default handler
	}))

	own := &countingExceptions{}
	b := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		panic("boom-b")
	})).HandleExceptionsWith(own) // overrides the default

	d.RegisterConsumer(a, b)
	d.Start()

	seq := d.Next(1)
	d.Get(seq).Value = 1
	d.Publish(seq, seq)
	d.Stop()

	if def.events.Load() == 0 {
		t.Fatal("default ExceptionHandler was not applied to consumer A")
	}
	if own.events.Load() == 0 {
		t.Fatal("per-consumer ExceptionHandler override was not used by consumer B")
	}
}
