package test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestSPSC verifies a single producer / single consumer carries every event.
func TestSPSC(t *testing.T) {
	const N = 1 << 16
	d := disruptor.NewDisruptor(1024, newEvent)

	var sum int64 // single consumer goroutine; read after Stop (happens-before)
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
	d.Stop()

	if sum != want {
		t.Fatalf("sum=%d want=%d", sum, want)
	}
}

// TestMPSC verifies many producers / single consumer: every sequence is
// delivered exactly once AND each slot carries the correct payload.
func TestMPSC(t *testing.T) {
	const producers = 4
	const perProd = 1 << 14
	const total = producers * perProd

	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))

	var sum int64 // single consumer; read after Stop
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		sum += e.Value
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perProd {
				seq := d.Next(1)
				d.Get(seq).Value = seq + 1 // payload derived from the sequence
				d.Publish(seq, seq)
			}
		})
	}
	wg.Wait()
	d.Stop()

	want := int64(total) * (int64(total) + 1) / 2 // sum of 1..total
	if sum != want {
		t.Fatalf("checksum=%d want=%d (lost, duplicated, or corrupted events)", sum, want)
	}
}

// TestRingWrap hammers a tiny ring so sequences lap many times.
func TestRingWrap(t *testing.T) {
	const N = 1000
	d := disruptor.NewDisruptor(8, newEvent) // capacity 8 -> ~125 laps

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
	d.Stop()

	if sum != want {
		t.Fatalf("sum=%d want=%d", sum, want)
	}
}

// TestDiamondDependencies verifies a fan-out/fan-in DAG: b and c run in parallel,
// merge depends on both and must never see a sequence before both have it.
func TestDiamondDependencies(t *testing.T) {
	const N = 1 << 14
	d := disruptor.NewDisruptor(1024, newEvent)

	bSeen := make([]int32, N)
	cSeen := make([]int32, N)
	var mergeCount, violations int64

	b := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		atomic.StoreInt32(&bSeen[e.Value], 1)
		return nil
	}))
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		atomic.StoreInt32(&cSeen[e.Value], 1)
		return nil
	}))
	merge := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		if atomic.LoadInt32(&bSeen[e.Value]) != 1 || atomic.LoadInt32(&cSeen[e.Value]) != 1 {
			atomic.AddInt64(&violations, 1)
		}
		atomic.AddInt64(&mergeCount, 1)
		return nil
	})).Depends(b, c)

	d.RegisterConsumer(b, c, merge)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i // value doubles as an index into bSeen/cSeen
		d.Publish(seq, seq)
	}
	d.Stop()

	if violations != 0 {
		t.Fatalf("merge saw %d events before b/c finished them", violations)
	}
	if mergeCount != N {
		t.Fatalf("mergeCount=%d want=%d", mergeCount, N)
	}
}

// TestGracefulDrain publishes a burst then stops; every event must be processed.
func TestGracefulDrain(t *testing.T) {
	const N = 5000
	d := disruptor.NewDisruptor(1024, newEvent)

	var count int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		count += e.Value
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
	d.Stop() // must drain all N before returning

	if count != want {
		t.Fatalf("drained sum=%d want=%d (events dropped on shutdown)", count, want)
	}
}

// TestBackpressureBlocksProducer parks the consumer and checks the producer
// cannot publish more than the ring holds.
func TestBackpressureBlocksProducer(t *testing.T) {
	const capacity = 4
	const total = capacity + 3

	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	var consumed int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		<-proceed // park until released; after close, returns immediately
		atomic.AddInt64(&consumed, 1)
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	var published int64
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		for range total {
			seq := d.Next(1)
			d.Get(seq).Value = 1
			d.Publish(seq, seq)
			atomic.AddInt64(&published, 1)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&published); got >= total {
		t.Fatalf("producer published %d/%d with consumer parked (no back-pressure)", got, total)
	}
	if got := atomic.LoadInt64(&published); got > capacity {
		t.Fatalf("producer published %d, exceeds ring capacity %d", got, capacity)
	}

	close(proceed)
	<-prodDone // contract: producers stopped before Stop
	d.Stop()

	if got := atomic.LoadInt64(&published); got != total {
		t.Fatalf("published=%d want=%d after release", got, total)
	}
	if consumed != total {
		t.Fatalf("consumed=%d want=%d", consumed, total)
	}
}

// TestExceptionHandlerContinues verifies a panicking handler is routed to the
// ExceptionHandler and the pipeline keeps draining the rest.
func TestExceptionHandlerContinues(t *testing.T) {
	const N = 100
	d := disruptor.NewDisruptor(64, newEvent)

	var processed int64
	exc := &countingExceptions{}
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		if e.Value == 50 {
			panic("boom")
		}
		atomic.AddInt64(&processed, 1)
		return nil
	})).HandleExceptionsWith(exc)
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if exc.events.Load() == 0 {
		t.Fatal("expected the handler panic to reach the ExceptionHandler")
	}
	if processed == 0 || processed >= N {
		t.Fatalf("processed=%d, expected to skip the poisoned event but keep going", processed)
	}
}

func ExampleDisruptor() {
	type Order struct{ ID int64 }

	d := disruptor.NewDisruptor(1024, func() Order { return Order{} })
	var total int64
	c := d.Consumer(disruptor.EventHandlerFunc[Order](func(e *Order, seq int64, eob bool) error {
		total += e.ID
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	for i := int64(1); i <= 3; i++ {
		seq := d.Next(1)
		d.Get(seq).ID = i
		d.Publish(seq, seq)
	}
	d.Stop() // drains before returning

	fmt.Println(total)
	// Output: 6
}

// TestTryNextRespectsCapacity: with a parked consumer, exactly capacity claims
// succeed, then TryNext fails instead of blocking.
func TestTryNextRespectsCapacity(t *testing.T) {
	const capacity = 8
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	var consumed int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		<-proceed
		atomic.AddInt64(&consumed, 1)
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	var claimed int64
	for {
		seq, ok := d.TryNext(1)
		if !ok {
			break
		}
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
		claimed++
		if claimed > capacity {
			t.Fatalf("TryNext kept succeeding past capacity %d", capacity)
		}
	}
	if claimed != capacity {
		t.Fatalf("claimed=%d before TryNext failed, want=%d", claimed, capacity)
	}

	close(proceed)
	d.Stop()
	if consumed != capacity {
		t.Fatalf("consumed=%d want=%d", consumed, capacity)
	}
}

// TestRemainingCapacity tracks free slots through publishing and draining.
func TestRemainingCapacity(t *testing.T) {
	const capacity = 8
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		<-proceed
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	if got := d.RemainingCapacity(); got != capacity {
		t.Fatalf("fresh ring RemainingCapacity=%d want=%d", got, capacity)
	}
	for range capacity {
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}
	if got := d.RemainingCapacity(); got != 0 {
		t.Fatalf("full ring RemainingCapacity=%d want 0", got)
	}

	close(proceed)
	d.Stop()
	if got := d.RemainingCapacity(); got != capacity {
		t.Fatalf("drained ring RemainingCapacity=%d want=%d", got, capacity)
	}
}

// TestTryNextMultiProducer drives many concurrent producers through TryNext.
func TestTryNextMultiProducer(t *testing.T) {
	const producers = 4
	const perProd = 1 << 12
	const total = producers * perProd

	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))

	var sum int64 // single consumer; read after Stop
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		sum += e.Value
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perProd {
				for {
					seq, ok := d.TryNext(1)
					if !ok {
						runtime.Gosched()
						continue
					}
					d.Get(seq).Value = seq + 1
					d.Publish(seq, seq)
					break
				}
			}
		})
	}
	wg.Wait()
	d.Stop()

	want := int64(total) * (int64(total) + 1) / 2
	if sum != want {
		t.Fatalf("checksum=%d want=%d (lost, duplicated, or corrupted events)", sum, want)
	}
}
