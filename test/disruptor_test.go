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

type Event struct{ Value int64 }

func newEvent() Event { return Event{} }

// TestSPSC verifies a single producer / single consumer carries every event.
func TestSPSC(t *testing.T) {
	const N = 1 << 16
	d := disruptor.NewDisruptor(1024, newEvent)

	var sum int64 // single consumer goroutine; read after Stop (happens-before)
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
	d.Stop()

	if sum != want {
		t.Fatalf("sum=%d want=%d", sum, want)
	}
}

// TestMPSC verifies many producers / single consumer: every sequence is
// delivered exactly once AND each slot carries the correct payload (a slot race
// would corrupt the checksum).
func TestMPSC(t *testing.T) {
	const producers = 4
	const perProd = 1 << 14
	const total = producers * perProd

	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))

	var sum int64 // single consumer; read after Stop
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			sum += buf[s&mask].Value
		}
	})
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

// TestRingWrap hammers a tiny ring so sequences lap many times, stressing the
// version-number (lap) gating.
func TestRingWrap(t *testing.T) {
	const N = 1000
	d := disruptor.NewDisruptor(8, newEvent) // capacity 8 -> ~125 laps

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
	d.Stop()

	if sum != want {
		t.Fatalf("sum=%d want=%d", sum, want)
	}
}

// TestDiamondDependencies verifies a fan-out/fan-in DAG: b and c run in parallel,
// merge depends on both. merge must never observe a sequence before both b and c
// have processed it.
func TestDiamondDependencies(t *testing.T) {
	const N = 1 << 14
	d := disruptor.NewDisruptor(1024, newEvent)

	bSeen := make([]int32, N)
	cSeen := make([]int32, N)
	var mergeCount int64
	var violations int64

	b := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			atomic.StoreInt32(&bSeen[buf[s&mask].Value], 1)
		}
	})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			atomic.StoreInt32(&cSeen[buf[s&mask].Value], 1)
		}
	})
	merge := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			v := buf[s&mask].Value
			if atomic.LoadInt32(&bSeen[v]) != 1 || atomic.LoadInt32(&cSeen[v]) != 1 {
				atomic.AddInt64(&violations, 1)
			}
			atomic.AddInt64(&mergeCount, 1)
		}
	}).Depends(b, c)

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

// TestGracefulDrain publishes a burst then stops immediately; every published
// event must still be processed.
func TestGracefulDrain(t *testing.T) {
	const N = 5000
	d := disruptor.NewDisruptor(1024, newEvent)

	var count int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			count += buf[s&mask].Value
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
	d.Stop() // must drain all N before returning

	if count != want {
		t.Fatalf("drained sum=%d want=%d (events dropped on shutdown)", count, want)
	}
}

// TestBackpressureBlocksProducer parks the consumer and checks the producer
// cannot publish more than the ring holds. NOTE: timing-based (uses a sleep);
// run with -race and on a loaded machine it is generally stable but not a strict
// guarantee.
func TestBackpressureBlocksProducer(t *testing.T) {
	const capacity = 4
	const total = capacity + 3

	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{}) // closed to release the consumer
	var consumed int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-proceed // park until released; after close, returns immediately
		atomic.AddInt64(&consumed, hi-lo+1)
	})
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

	// The ring holds `capacity` events and the consumer is parked, so the
	// producer must block before publishing all `total` events.
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&published); got >= total {
		t.Fatalf("producer published %d/%d with consumer parked (no back-pressure)", got, total)
	}
	if got := atomic.LoadInt64(&published); got > capacity {
		t.Fatalf("producer published %d, exceeds ring capacity %d", got, capacity)
	}

	close(proceed) // release consumer; the blocked producer can now finish
	// The Disruptor contract requires every producer to have stopped publishing
	// before Stop. Join the producer goroutine first, otherwise Stop may snapshot
	// a partial writer cursor and alert the consumer before the late publishes are
	// drained (a flaky consumed/published < total).
	<-prodDone
	d.Stop()

	if got := atomic.LoadInt64(&published); got != total {
		t.Fatalf("published=%d want=%d after release", got, total)
	}
	if consumed != total {
		t.Fatalf("consumed=%d want=%d", consumed, total)
	}
}

// TestOnPanicContinues verifies a panicking handler is recovered and the
// pipeline keeps draining the rest.
func TestOnPanicContinues(t *testing.T) {
	const N = 100
	d := disruptor.NewDisruptor(64, newEvent)

	var processed int64
	var panics int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			if buf[s&mask].Value == 50 {
				panic("boom")
			}
			atomic.AddInt64(&processed, 1)
		}
	}).OnPanic(func(any, int64, int64) {
		atomic.AddInt64(&panics, 1)
	})
	d.RegisterConsumer(c)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if panics == 0 {
		t.Fatalf("expected the handler to panic at least once")
	}
	// Everything except the single poisoned batch should have been processed.
	if processed == 0 || processed >= N {
		t.Fatalf("processed=%d, expected to skip the poisoned batch but keep going", processed)
	}
}

func ExampleDisruptor() {
	type Order struct{ ID int64 }

	d := disruptor.NewDisruptor(1024, func() Order { return Order{} })
	var total int64
	c := d.Consumer(func(buf []Order, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			total += buf[s&mask].ID
		}
	})
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

// TestTryNextRespectsCapacity parks the consumer, then claims with the
// non-blocking TryNext. Exactly `capacity` claims must succeed (the consumer's
// read cursor stays put while it is parked), and the next claim must fail
// instead of blocking.
func TestTryNextRespectsCapacity(t *testing.T) {
	const capacity = 8
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{}) // closed to release the parked consumer
	var consumed int64
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-proceed
		atomic.AddInt64(&consumed, hi-lo+1)
	})
	d.RegisterConsumer(c)
	d.Start()

	var claimed int64
	for {
		seq, ok := d.TryNext(1)
		if !ok {
			break // ring full — expected once `capacity` slots are in flight
		}
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
		claimed++
		if claimed > capacity {
			t.Fatalf("TryNext kept succeeding past capacity %d (no back-pressure)", capacity)
		}
	}
	if claimed != capacity {
		t.Fatalf("claimed=%d before TryNext failed, want=%d", claimed, capacity)
	}

	close(proceed) // release the consumer so the pipeline drains
	d.Stop()
	if consumed != capacity {
		t.Fatalf("consumed=%d want=%d", consumed, capacity)
	}
}

// TestRemainingCapacity checks the free-slot count tracks publishing and
// recovers to full once the ring has drained.
func TestRemainingCapacity(t *testing.T) {
	const capacity = 8
	d := disruptor.NewDisruptor(capacity, newEvent)

	proceed := make(chan struct{})
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		<-proceed
	})
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

// TestTryNextMultiProducer drives many concurrent producers through TryNext,
// retrying on a full ring, and verifies every sequence still lands exactly once
// with the correct payload.
func TestTryNextMultiProducer(t *testing.T) {
	const producers = 4
	const perProd = 1 << 12
	const total = producers * perProd

	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))

	var sum int64 // single consumer; read after Stop
	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			sum += buf[s&mask].Value
		}
	})
	d.RegisterConsumer(c)
	d.Start()

	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perProd {
				for {
					seq, ok := d.TryNext(1)
					if !ok {
						runtime.Gosched() // ring full; let the consumer drain
						continue
					}
					d.Get(seq).Value = seq + 1 // payload derived from the sequence
					d.Publish(seq, seq)
					break
				}
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
