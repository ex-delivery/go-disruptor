package test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// TestWorkerPoolProcessesEachEventOnce checks the load-balancing guarantee:
// across N workers every published event is handled by exactly one worker — none
// dropped, none handled twice.
func TestWorkerPoolProcessesEachEventOnce(t *testing.T) {
	const workers = 4
	const N = 1 << 14
	d := disruptor.NewDisruptor(1024, newEvent)

	seen := make([]int32, N)
	var processed, dupes int64
	pool := d.WorkerPool(workers, func(buf []Event, mask, seq int64) {
		v := buf[seq&mask].Value // 1..N
		if !atomic.CompareAndSwapInt32(&seen[v-1], 0, 1) {
			atomic.AddInt64(&dupes, 1) // a second worker touched the same event
		}
		atomic.AddInt64(&processed, 1)
	})
	d.RegisterWorkerPool(pool)
	d.Start()

	for i := int64(1); i <= N; i++ {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}
	d.Stop()

	if processed != N {
		t.Fatalf("processed=%d want=%d (events lost on shutdown?)", processed, N)
	}
	if dupes != 0 {
		t.Fatalf("%d events handled by more than one worker", dupes)
	}
	for i, s := range seen {
		if s != 1 {
			t.Fatalf("event %d handled %d times, want exactly 1", i+1, s)
		}
	}
}

// TestWorkerPoolMultiProducer is the full MPMC case: many producers feeding a
// pool of workers. The checksum catches any lost, duplicated, or corrupted event.
func TestWorkerPoolMultiProducer(t *testing.T) {
	const producers = 3
	const workers = 4
	const perProd = 1 << 12
	const total = producers * perProd
	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))

	var sum, processed int64
	pool := d.WorkerPool(workers, func(buf []Event, mask, seq int64) {
		atomic.AddInt64(&sum, buf[seq&mask].Value)
		atomic.AddInt64(&processed, 1)
	})
	d.RegisterWorkerPool(pool)
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

	if processed != total {
		t.Fatalf("processed=%d want=%d", processed, total)
	}
	want := int64(total) * (int64(total) + 1) / 2 // sum of payloads 1..total
	if sum != want {
		t.Fatalf("checksum=%d want=%d (lost, duplicated, or corrupted events)", sum, want)
	}
}

// TestWorkerPoolPanicContinues verifies OnPanic recovers a poisoned event and
// the pool keeps draining the rest. StopContext with a deadline turns a stall
// into a failure instead of a hang.
func TestWorkerPoolPanicContinues(t *testing.T) {
	const N = 200
	d := disruptor.NewDisruptor(64, newEvent)

	var processed, panics int64
	pool := d.WorkerPool(3, func(buf []Event, mask, seq int64) {
		if buf[seq&mask].Value == 100 {
			panic("boom")
		}
		atomic.AddInt64(&processed, 1)
	}).OnPanic(func(recovered any, seq int64) {
		atomic.AddInt64(&panics, 1)
	})
	d.RegisterWorkerPool(pool)
	d.Start()

	for i := range int64(N) {
		seq := d.Next(1)
		d.Get(seq).Value = i
		d.Publish(seq, seq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.StopContext(ctx); err != nil {
		t.Fatalf("pool stalled after a panic: %v", err)
	}

	if panics != 1 {
		t.Fatalf("panics=%d, want exactly 1", panics)
	}
	if processed != N-1 {
		t.Fatalf("processed=%d, want %d (all but the poisoned event)", processed, N-1)
	}
}
