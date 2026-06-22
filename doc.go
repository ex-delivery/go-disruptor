// Package disruptor is a generic, lock-free ring buffer for ultra-low-latency
// producer/consumer pipelines, modeled on the LMAX Disruptor.
//
// # Concepts
//
//   - RingBuffer[T]: a power-of-two ring of pre-allocated T values. The hot path
//     never allocates; slots are reused as sequences advance.
//   - Sequence: a cache-line-isolated atomic cursor. The writer cursor marks the
//     highest published slot; each consumer has a read cursor marking its
//     progress.
//   - Barrier: aggregates upstream Sequences and exposes their minimum as a gate.
//     Consumers gate on the writer cursor (plus dependencies); the producer gates
//     on sink consumers for back-pressure.
//   - Producer: claims slots (Next, or the non-blocking TryNext) and commits them
//     (Publish). Single-writer and multi-writer implementations are provided.
//   - Consumer: a goroutine that batch-processes published events via an
//     EventHandler. Every consumer sees every event (broadcast).
//   - WorkerPool: a set of goroutines that load-balance the stream so each event
//     is handled by exactly one worker.
//
// # Basic usage
//
//	type Event struct{ Value int64 }
//
//	d := disruptor.NewDisruptor[Event](1024, func() Event { return Event{} })
//	c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
//	    for s := lo; s <= hi; s++ {
//	        _ = buf[s&mask] // process
//	    }
//	})
//	d.RegisterConsumer(c)
//	d.Start()
//	defer d.Stop()
//
//	seq := d.Next(1)
//	d.Get(seq).Value = 42
//	d.Publish(seq, seq)
//
// # Multiple producers
//
// Pass WithProducerFunc(NewMultiProducer) to allow concurrent writer goroutines.
// The default (NewSingleProducer) is faster but safe for exactly one writer.
//
// # Dependency graphs (DAGs)
//
// A consumer can depend on others so it only processes a sequence after they
// have. This builds parallel stages and fan-in/fan-out topologies:
//
//	b := d.Consumer(handlerB)
//	c := d.Consumer(handlerC)
//	merge := d.Consumer(handlerMerge).Depends(b, c) // runs after b and c
//	d.RegisterConsumer(b, c, merge)
//
// All Depends wiring must happen before Start. The consumer graph must be
// acyclic.
//
// # Worker pools
//
// Where a Consumer broadcasts every event to every handler, a WorkerPool
// load-balances: each published event is processed by exactly one worker. Use it
// to parallelise independent per-event work across cores.
//
//	pool := d.WorkerPool(runtime.NumCPU(), func(buf []Event, mask, seq int64) {
//	    _ = buf[seq&mask] // handle a single event
//	})
//	d.RegisterWorkerPool(pool)
//
// # Wait strategies
//
// WithWaitStrategy selects how waiters spin: BusySpinWait (lowest latency, pins a
// core), YieldingWait (default, near-busy-spin without pinning), or SleepingWait
// (lowest CPU, higher latency). The choice applies to both consumers and
// producer back-pressure.
//
// # Panic handling
//
// By default a panic in an event handler crashes that consumer's goroutine,
// which stalls the pipeline. Install Consumer.OnPanic (or WorkerPool.OnPanic) to
// recover and continue, or set a disruptor-wide default with WithPanicHandler
// that every consumer inherits. Setting one is strongly recommended for
// long-running services.
//
// # Lifecycle hooks
//
// Consumer.OnStart/OnShutdown (and WorkerPool.OnStart/OnShutdown) run in the
// stage's own goroutine — OnStart before it processes anything, OnShutdown after
// it has drained and before the goroutine exits (so it completes before Stop
// returns). Use them for per-goroutine setup and teardown. For a worker pool they
// fire once per worker.
//
// # Shutdown contract
//
// Stop drains all published events before exiting:
//
//  1. Stop publishing (the application stops calling Next/Publish).
//  2. Call Stop. It waits for every consumer to reach the last published
//     sequence, then alerts them to exit.
//
// Do not call Stop while a producer may still publish, and do not call Stop while
// a producer is blocked in Next on a full ring — Stop does not unblock producers.
// A producer that must stay responsive to shutdown should claim with TryNext
// (returns (0, false) when the ring is full) or NextContext (returns ctx.Err()
// when its context is cancelled).
//
// StopContext(ctx) is the bounded variant of Stop: if a consumer is wedged it
// returns ctx.Err() at the deadline instead of blocking forever, so a stuck
// handler can never hang shutdown.
//
// # Concurrency rules
//
//   - Start before any Next/Publish.
//   - With a single producer, only one goroutine may call Next/Publish.
//   - With a multi producer, any number of goroutines may call Next/Publish.
//   - Configure consumers (Depends/OnPanic/OnStart/OnShutdown) and worker pools
//     before Start.
//
// # Observability
//
// Stats returns a cheap, pull-based snapshot — events published, free slots,
// cumulative back-pressure count, and per-consumer / per-pool lag — suitable for
// periodic scraping. WithMetrics wires a background sampler that pushes a Stats
// snapshot to a sink on an interval. Neither adds cost to the publish/consume hot
// path.
package disruptor
