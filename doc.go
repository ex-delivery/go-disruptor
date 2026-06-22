// Package disruptor is a generic, lock-free ring buffer for ultra-low-latency
// producer/consumer pipelines, modeled on the LMAX Disruptor (v4 event model).
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
//   - Producer: claims slots (Next, the non-blocking TryNext, or the cancellable
//     NextContext) and commits them (Publish). Single- and multi-writer
//     implementations are provided.
//   - Consumer: a goroutine that drives an EventHandler, calling OnEvent once per
//     event with an endOfBatch flag — the Disruptor v4 model.
//
// # Event handlers
//
// An EventHandler is invoked once per event. The common case uses EventHandlerFunc
// to adapt a function:
//
//	type Event struct{ Value int64 }
//
//	d := disruptor.NewDisruptor[Event](1024, func() Event { return Event{} })
//	c := d.Consumer(disruptor.EventHandlerFunc[Event](
//	    func(e *Event, seq int64, endOfBatch bool) error {
//	        _ = e // process the event
//	        return nil
//	    }))
//	d.RegisterConsumer(c)
//	d.Start()
//	defer d.Stop()
//
//	seq := d.Next(1)
//	d.Get(seq).Value = 42
//	d.Publish(seq, seq)
//
// A handler may also implement the optional interfaces LifecycleAware
// (OnStart/OnShutdown), BatchStartAware (OnBatchStart), and TimeoutAware
// (OnTimeout). Disruptor v4 folds these into EventHandler as default methods; Go
// has no default methods, so they are optional interfaces the processor detects.
//
// # Multiple producers
//
// Pass WithProducerFunc(NewMultiProducer) to allow concurrent writer goroutines.
// The default (NewSingleProducer) is faster but safe for exactly one writer.
//
// # Dependency graphs (DAGs)
//
// A consumer can depend on others so it only processes a sequence after they
// have, building parallel stages and fan-in/fan-out topologies:
//
//	b := d.Consumer(handlerB)
//	c := d.Consumer(handlerC)
//	merge := d.Consumer(handlerMerge).Depends(b, c) // runs after b and c
//	d.RegisterConsumer(b, c, merge)
//
// All Depends wiring must happen before Start. The consumer graph must be acyclic.
//
// # Wait strategies
//
// WithWaitStrategy selects how waiters spin: BusySpinWait (lowest latency, pins a
// core), YieldingWait (default, near-busy-spin without pinning), or SleepingWait
// (lowest CPU, higher latency). The choice applies to both consumers and producer
// back-pressure.
//
// # Exception handling
//
// A non-rewind error returned from OnEvent, or a panic inside it, is routed to the
// consumer's ExceptionHandler (HandleEventException); panics in the lifecycle
// callbacks go to HandleOnStartException / HandleOnShutdownException. Install one
// per consumer with Consumer.HandleExceptionsWith, or a disruptor-wide default
// with Disruptor.HandleExceptionsWith. With none set, returned errors are ignored
// and panics propagate (fail-fast) — set one for long-running services.
//
// # Batch rewind
//
// A handler may return ErrRewind to ask that the whole current batch be
// reprocessed from its first sequence (v4 batch rewind). Enable it with
// Consumer.WithRewindStrategy (AlwaysRewind, or GiveUpAfter for a bounded number
// of retries); without a strategy, ErrRewind is treated like any other error.
// Consumer.MaxBatchSize caps how many events the handler is given per batch.
//
// # Lifecycle and timeout hooks
//
// If the handler implements LifecycleAware, OnStart runs in the processor
// goroutine before any event and OnShutdown after it has drained (before Stop
// returns). If it implements TimeoutAware and Consumer.Timeout is set, OnTimeout
// fires after that much idleness with no new events.
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
// when its context is cancelled). StopContext(ctx) is the bounded variant of Stop:
// a wedged consumer makes it return ctx.Err() at the deadline rather than hang.
//
// # Observability
//
// Stats returns a cheap, pull-based snapshot — events published, free slots,
// cumulative back-pressure count, and per-consumer lag — suitable for periodic
// scraping. WithMetrics wires a background sampler that pushes a Stats snapshot to
// a sink on an interval. Neither adds cost to the publish/consume hot path.
//
// # Concurrency rules
//
//   - Start before any Next/Publish.
//   - With a single producer, only one goroutine may call Next/Publish.
//   - With a multi producer, any number of goroutines may call Next/Publish.
//   - Configure consumers (Depends / HandleExceptionsWith / WithRewindStrategy /
//     MaxBatchSize / Timeout) before Start.
package disruptor
