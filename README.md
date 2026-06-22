# go-disruptor

**English** | [简体中文](README.zh-CN.md)

A generic, lock-free ring buffer for ultra-low-latency producer/consumer
pipelines in Go, modeled on the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
(v4 event model).

The hot path never allocates: slots are pre-allocated `T` values reused as
sequences advance. Coordination is done with cache-line-isolated atomic cursors
instead of channels or mutexes.

```
goos: darwin  goarch: arm64  cpu: Apple M4  (Go 1.26.2)

# throughput — 0 allocs/op
BenchmarkSPSC            ~6   ns/op    # disruptor: 1 producer -> 1 consumer
BenchmarkMPSC          ~240  ns/op    # disruptor: 4 producers -> 1 consumer
BenchmarkChannelSPSC    ~25  ns/op    # buffered channel, for contrast

# one-way latency (publish -> consume), representative
BenchmarkLatencySPSC          p50 ~7µs   p99 ~27µs    # disruptor
BenchmarkChannelLatencySPSC   p50 ~41µs  p99 ~147µs   # buffered channel
```

## Features

- **Generic** — `Disruptor[T]` over any value type; no `interface{}`, no boxing.
- **Zero-allocation hot path** — slots are pre-built once; publish/consume reuse them.
- **Disruptor v4 event model** — `EventHandler.OnEvent(event, sequence, endOfBatch)`
  is called once per event; `EventHandlerFunc` adapts a plain function.
- **Single- and multi-producer** — pick the fast single-writer path or a CAS-based
  multi-writer sequencer.
- **Blocking, non-blocking & context-aware claims** — `Next` applies back-pressure;
  `TryNext` fails fast on a full ring; `NextContext` blocks but is cancellable.
- **DAG consumer graphs** — consumers can depend on others to build parallel
  stages and fan-out/fan-in topologies.
- **Batch rewind** — a handler can return `ErrRewind` to reprocess the whole
  batch, governed by a `BatchRewindStrategy`; `MaxBatchSize` caps batch size.
- **Optional handler hooks** — implement `LifecycleAware` (OnStart/OnShutdown),
  `BatchStartAware` (OnBatchStart), or `TimeoutAware` (OnTimeout).
- **Pluggable wait strategies** — trade latency for CPU: busy-spin, yielding
  (default), or sleeping.
- **Bounded, graceful shutdown** — `Stop` drains every published event first;
  `StopContext` adds a deadline so a wedged consumer can never hang shutdown.
- **v4-style exception handling** — route handler errors/panics to an
  `ExceptionHandler` per consumer or disruptor-wide.
- **Observability** — pull-based `Stats` (lag, occupancy, back-pressure count) plus
  an optional `WithMetrics` sampler, both off the hot path.
- **False-sharing protection** — every hot cursor is padded to a cache line
  (128 B on amd64/arm64, 64 B elsewhere).

## Install

```sh
go get github.com/ex-delivery/go-disruptor
```

Requires Go 1.26.2 or newer.

## Quick start (single producer, single consumer)

```go
type Event struct{ Value int64 }

d := disruptor.NewDisruptor(1024, func() Event { return Event{} })

// Handlers are called once per event. EventHandlerFunc adapts a function; a
// struct implementing EventHandler can also add the optional hooks below.
c := d.Consumer(disruptor.EventHandlerFunc[Event](
    func(e *Event, seq int64, endOfBatch bool) error {
        _ = e // process the event
        return nil
    }))
d.RegisterConsumer(c)
d.Start()
defer d.Stop()

seq := d.Next(1)        // claim one slot (blocks under back-pressure)
d.Get(seq).Value = 42   // fill it in place
d.Publish(seq, seq)     // make it visible to the consumer
```

`NewDisruptor` rounds the capacity up to a power of two so the slot index is
`seq & mask` rather than a modulo.

## Multiple producers

Pass `WithProducerFunc(NewMultiProducer)` to allow concurrent writer goroutines.
The default `NewSingleProducer` is faster but safe for exactly one writer.

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithProducerFunc(disruptor.NewMultiProducer))
```

## Non-blocking & context-aware claims

`Next` blocks (per the wait strategy) until the slowest consumer frees room.
`TryNext` returns `(0, false)` immediately when the ring is full; `NextContext`
blocks but returns `ctx.Err()` if its context is cancelled — both let a producer
stay responsive to shutdown (which cannot unblock `Next`).

```go
if seq, ok := d.TryNext(1); ok {
    d.Get(seq).Value = 42
    d.Publish(seq, seq)
}

seq, err := d.NextContext(ctx, 1) // err == ctx.Err() on cancel
free := d.RemainingCapacity()     // free slots right now (an estimate)
```

## Dependency graphs (DAGs)

A consumer can depend on others so it only processes a sequence after they have:

```go
b := d.Consumer(handlerB)
c := d.Consumer(handlerC)
merge := d.Consumer(handlerMerge).Depends(b, c) // runs only after b and c
d.RegisterConsumer(b, c, merge)
```

All `Depends` wiring must happen before `Start`, and the graph must be acyclic.
Back-pressure automatically gates only on the *sink* consumers.

## Exception handling

A non-rewind error returned from `OnEvent`, or a panic inside it, is routed to the
consumer's `ExceptionHandler`; panics in the lifecycle callbacks go to
`HandleOnStartException` / `HandleOnShutdownException`. Set one per consumer with
`HandleExceptionsWith`, or a disruptor-wide default with
`Disruptor.HandleExceptionsWith`. With none set, returned errors are ignored and
panics propagate (fail-fast) — set one for long-running services.

```go
c := d.Consumer(handler).HandleExceptionsWith(myExceptionHandler)
```

## Batch rewind & max batch size

A handler can return `ErrRewind` to ask that the whole current batch be
reprocessed from its first sequence (handlers must be idempotent across a rewind).
Enable it with a `BatchRewindStrategy`:

```go
c := d.Consumer(handler).
    WithRewindStrategy(disruptor.GiveUpAfter{Max: 3}). // or AlwaysRewind{}
    MaxBatchSize(256)                                  // cap events per batch
```

Without a strategy, `ErrRewind` is treated like any other error.

## Lifecycle & timeout hooks

Implement the optional interfaces on your handler; the processor detects them
(Disruptor v4 folds these into `EventHandler` as default methods):

```go
type Handler struct{}
func (Handler) OnEvent(e *Event, seq int64, endOfBatch bool) error { return nil }
func (Handler) OnStart()                       {} // LifecycleAware
func (Handler) OnShutdown()                    {} // runs before Stop returns
func (Handler) OnBatchStart(size, depth int64) {} // BatchStartAware
func (Handler) OnTimeout(seq int64)            {} // TimeoutAware (needs Timeout)
```

`OnTimeout` fires after `Consumer.Timeout(d)` of idleness with no new events.

## Wait strategies

`WithWaitStrategy` selects how waiters spin; it applies to both consumers and
producer back-pressure.

| Strategy        | Latency | CPU    | Notes                                   |
|-----------------|---------|--------|-----------------------------------------|
| `BusySpinWait`  | lowest  | high   | Pins a core; use only with a spare core |
| `YieldingWait`  | low     | medium | **Default** — spins, then yields the P  |
| `SleepingWait`  | higher  | low    | Spin → yield → short sleep; idle streams |

## Shutdown contract

`Stop` is drain-then-alert: it waits for every consumer to reach the last
published sequence, then alerts the barriers so the goroutines exit. Nothing
already published is dropped.

1. Stop publishing — the application must stop calling `Next`/`Publish`.
2. Call `Stop` (or `StopContext`).

Do **not** call `Stop` while a producer may still publish or is blocked in `Next`
on a full ring — `Stop` does not unblock producers. `StopContext(ctx)` is the
bounded variant: a wedged consumer makes it return `ctx.Err()` at the deadline
instead of hanging.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := d.StopContext(ctx); err != nil {
    log.Printf("shutdown did not drain cleanly: %v", err)
}
```

## Observability

`Stats()` is a cheap, pull-based snapshot for periodic scraping; it only reads
atomic cursors and never touches the hot path.

```go
s := d.Stats()
// s.Published, s.Capacity, s.Free, s.Backpressure, s.ConsumerLag[i]
```

`WithMetrics(interval, sink)` runs a background sampler that pushes a `Stats`
snapshot to `sink` every interval (plus a final one at shutdown).

## Concurrency rules

- `Start` before any `Next`/`Publish`.
- With a single producer, only one goroutine may call `Next`/`Publish`.
- With a multi producer, any number of goroutines may.
- Configure consumers (`Depends` / `HandleExceptionsWith` / `WithRewindStrategy` /
  `MaxBatchSize` / `Timeout`) before `Start`.
- Never copy a `Sequence`, `RingBuffer`, or `Disruptor` by value — pass pointers.

## Design notes

- **Sequences** embed `atomic.Int64`, padded on both sides so a hot cursor never
  shares a cache line with its neighbours (false sharing).
- **Single-writer publish** is a plain atomic store of the cursor; **multi-writer
  publish** tags each slot with its lap number so a stale value from a previous
  lap can never be mistaken for a fresh publish, then advances the published
  cursor cooperatively to the highest contiguous sequence.
- **Barriers** aggregate upstream sequences and expose their minimum as a single
  gate, used both for consumer progress and producer back-pressure.

## Relationship to Disruptor v4

This is a Go port of the Disruptor **v4** event model: per-event `OnEvent` with an
`endOfBatch` flag, lifecycle/batch-start/timeout folded into the handler, batch
rewind, and a max batch size. Like v4 it does **not** include the old
`WorkerPool`/`WorkHandler`. The Go API is idiomatic (interfaces, errors, and
`context`) rather than a literal transliteration.

## Testing

```sh
go test -race ./...                        # correctness, incl. DAG + drain + rewind
go test -run '^$' -bench . -benchmem ./...  # benchmarks
```

## License

Released under the [MIT License](LICENSE). © 2026 gagral.
