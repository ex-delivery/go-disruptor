# go-disruptor

**English** | [简体中文](README.zh-CN.md)

A generic, lock-free ring buffer for ultra-low-latency producer/consumer
pipelines in Go, modeled on the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/).

The hot path never allocates: slots are pre-allocated `T` values reused as
sequences advance. Coordination is done with cache-line-isolated atomic cursors
instead of channels or mutexes.

```
goos: darwin  goarch: arm64  cpu: Apple M4  (Go 1.26.2)

# throughput — 0 allocs/op
BenchmarkSPSC            9.4 ns/op    # disruptor: 1 producer -> 1 consumer
BenchmarkMPSC          282   ns/op    # disruptor: 4 producers -> 1 consumer
BenchmarkChannelSPSC    25   ns/op    # buffered channel, for contrast

# one-way latency (publish -> consume)
BenchmarkLatencySPSC          p50 ~0     p99 ~5µs     # ~0 = below clock resolution
BenchmarkChannelLatencySPSC   p50 ~67µs  p99 ~152µs   # buffered channel
```

## Features

- **Generic** — `Disruptor[T]` over any value type; no `interface{}`, no boxing.
- **Zero-allocation hot path** — slots are pre-built once; publish/consume reuse them.
- **Single- and multi-producer** — pick the fast single-writer path or a CAS-based
  multi-writer sequencer.
- **Blocking, non-blocking & context-aware claims** — `Next` applies back-pressure;
  `TryNext` fails fast on a full ring; `NextContext` blocks but is cancellable.
- **DAG consumer graphs** — consumers can depend on others to build parallel
  stages and fan-out/fan-in topologies.
- **Worker pools** — load-balance the stream across N workers, each event handled
  exactly once (vs. broadcast consumers).
- **Pluggable wait strategies** — trade latency for CPU: busy-spin, yielding
  (default), or sleeping.
- **Bounded, graceful shutdown** — `Stop` drains every published event first;
  `StopContext` adds a deadline so a wedged consumer can never hang shutdown.
- **Panic isolation** — a per-consumer/pool handler or a disruptor-wide
  `WithPanicHandler` default recovers a panicking batch and keeps running.
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

d := disruptor.NewDisruptor[Event](1024, func() Event { return Event{} })

c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
    for s := lo; s <= hi; s++ {
        _ = buf[s&mask] // process the slot
    }
})
d.RegisterConsumer(c)
d.Start()
defer d.Stop()

seq := d.Next(1)        // claim one slot (blocks under back-pressure)
d.Get(seq).Value = 42   // fill it in place
d.Publish(seq, seq)     // make it visible to consumers
```

`NewDisruptor` rounds the capacity up to a power of two so the slot index can be
computed with `seq & mask` instead of a modulo.

## Multiple producers

Pass `WithProducerFunc(NewMultiProducer)` to allow concurrent writer goroutines.
The default `NewSingleProducer` is faster but safe for exactly one writer.

```go
d := disruptor.NewDisruptor[Event](1024, newEvent,
    disruptor.WithProducerFunc(disruptor.NewMultiProducer))
// Any number of goroutines may now call d.Next / d.Publish.
```

## Non-blocking publish

`Next` blocks (per the wait strategy) until the slowest consumer frees room.
`TryNext` instead returns `(0, false)` immediately when the ring is full — the
safe way to publish from a goroutine that must also react to shutdown, since
`Stop` cannot unblock a producer parked in `Next`.

```go
if seq, ok := d.TryNext(1); ok {
    d.Get(seq).Value = 42
    d.Publish(seq, seq)
} else {
    // ring is full right now — drop, retry, or back off
}

free := d.RemainingCapacity() // free slots right now (an estimate under contention)
```

## Dependency graphs (DAGs)

A consumer can depend on others so it only processes a sequence after they have.
This builds a diamond (fan-out then fan-in):

```go
b := d.Consumer(handlerB)
c := d.Consumer(handlerC)
merge := d.Consumer(handlerMerge).Depends(b, c) // runs only after b and c
d.RegisterConsumer(b, c, merge)
```

All `Depends` wiring must happen before `Start`, and the graph must be acyclic.
Back-pressure automatically gates only on the *sink* consumers (those nobody
depends on), since their cursors trail every ancestor's.

## Worker pools

Where a consumer broadcasts every event to every handler, a `WorkerPool`
load-balances: each event is handled by exactly one worker. Use it to parallelise
independent per-event work across cores.

```go
pool := d.WorkerPool(runtime.NumCPU(), func(buf []Event, mask, seq int64) {
    _ = buf[seq&mask] // handle a single event
})
d.RegisterWorkerPool(pool)
d.Start()
```

The pool gates the producer as a sink (a slot is never overwritten while any
worker is still on it) and drains on shutdown like a consumer. Add `OnPanic` for
per-event panic recovery.

## Wait strategies

`WithWaitStrategy` selects how waiters spin; it applies to both consumers and
producer back-pressure.

| Strategy        | Latency | CPU    | Notes                                   |
|-----------------|---------|--------|-----------------------------------------|
| `BusySpinWait`  | lowest  | high   | Pins a core; use only with a spare core |
| `YieldingWait`  | low     | medium | **Default** — spins, then yields the P  |
| `SleepingWait`  | higher  | low    | Spin → yield → short sleep; for idle streams |

```go
d := disruptor.NewDisruptor[Event](1024, newEvent,
    disruptor.WithWaitStrategy(disruptor.BusySpinWait{}))
```

## Panic handling

By default a panic in an event handler crashes that consumer's goroutine, which
stalls the pipeline. Install `Consumer.OnPanic` (or `WorkerPool.OnPanic`) to
recover and continue, or set a disruptor-wide default with `WithPanicHandler`
that every consumer inherits — recommended for long-running services.

```go
c := d.Consumer(handler).OnPanic(func(recovered any, lo, hi int64) {
    log.Printf("handler panicked on [%d,%d]: %v", lo, hi, recovered)
})
```

## Lifecycle hooks

`OnStart` / `OnShutdown` run in the stage's own goroutine — `OnStart` before it
processes anything, `OnShutdown` after it has drained and before `Stop` returns —
for per-goroutine setup and teardown. On a worker pool they fire once per worker.

```go
c := d.Consumer(handler).
    OnStart(func() { /* warm caches, register metrics */ }).
    OnShutdown(func() { /* flush, release resources */ })
```

## Shutdown contract

`Stop` is drain-then-alert: it waits for every consumer to reach the last
published sequence, then alerts the barriers so the consumer goroutines exit.
Nothing already published is dropped.

1. Stop publishing — the application must stop calling `Next`/`Publish`.
2. Call `Stop`.

Do **not** call `Stop` while a producer may still publish, and do **not** call
`Stop` while a producer is blocked in `Next` on a full ring — `Stop` does not
unblock producers. A producer that must stay responsive to shutdown should claim
with `TryNext` or `NextContext`.

`StopContext(ctx)` is the bounded variant: if a consumer is wedged it returns
`ctx.Err()` at the deadline instead of blocking forever.

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
// s.Published, s.Capacity, s.Free, s.Backpressure,
// s.ConsumerLag[i], s.WorkerPoolLag[i]
```

`WithMetrics(interval, sink)` runs a background sampler that pushes a `Stats`
snapshot to `sink` every interval (plus a final one at shutdown):

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithMetrics(time.Second, func(s disruptor.Stats) {
        log.Printf("published=%d free=%d backpressure=%d lag=%v",
            s.Published, s.Free, s.Backpressure, s.ConsumerLag)
    }))
```

## Concurrency rules

- `Start` before any `Next`/`Publish`.
- With a single producer, only one goroutine may call `Next`/`Publish`.
- With a multi producer, any number of goroutines may.
- Configure consumers (`Depends`/`OnPanic`) before `Start`.
- Never copy a `Sequence`, `RingBuffer`, or `Disruptor` by value — pass pointers.
  (`go vet` flags accidental copies of the embedded atomics.)

## Design notes

- **Sequences** are monotonically increasing cursors embedding `atomic.Int64`,
  padded on both sides so a hot cursor never shares a cache line with its
  neighbours (false sharing).
- **Single-writer publish** is a plain atomic store of the cursor — no gap can
  exist because slots are claimed in order.
- **Multi-writer publish** tags each slot with its lap number (`seq >> shift`) so
  a stale value from a previous lap can never be mistaken for a fresh publish;
  the published cursor is then advanced cooperatively to the highest contiguous
  sequence, so producers never stall on each other's out-of-order commits.
- **Barriers** aggregate a set of upstream sequences and expose their minimum as
  a single gate, used both for consumer progress and producer back-pressure.

See the package documentation (`go doc`) for the full API.

## Testing

```sh
go test -race ./...                       # correctness, incl. DAG + drain + back-pressure
go test -run '^$' -bench . -benchmem ./... # benchmarks
```

## License

Released under the [MIT License](LICENSE). © 2026 gagral.
