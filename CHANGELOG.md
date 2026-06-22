# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres to
[Semantic Versioning](https://semver.org/).

## [v0.2.0] - 2026-06-22

Aligned the event model with the LMAX Disruptor **v4**. This is a breaking change.

### Changed

- `EventHandler` is now an interface with the v4 per-event signature
  `OnEvent(event *T, sequence int64, endOfBatch bool) error` (previously a
  batch-slice function). `EventHandlerFunc` adapts a plain function.
- Failure handling moved to a v4-style `ExceptionHandler`, set per consumer via
  `Consumer.HandleExceptionsWith` or disruptor-wide via
  `Disruptor.HandleExceptionsWith` — replaces `OnPanic` / `WithPanicHandler`.
- Lifecycle hooks are now optional handler interfaces (`LifecycleAware`,
  `BatchStartAware`, `TimeoutAware`) rather than `Consumer.OnStart`/`OnShutdown`
  methods, matching v4 folding them into `EventHandler`.

### Added

- Batch rewind: a handler may return `ErrRewind`; `BatchRewindStrategy`
  (`AlwaysRewind`, `GiveUpAfter`) governs retries.
- `Consumer.MaxBatchSize` caps events processed per batch.
- `Consumer.Timeout` with `TimeoutAware.OnTimeout` for idle notifications.
- `BatchStartAware.OnBatchStart(batchSize, queueDepth)`.

### Removed

- `WorkerPool` / `WorkHandler` (removed in Disruptor v4; partition work across
  multiple consumers instead) and `Stats.WorkerPoolLag`.

## [v0.1.0] - 2026-06-22

Initial release — a generic, lock-free LMAX-style disruptor for Go.

### Added

- Generic `Disruptor[T]` over a power-of-two ring of pre-allocated values, with a
  zero-allocation publish/consume hot path.
- Single- and multi-producer sequencers (`NewSingleProducer`, `NewMultiProducer`).
- Claim variants: blocking `Next`, non-blocking `TryNext`, cancellable
  `NextContext`, plus `RemainingCapacity`.
- Broadcast `Consumer`s with DAG dependencies (`Depends`) over batch
  `EventHandler`s.
- `WorkerPool` for load-balanced consumption — each event handled by exactly one
  worker.
- Wait strategies: `BusySpinWait`, `YieldingWait` (default), `SleepingWait`.
- Graceful drain shutdown (`Stop`) and bounded, context-aware `StopContext`.
- Panic recovery via `Consumer`/`WorkerPool` `OnPanic` and a disruptor-wide
  `WithPanicHandler` default.
- Lifecycle hooks: `OnStart` / `OnShutdown` on consumers and worker pools.
- Observability: pull-based `Stats` (published, free, back-pressure, per-stage
  lag) and an optional `WithMetrics` sampler.
- Cache-line padding tuned per architecture (128 B on amd64/arm64, 64 B
  elsewhere).

[v0.2.0]: https://github.com/ex-delivery/go-disruptor/releases/tag/v0.2.0
[v0.1.0]: https://github.com/ex-delivery/go-disruptor/releases/tag/v0.1.0
