# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres to
[Semantic Versioning](https://semver.org/).

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

[v0.1.0]: https://github.com/ex-delivery/go-disruptor/releases/tag/v0.1.0
