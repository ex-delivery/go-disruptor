package disruptor

import "sync/atomic"

// EventHandler processes a contiguous, already-published range [lower, upper] of
// the ring in one batch. storage and mask are passed directly so the handler can
// index slots as storage[seq&mask] without extra indirection.
type EventHandler[T any] func(storage []T, mask, lower, upper int64)

// Consumer runs in its own goroutine, batch-consuming published events. Each
// Consumer owns a private Barrier (gating on the ring writer cursor plus any
// dependency consumers) that is distinct from the Disruptor-level back-pressure
// barrier.
//
// Wire dependencies and panic handling BEFORE Disruptor.Start; the fluent
// Depends/OnPanic methods return the Consumer for chaining.
type Consumer[T any] struct {
	readIndex  Sequence
	barrier    Barrier
	ringBuffer *RingBuffer[T]
	fn         EventHandler[T]
	onPanic    func(recovered any, lower, upper int64)
	onStart    func()
	onShutdown func()
	dependents int // how many consumers depend on this one (set via Depends)
	stopped    atomic.Bool
	done       chan struct{}
}

// NewConsumer creates a consumer over buffer. It gates on the ring's writer
// cursor by default; add upstream dependencies with Depends.
func NewConsumer[T any](buffer *RingBuffer[T], fn EventHandler[T]) *Consumer[T] {
	c := &Consumer[T]{
		ringBuffer: buffer,
		fn:         fn,
		done:       make(chan struct{}),
	}
	c.readIndex.Store(-1)
	c.barrier.Register(buffer.WriterIndex())
	return c
}

// Depends makes this consumer wait for each of deps to have processed a sequence
// before it processes that sequence itself, forming a DAG stage. Must be called
// before Start. Returns the consumer for chaining.
func (c *Consumer[T]) Depends(deps ...*Consumer[T]) *Consumer[T] {
	for _, dep := range deps {
		c.barrier.Register(&dep.readIndex)
		dep.dependents++
	}
	return c
}

// OnPanic installs a handler invoked if the event handler panics on a batch. If
// set, the panic is recovered and consumption continues with the next batch
// (the panicking batch is skipped). If nil (the default), a panicking handler
// crashes its goroutine — faithful to a "fail fast" model, but it will stall the
// pipeline, so prefer setting a handler in production. Returns the consumer for
// chaining.
func (c *Consumer[T]) OnPanic(h func(recovered any, lower, upper int64)) *Consumer[T] {
	c.onPanic = h
	return c
}

// OnStart registers fn to run once in the consumer's own goroutine just before it
// begins processing events — useful for goroutine-affined setup or a "started"
// signal. Call before Start; returns the consumer for chaining.
func (c *Consumer[T]) OnStart(fn func()) *Consumer[T] {
	c.onStart = fn
	return c
}

// OnShutdown registers fn to run once in the consumer's own goroutine after it
// has stopped and drained, before the goroutine exits — so it completes before
// Stop/StopContext returns. Useful for flushing or releasing per-consumer
// resources. Call before Start; returns the consumer for chaining.
func (c *Consumer[T]) OnShutdown(fn func()) *Consumer[T] {
	c.onShutdown = fn
	return c
}

// ReadIndex exposes this consumer's progress cursor (the last sequence it has
// finished processing).
func (c *Consumer[T]) ReadIndex() *Sequence { return &c.readIndex }

// Lag reports how many published events this consumer has not yet processed
// (writer cursor minus this consumer's progress). A persistently growing lag
// means the consumer cannot keep up with the producer. Cheap and concurrency
// safe; intended for metrics sampling.
func (c *Consumer[T]) Lag() int64 {
	return c.ringBuffer.WriterIndex().Load() - c.readIndex.Load()
}

func (c *Consumer[T]) run() {
	defer close(c.done)
	if c.onShutdown != nil {
		defer c.onShutdown() // runs before close(done), so before waitDone returns
	}
	if c.onStart != nil {
		c.onStart()
	}
	nextSeq := c.readIndex.Load() + 1
	for {
		available, alerted := c.barrier.WaitFor(nextSeq)
		if available >= nextSeq {
			// Data is ready: process it even when alerted, so we fully drain.
			c.dispatch(nextSeq, available)
			c.readIndex.Store(available)
			nextSeq = available + 1
		} else if alerted {
			// No data left and we've been told to stop.
			return
		}
	}
}

func (c *Consumer[T]) dispatch(lower, upper int64) {
	if c.onPanic != nil {
		defer func() {
			if r := recover(); r != nil {
				c.onPanic(r, lower, upper)
			}
		}()
	}
	c.fn(c.ringBuffer.storage, c.ringBuffer.mask, lower, upper)
}

// signalStop alerts the consumer's barrier exactly once so run can exit.
func (c *Consumer[T]) signalStop() {
	if c.stopped.CompareAndSwap(false, true) {
		c.barrier.Alert()
	}
}

// waitDone blocks until the consumer goroutine has returned.
func (c *Consumer[T]) waitDone() { <-c.done }
