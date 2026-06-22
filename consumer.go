package disruptor

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Consumer runs an EventHandler in its own goroutine, consuming published events.
// Each Consumer owns a private Barrier (gating on the ring writer cursor plus any
// dependency consumers) distinct from the Disruptor-level back-pressure barrier.
//
// Mirroring Disruptor v4, the handler is called once per event via OnEvent and
// may implement the optional LifecycleAware / BatchStartAware / TimeoutAware
// interfaces. Configure dependencies, exception handling, rewind, max batch size,
// and timeout BEFORE Disruptor.Start; the fluent methods return the Consumer for
// chaining.
type Consumer[T any] struct {
	readIndex  Sequence
	barrier    Barrier
	ringBuffer *RingBuffer[T]
	handler    EventHandler[T]
	exceptions ExceptionHandler[T]
	rewind     BatchRewindStrategy
	maxBatch   int64         // 0 = unlimited
	timeout    time.Duration // 0 = block indefinitely (no OnTimeout)
	dependents int           // how many consumers depend on this one (set via Depends)
	stopped    atomic.Bool
	done       chan struct{}
}

// NewConsumer creates a consumer over buffer driven by handler. It gates on the
// ring's writer cursor by default; add upstream dependencies with Depends.
func NewConsumer[T any](buffer *RingBuffer[T], handler EventHandler[T]) *Consumer[T] {
	c := &Consumer[T]{
		ringBuffer: buffer,
		handler:    handler,
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

// HandleExceptionsWith installs the v4-style ExceptionHandler for this consumer,
// overriding any disruptor-wide default. Returns the consumer for chaining.
func (c *Consumer[T]) HandleExceptionsWith(h ExceptionHandler[T]) *Consumer[T] {
	c.exceptions = h
	return c
}

// WithRewindStrategy enables batch rewind: when the handler returns ErrRewind the
// strategy decides whether to reprocess the batch. Returns the consumer for
// chaining.
func (c *Consumer[T]) WithRewindStrategy(s BatchRewindStrategy) *Consumer[T] {
	c.rewind = s
	return c
}

// MaxBatchSize caps how many events the handler is given per batch (v4
// setMaxBatchSize); n <= 0 means unlimited. Returns the consumer for chaining.
func (c *Consumer[T]) MaxBatchSize(n int64) *Consumer[T] {
	if n > 0 {
		c.maxBatch = n
	}
	return c
}

// Timeout makes the consumer call the handler's OnTimeout (if it implements
// TimeoutAware) after d of idleness with no new events; d <= 0 disables it.
// Returns the consumer for chaining.
func (c *Consumer[T]) Timeout(d time.Duration) *Consumer[T] {
	c.timeout = d
	return c
}

// ReadIndex exposes this consumer's progress cursor (the last sequence it has
// finished processing).
func (c *Consumer[T]) ReadIndex() *Sequence { return &c.readIndex }

// Lag reports how many published events this consumer has not yet processed
// (writer cursor minus this consumer's progress). Cheap and concurrency safe;
// intended for metrics sampling.
func (c *Consumer[T]) Lag() int64 {
	return c.ringBuffer.WriterIndex().Load() - c.readIndex.Load()
}

func (c *Consumer[T]) run() {
	defer close(c.done)
	if la, ok := c.handler.(LifecycleAware); ok {
		c.invokeStart(la)
		defer c.invokeShutdown(la)
	}

	nextSeq := c.readIndex.Load() + 1
	for {
		var available int64
		var alerted, timedOut bool
		if c.timeout > 0 {
			available, alerted, timedOut = c.barrier.waitForTimeout(nextSeq, c.timeout)
		} else {
			available, alerted = c.barrier.WaitFor(nextSeq)
		}

		switch {
		case available >= nextSeq:
			// Data is ready: process it even when alerted, so we fully drain.
			end := available
			if c.maxBatch > 0 && end-nextSeq+1 > c.maxBatch {
				end = nextSeq + c.maxBatch - 1
			}
			c.processBatch(nextSeq, end, available)
			c.readIndex.Store(end)
			nextSeq = end + 1
		case timedOut:
			if ta, ok := c.handler.(TimeoutAware); ok {
				ta.OnTimeout(nextSeq - 1)
			}
		case alerted:
			// No data left and we've been told to stop.
			return
		}
	}
}

// processBatch runs OnBatchStart (if any) once, then dispatches [lo, hi] event by
// event, honouring rewind requests.
func (c *Consumer[T]) processBatch(lo, hi, available int64) {
	if bsa, ok := c.handler.(BatchStartAware); ok {
		bsa.OnBatchStart(hi-lo+1, available-lo+1)
	}
	for attempts := 0; ; attempts++ {
		rewind := false
		for seq := lo; seq <= hi; seq++ {
			err := c.dispatch(c.ringBuffer.Get(seq), seq, seq == hi)
			if err == nil {
				continue
			}
			if errors.Is(err, ErrRewind) && c.rewind != nil {
				if c.rewind.HandleRewind(attempts) == Rewind {
					rewind = true
					break // restart the whole batch from lo
				}
				// gave up: fall through and report it like any other error
			}
			c.handleEventException(err, seq, c.ringBuffer.Get(seq))
		}
		if !rewind {
			return
		}
	}
}

// dispatch calls OnEvent, converting a panic into an exception-handler call (or
// re-panicking when no handler is set, for fail-fast).
func (c *Consumer[T]) dispatch(event *T, seq int64, endOfBatch bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if c.exceptions != nil {
				c.exceptions.HandleEventException(fmt.Errorf("disruptor: handler panic: %v", r), seq, event)
				err = nil // panic handled; do not also rewind/report
			} else {
				panic(r)
			}
		}
	}()
	return c.handler.OnEvent(event, seq, endOfBatch)
}

func (c *Consumer[T]) handleEventException(err error, seq int64, event *T) {
	if c.exceptions != nil {
		c.exceptions.HandleEventException(err, seq, event)
	}
	// else: no handler — a returned error is dropped and processing continues.
}

func (c *Consumer[T]) invokeStart(la LifecycleAware) {
	defer func() {
		if r := recover(); r != nil {
			if c.exceptions != nil {
				c.exceptions.HandleOnStartException(r)
			} else {
				panic(r)
			}
		}
	}()
	la.OnStart()
}

func (c *Consumer[T]) invokeShutdown(la LifecycleAware) {
	defer func() {
		if r := recover(); r != nil {
			if c.exceptions != nil {
				c.exceptions.HandleOnShutdownException(r)
			} else {
				panic(r)
			}
		}
	}()
	la.OnShutdown()
}

// signalStop alerts the consumer's barrier exactly once so run can exit.
func (c *Consumer[T]) signalStop() {
	if c.stopped.CompareAndSwap(false, true) {
		c.barrier.Alert()
	}
}

// waitDone blocks until the consumer goroutine has returned.
func (c *Consumer[T]) waitDone() { <-c.done }
