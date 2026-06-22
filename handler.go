package disruptor

import "errors"

// EventHandler processes published events one at a time, mirroring the Disruptor
// v4 EventHandler: OnEvent receives a pointer to the ring slot, its sequence, and
// whether it is the last event of the current batch.
//
// OnEvent's error return:
//   - nil: processed successfully.
//   - ErrRewind: ask the consumer's BatchRewindStrategy to reprocess the whole
//     current batch from its start (v4 batch rewind). Honoured only if a
//     BatchRewindStrategy is configured; otherwise treated like any other error.
//   - any other error: handed to the consumer's ExceptionHandler.
//
// A handler may also implement any of the optional interfaces LifecycleAware,
// BatchStartAware, and TimeoutAware. In Disruptor v4 these are default methods on
// EventHandler; Go has no default methods, so they are optional interfaces the
// processor detects by type assertion.
type EventHandler[T any] interface {
	OnEvent(event *T, sequence int64, endOfBatch bool) error
}

// EventHandlerFunc adapts a plain function to EventHandler for the common case of
// a handler with no lifecycle/batch/timeout callbacks. (A Go convenience; the v4
// contract remains the EventHandler interface.)
type EventHandlerFunc[T any] func(event *T, sequence int64, endOfBatch bool) error

// OnEvent calls the underlying function.
func (f EventHandlerFunc[T]) OnEvent(event *T, sequence int64, endOfBatch bool) error {
	return f(event, sequence, endOfBatch)
}

// LifecycleAware, if implemented by an EventHandler, is notified when the
// processor goroutine starts (OnStart, before any event) and after it has stopped
// and drained (OnShutdown, before Stop returns).
type LifecycleAware interface {
	OnStart()
	OnShutdown()
}

// BatchStartAware, if implemented, is called once at the start of each batch with
// the batch size and the queue depth (events available from this batch's start —
// how far behind the handler is).
type BatchStartAware interface {
	OnBatchStart(batchSize, queueDepth int64)
}

// TimeoutAware, if implemented, is called when the consumer's wait times out with
// no new events. Requires Consumer.Timeout to be set.
type TimeoutAware interface {
	OnTimeout(sequence int64)
}

// ErrRewind, returned from EventHandler.OnEvent, requests that the whole current
// batch be reprocessed from its first sequence, subject to the consumer's
// BatchRewindStrategy. It is the Go analogue of v4's RewindableException.
var ErrRewind = errors.New("disruptor: rewind batch")

// RewindAction is the decision a BatchRewindStrategy returns.
type RewindAction int

const (
	// Rewind reprocesses the batch from its first sequence.
	Rewind RewindAction = iota
	// GiveUp stops rewinding; the triggering error goes to the ExceptionHandler.
	GiveUp
)

// BatchRewindStrategy decides whether a batch whose handler returned ErrRewind
// should be retried, given how many rewinds it has already done (0 on the first).
type BatchRewindStrategy interface {
	HandleRewind(attempts int) RewindAction
}

// AlwaysRewind retries a rewinding batch indefinitely (v4 SimpleBatchRewindStrategy).
type AlwaysRewind struct{}

// HandleRewind always rewinds.
func (AlwaysRewind) HandleRewind(int) RewindAction { return Rewind }

// GiveUpAfter rewinds up to Max times, then gives up (v4
// EventuallyGiveUpBatchRewindStrategy).
type GiveUpAfter struct{ Max int }

// HandleRewind rewinds while attempts < Max.
func (g GiveUpAfter) HandleRewind(attempts int) RewindAction {
	if attempts < g.Max {
		return Rewind
	}
	return GiveUp
}

// ExceptionHandler is notified of failures in an event handler, mirroring
// Disruptor v4's ExceptionHandler. A non-rewind error returned from OnEvent — or
// a panic in OnEvent — goes to HandleEventException; panics in the lifecycle
// callbacks go to HandleOnStartException / HandleOnShutdownException. With no
// ExceptionHandler set, returned errors are ignored and panics propagate
// (fail-fast).
type ExceptionHandler[T any] interface {
	HandleEventException(err error, sequence int64, event *T)
	HandleOnStartException(recovered any)
	HandleOnShutdownException(recovered any)
}
