package test

import "sync/atomic"

// Event is the shared test payload.
type Event struct{ Value int64 }

func newEvent() Event { return Event{} }

// countingExceptions is a test ExceptionHandler[Event] that tallies each callback.
type countingExceptions struct {
	events    atomic.Int64
	starts    atomic.Int64
	shutdowns atomic.Int64
}

func (c *countingExceptions) HandleEventException(err error, seq int64, e *Event) { c.events.Add(1) }
func (c *countingExceptions) HandleOnStartException(any)                          { c.starts.Add(1) }
func (c *countingExceptions) HandleOnShutdownException(any)                       { c.shutdowns.Add(1) }
