package disruptor

import (
	"context"
	"runtime"
)

// StopContext is like Stop but bounded by ctx: it drains published events and
// shuts the consumers down, yet if ctx is cancelled (or its deadline passes)
// before a consumer has caught up to the last published sequence, it alerts the
// consumers anyway and returns ctx.Err().
//
// A non-nil return means shutdown was NOT clean: at least one consumer had not
// finished draining (for example a wedged handler). Because Go cannot force-kill
// a goroutine, such a goroutine may still be running after StopContext returns —
// treat a timeout as a serious condition, not a routine path. On a nil error
// every published event was processed and every stage goroutine has exited.
//
// Like Stop it is idempotent, requires an acyclic consumer graph, and must be
// called only after all producers have stopped publishing.
func (d *Disruptor[T]) StopContext(ctx context.Context) error {
	if !d.started.Load() {
		return nil // nothing was ever launched
	}
	if !d.stopped.CompareAndSwap(false, true) {
		return nil
	}
	final := d.RingBuffer.WriterIndex().Load()

	// Drain: wait for every consumer and worker pool to reach the last published
	// sequence, bailing out early if ctx ends first.
	drained := true
	for _, c := range d.consumers {
		if !drainUntil(ctx, final, c.readIndex.Load) {
			drained = false
			break
		}
	}
	// Alert every consumer so the goroutines that can exit do.
	for _, c := range d.consumers {
		c.signalStop()
	}

	// Stop the metrics sampler (if any), taking its final snapshot.
	if d.samplerStop != nil {
		close(d.samplerStop)
		<-d.samplerDone
	}

	if !drained {
		return ctx.Err() // shutdown was not clean
	}

	for _, c := range d.consumers {
		c.waitDone()
	}
	return nil
}

// drainUntil spins until reached() >= final, returning false if ctx ends first.
func drainUntil(ctx context.Context, final int64, reached func() int64) bool {
	for reached() < final {
		if ctx.Err() != nil {
			return false
		}
		runtime.Gosched()
	}
	return true
}
