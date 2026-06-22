package disruptor

// WorkHandler processes a single published event at sequence seq, indexing the
// ring as storage[seq&mask]. Unlike EventHandler — which receives a batch and
// runs in every Consumer (broadcast) — a WorkHandler runs in exactly one worker
// of a WorkerPool, so each event is handled once.
type WorkHandler[T any] func(storage []T, mask, seq int64)

// WorkerPool runs a set of worker goroutines that compete to consume the same
// stream: each published sequence is handled by exactly one worker
// (load-balanced), in contrast to a Consumer where every consumer sees every
// event. Use it to spread independent per-event work across cores.
//
// Wire it before Start with Disruptor.WorkerPool + RegisterWorkerPool. The pool
// gates on the ring writer cursor and applies back-pressure as a sink: the
// producer never overwrites a slot any worker is still processing.
type WorkerPool[T any] struct {
	ringBuffer   *RingBuffer[T]
	barrier      Barrier
	workSequence Sequence // shared claim cursor; workers CAS to claim the next seq
	workers      []*workProcessor[T]
	fn           WorkHandler[T]
	onPanic      func(recovered any, seq int64)
	onStart      func()
	onShutdown   func()
}

// workProcessor is one worker goroutine. Its readIndex is the highest sequence
// it has fully processed; while processing sequence S it advertises S-1, so the
// pool's minimum read cursor never passes a slot still in flight.
type workProcessor[T any] struct {
	readIndex Sequence
	pool      *WorkerPool[T]
	done      chan struct{}
}

// OnPanic installs a handler invoked if a work handler panics on an event;
// processing then continues with the next event. Without it (and without a
// disruptor-wide WithPanicHandler) a panic crashes the worker goroutine. Returns
// the pool for chaining; call before Start.
func (p *WorkerPool[T]) OnPanic(h func(recovered any, seq int64)) *WorkerPool[T] {
	p.onPanic = h
	return p
}

// OnStart registers fn to run once in each worker's goroutine just before it
// begins processing — so it fires once per worker. Useful for per-worker setup.
// Call before Start; returns the pool for chaining.
func (p *WorkerPool[T]) OnStart(fn func()) *WorkerPool[T] {
	p.onStart = fn
	return p
}

// OnShutdown registers fn to run once in each worker's goroutine after it stops,
// before the goroutine exits — so every call completes before Stop/StopContext
// returns. Useful for per-worker teardown. Call before Start; returns the pool
// for chaining.
func (p *WorkerPool[T]) OnShutdown(fn func()) *WorkerPool[T] {
	p.onShutdown = fn
	return p
}

// Lag reports how many published events the pool has not yet finished (writer
// cursor minus the slowest worker's progress). Cheap and concurrency safe.
func (p *WorkerPool[T]) Lag() int64 {
	return p.ringBuffer.WriterIndex().Load() - p.minSequence()
}

func (p *WorkerPool[T]) start() {
	for _, w := range p.workers {
		go w.run()
	}
}

// gatingSequences returns each worker's read cursor for producer back-pressure.
func (p *WorkerPool[T]) gatingSequences() []*Sequence {
	seqs := make([]*Sequence, len(p.workers))
	for i, w := range p.workers {
		seqs[i] = &w.readIndex
	}
	return seqs
}

// minSequence is the slowest worker's progress, used to drain on shutdown.
func (p *WorkerPool[T]) minSequence() int64 {
	m := p.workers[0].readIndex.Load()
	for _, w := range p.workers[1:] {
		if v := w.readIndex.Load(); v < m {
			m = v
		}
	}
	return m
}

// signalStop alerts the shared barrier once, waking every worker.
func (p *WorkerPool[T]) signalStop() { p.barrier.Alert() }

func (p *WorkerPool[T]) waitDone() {
	for _, w := range p.workers {
		<-w.done
	}
}

func (w *workProcessor[T]) run() {
	defer close(w.done)
	p := w.pool
	if p.onShutdown != nil {
		defer p.onShutdown()
	}
	if p.onStart != nil {
		p.onStart()
	}
	claimed := int64(-1)
	needClaim := true
	for {
		if needClaim {
			// Claim the next sequence by CAS on the shared cursor. Exactly one
			// worker wins each sequence.
			for {
				claimed = p.workSequence.Load() + 1
				if p.workSequence.CompareAndSwap(claimed-1, claimed) {
					break
				}
			}
			// Everything strictly before `claimed` is done (claimed earlier by
			// workers that have since moved on); advertise that so back-pressure
			// won't overwrite the slot we are about to read.
			w.readIndex.Store(claimed - 1)
			needClaim = false
		}
		available, alerted := p.barrier.WaitFor(claimed)
		if available >= claimed {
			// Data is ready: process it even when alerted, so we fully drain.
			w.dispatch(claimed)
			w.readIndex.Store(claimed)
			needClaim = true
		} else if alerted {
			// Our claimed sequence was never published and we've been told to
			// stop; abandon it and exit.
			return
		}
	}
}

func (w *workProcessor[T]) dispatch(seq int64) {
	p := w.pool
	if p.onPanic != nil {
		defer func() {
			if r := recover(); r != nil {
				p.onPanic(r, seq)
			}
		}()
	}
	p.fn(p.ringBuffer.storage, p.ringBuffer.mask, seq)
}
