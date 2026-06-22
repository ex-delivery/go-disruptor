package disruptor

import (
	"context"
	"math/bits"
	"sync/atomic"
	"time"
)

// Option configures a Disruptor at construction.
type Option func(*Options)

// ProducerFunc builds a Producer from the ring's writer cursor, the gating
// (back-pressure) barrier, and the capacity. NewSingleProducer and
// NewMultiProducer both satisfy it.
type ProducerFunc func(writeIndex *Sequence, barrier *Barrier, capacity int64) Producer

// Options holds resolved construction settings.
type Options struct {
	producerFunc    ProducerFunc
	wait            WaitStrategy
	panicHandler    func(recovered any, lower, upper int64)
	metricsInterval time.Duration
	metricsSink     func(Stats)
}

// WithProducerFunc selects the producer implementation (default
// NewSingleProducer). Pass NewMultiProducer for concurrent writers.
func WithProducerFunc(f ProducerFunc) Option {
	return func(o *Options) { o.producerFunc = f }
}

// WithWaitStrategy selects the wait strategy for both consumers and producer
// back-pressure (default YieldingWait).
func WithWaitStrategy(w WaitStrategy) Option {
	return func(o *Options) { o.wait = w }
}

// WithPanicHandler installs a default panic handler applied to every consumer
// built via Disruptor.Consumer; a consumer's own OnPanic overrides it. Strongly
// recommended for long-running services: with no handler at all, a panicking
// event handler crashes its consumer goroutine and stalls the whole pipeline.
// The handler recovers the panicking batch and consumption continues.
func WithPanicHandler(h func(recovered any, lower, upper int64)) Option {
	return func(o *Options) { o.panicHandler = h }
}

// WithMetrics starts a background goroutine that samples Stats every interval and
// passes each snapshot to sink, until the disruptor is stopped (one final sample
// is taken at shutdown). Sampling is pull-based and off the hot path. A
// non-positive interval or a nil sink disables it.
func WithMetrics(interval time.Duration, sink func(Stats)) Option {
	return func(o *Options) {
		o.metricsInterval = interval
		o.metricsSink = sink
	}
}

func (o *Options) apply() {
	if o.producerFunc == nil {
		o.producerFunc = NewSingleProducer
	}
	if o.wait == nil {
		o.wait = YieldingWait{}
	}
}

// Disruptor ties together a ring buffer, a producer, and a set of consumers.
// It embeds Producer (Next/Publish) and *RingBuffer (Get/Capacity/WriterIndex),
// so the common publish path reads naturally:
//
//	seq := d.Next(1)
//	d.Get(seq).Field = value
//	d.Publish(seq, seq)
//
// Lifecycle: build, register consumers, Start, publish, Stop. Start must be
// called before any Next/Publish; Stop must be called only after all producers
// have stopped publishing.
type Disruptor[T any] struct {
	Producer
	*RingBuffer[T]

	barrier         *Barrier // producer back-pressure gate (sink consumers)
	wait            WaitStrategy
	consumers       []*Consumer[T]
	pools           []*WorkerPool[T]
	defaultPanic    func(recovered any, lower, upper int64) // applied to each Consumer
	metricsInterval time.Duration
	metricsSink     func(Stats)
	samplerStop     chan struct{}
	samplerDone     chan struct{}
	started         atomic.Bool
	stopped         atomic.Bool
}

// NewDisruptor builds a disruptor with a ring of (at least) capacity slots
// rounded up to a power of two, each initialised by factory.
func NewDisruptor[T any](capacity int64, factory func() T, opts ...Option) *Disruptor[T] {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}
	options.apply()

	capacity = CeilPowerOf2(capacity)
	rb := NewRingBuffer(capacity, factory)

	gate := &Barrier{countStalls: true} // the producer gate tracks back-pressure
	gate.setStrategy(options.wait)

	return &Disruptor[T]{
		Producer:        options.producerFunc(rb.WriterIndex(), gate, capacity),
		RingBuffer:      rb,
		barrier:         gate,
		wait:            options.wait,
		defaultPanic:    options.panicHandler,
		metricsInterval: options.metricsInterval,
		metricsSink:     options.metricsSink,
	}
}

// Consumer creates a consumer bound to this disruptor's ring. Configure it
// (Depends/OnPanic) and then pass it to RegisterConsumer before Start.
func (d *Disruptor[T]) Consumer(fn EventHandler[T]) *Consumer[T] {
	c := NewConsumer(d.RingBuffer, fn)
	c.barrier.setStrategy(d.wait)
	c.onPanic = d.defaultPanic // default; a later OnPanic call overrides it
	return c
}

// RegisterConsumer enrolls one or more consumers to be launched at Start.
func (d *Disruptor[T]) RegisterConsumer(cs ...*Consumer[T]) {
	d.consumers = append(d.consumers, cs...)
}

// WorkerPool creates a pool of size worker goroutines that load-balance the
// stream: each published event is handled by exactly one worker via fn. Gates
// on the ring writer cursor. Configure it (OnPanic) and pass it to
// RegisterWorkerPool before Start. size < 1 is treated as 1.
func (d *Disruptor[T]) WorkerPool(size int, fn WorkHandler[T]) *WorkerPool[T] {
	if size < 1 {
		size = 1
	}
	p := &WorkerPool[T]{
		ringBuffer: d.RingBuffer,
		fn:         fn,
	}
	p.barrier.setStrategy(d.wait)
	p.barrier.Register(d.RingBuffer.WriterIndex())
	p.workSequence.Store(-1)
	if d.defaultPanic != nil {
		dp := d.defaultPanic
		p.onPanic = func(recovered any, seq int64) { dp(recovered, seq, seq) }
	}
	for range size {
		w := &workProcessor[T]{pool: p, done: make(chan struct{})}
		w.readIndex.Store(-1)
		p.workers = append(p.workers, w)
	}
	return p
}

// RegisterWorkerPool enrolls one or more worker pools to be launched at Start.
func (d *Disruptor[T]) RegisterWorkerPool(pools ...*WorkerPool[T]) {
	d.pools = append(d.pools, pools...)
}

// Start launches every registered consumer goroutine and wires producer
// back-pressure. It is idempotent. Call before producing.
func (d *Disruptor[T]) Start() {
	if !d.started.CompareAndSwap(false, true) {
		return
	}
	// Back-pressure only needs to gate on sink consumers (those nobody depends
	// on). In an acyclic graph a sink's read cursor is always <= its ancestors',
	// so the minimum over sinks equals the minimum over all consumers — with
	// fewer sequences to scan on the hot path.
	for _, c := range d.consumers {
		if c.dependents == 0 {
			d.barrier.Register(&c.readIndex)
		}
	}
	// Worker pools are always sinks: every worker's read cursor gates the
	// producer so no slot is overwritten while any worker is still on it.
	for _, p := range d.pools {
		for _, seq := range p.gatingSequences() {
			d.barrier.Register(seq)
		}
	}
	for _, c := range d.consumers {
		go c.run()
	}
	for _, p := range d.pools {
		p.start()
	}
	if d.metricsSink != nil && d.metricsInterval > 0 {
		d.samplerStop = make(chan struct{})
		d.samplerDone = make(chan struct{})
		go d.runSampler()
	}
}

// Stop drains all in-flight events and shuts the consumers down. The caller MUST
// have stopped all producers first; Stop does not interrupt a producer blocked
// in Next.
//
// Shutdown is drain-then-alert: it first waits for every consumer to catch up to
// the last published sequence (so nothing already published is dropped), and
// only then alerts the barriers so the consumer goroutines exit. This ordering
// matters for DAGs — alerting first could let a downstream stage exit before its
// upstream finished draining. It is idempotent. Requires an acyclic consumer
// graph.
func (d *Disruptor[T]) Stop() {
	// Background never cancels, so this drains fully then alerts — the original
	// unbounded shutdown. StopContext is the bounded variant.
	_ = d.StopContext(context.Background())
}

// CeilPowerOf2 rounds n up to the nearest power of two (minimum 1).
func CeilPowerOf2(n int64) int64 {
	if n < 1 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	return int64(1) << bits.Len64(uint64(n))
}
