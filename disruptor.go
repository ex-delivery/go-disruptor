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

	barrier           *Barrier // producer back-pressure gate (sink consumers)
	wait              WaitStrategy
	consumers         []*Consumer[T]
	defaultExceptions ExceptionHandler[T] // applied to each Consumer; override via HandleExceptionsWith
	metricsInterval   time.Duration
	metricsSink       func(Stats)
	samplerStop       chan struct{}
	samplerDone       chan struct{}
	started           atomic.Bool
	stopped           atomic.Bool
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
		metricsInterval: options.metricsInterval,
		metricsSink:     options.metricsSink,
	}
}

// Consumer creates a consumer bound to this disruptor's ring, driven by handler.
// Configure it (Depends / HandleExceptionsWith / WithRewindStrategy /
// MaxBatchSize / Timeout) and then pass it to RegisterConsumer before Start.
func (d *Disruptor[T]) Consumer(handler EventHandler[T]) *Consumer[T] {
	c := NewConsumer(d.RingBuffer, handler)
	c.barrier.setStrategy(d.wait)
	c.exceptions = d.defaultExceptions // default; HandleExceptionsWith overrides
	return c
}

// HandleExceptionsWith sets the default ExceptionHandler applied to consumers
// created afterwards via Consumer; a consumer's own HandleExceptionsWith
// overrides it. Returns the disruptor for chaining; call before creating
// consumers.
func (d *Disruptor[T]) HandleExceptionsWith(h ExceptionHandler[T]) *Disruptor[T] {
	d.defaultExceptions = h
	return d
}

// RegisterConsumer enrolls one or more consumers to be launched at Start.
func (d *Disruptor[T]) RegisterConsumer(cs ...*Consumer[T]) {
	d.consumers = append(d.consumers, cs...)
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
	for _, c := range d.consumers {
		go c.run()
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
