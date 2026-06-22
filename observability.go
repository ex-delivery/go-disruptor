package disruptor

import "time"

// Stats is a point-in-time snapshot of disruptor health. It is cheap to take —
// it only reads atomic cursors, adding nothing to the publish/consume hot paths
// — and safe to call concurrently. Intended for periodic scraping by a metrics
// collector (Prometheus, expvar, logs, ...).
type Stats struct {
	Published     int64   // total events published so far (writer cursor + 1)
	Capacity      int64   // ring size
	Free          int64   // currently free slots; 0 means the producer is back-pressured
	Backpressure  int64   // cumulative producer claims that had to wait for room
	ConsumerLag   []int64 // per registered consumer: published events not yet processed
	WorkerPoolLag []int64 // per registered worker pool: events not yet finished
}

// Stats returns a snapshot of the disruptor's health. Call it from a metrics
// goroutine on whatever interval suits you; it costs only a handful of atomic
// loads and never blocks the producers or consumers.
func (d *Disruptor[T]) Stats() Stats {
	s := Stats{
		Published:    d.RingBuffer.WriterIndex().Load() + 1,
		Capacity:     d.Capacity(),
		Free:         d.RemainingCapacity(),
		Backpressure: d.barrier.stallCount(),
	}
	for _, c := range d.consumers {
		s.ConsumerLag = append(s.ConsumerLag, c.Lag())
	}
	for _, p := range d.pools {
		s.WorkerPoolLag = append(s.WorkerPoolLag, p.Lag())
	}
	return s
}

// runSampler is the goroutine launched by WithMetrics: it pushes a Stats
// snapshot to the sink every interval, plus one final snapshot when stopped.
func (d *Disruptor[T]) runSampler() {
	defer close(d.samplerDone)
	t := time.NewTicker(d.metricsInterval)
	defer t.Stop()
	for {
		select {
		case <-d.samplerStop:
			d.metricsSink(d.Stats()) // final snapshot at shutdown
			return
		case <-t.C:
			d.metricsSink(d.Stats())
		}
	}
}
