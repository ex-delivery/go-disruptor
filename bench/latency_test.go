package bench

import (
	"slices"
	"testing"
	"time"

	"github.com/ex-delivery/go-disruptor"
)

// stamped carries the publish timestamp so the consumer can measure end-to-end
// latency (publish -> consume).
type stamped struct{ pub int64 }

// reportPercentiles sorts the collected latencies and reports p50/p99/p99.9 (ns)
// as custom benchmark metrics.
func reportPercentiles(b *testing.B, lat []int64) {
	if len(lat) == 0 {
		return
	}
	slices.Sort(lat)
	at := func(p float64) float64 {
		idx := int(float64(len(lat)) * p / 100)
		if idx >= len(lat) {
			idx = len(lat) - 1
		}
		return float64(lat[idx])
	}
	b.ReportMetric(at(50), "p50-ns")
	b.ReportMetric(at(99), "p99-ns")
	b.ReportMetric(at(99.9), "p99.9-ns")
}

// BenchmarkLatencySPSC measures end-to-end latency through the disruptor and
// reports the distribution (p50/p99/p99.9) rather than just throughput — the
// metric that matters for an ultra-low-latency pipeline.
//
// Note: one-way latencies here are often below time.Now()'s effective resolution
// (and the ~tens-of-ns cost of the call itself), so p50 commonly reports 0 —
// read that as "delivered faster than the clock can resolve", not zero latency.
// The tail (p99/p99.9) still captures scheduling and GC jitter, and
// BenchmarkChannelLatencySPSC gives a same-method baseline to compare against.
func BenchmarkLatencySPSC(b *testing.B) {
	d := disruptor.NewDisruptor(1024, func() stamped { return stamped{} })

	lat := make([]int64, 0, 1<<16)
	c := d.Consumer(func(buf []stamped, mask, lo, hi int64) {
		for s := lo; s <= hi; s++ {
			lat = append(lat, time.Now().UnixNano()-buf[s&mask].pub) // per-event, matching the channel bench
		}
	})
	d.RegisterConsumer(c)
	d.Start()

	for b.Loop() {
		seq := d.Next(1)
		d.Get(seq).pub = time.Now().UnixNano()
		d.Publish(seq, seq)
	}
	d.Stop()

	reportPercentiles(b, lat)
}

// BenchmarkChannelLatencySPSC is the buffered-channel equivalent, for a like-for-
// like latency comparison against BenchmarkLatencySPSC.
func BenchmarkChannelLatencySPSC(b *testing.B) {
	ch := make(chan stamped, 1024)
	lat := make([]int64, 0, 1<<16)
	done := make(chan struct{})
	go func() {
		for e := range ch {
			lat = append(lat, time.Now().UnixNano()-e.pub)
		}
		close(done)
	}()

	for b.Loop() {
		ch <- stamped{pub: time.Now().UnixNano()}
	}
	close(ch)
	<-done

	reportPercentiles(b, lat)
}

// BenchmarkChannelSPSC is the buffered-channel throughput equivalent of
// BenchmarkSPSC, to contrast raw send/receive cost.
func BenchmarkChannelSPSC(b *testing.B) {
	ch := make(chan Event, 1024)
	var sink int64
	done := make(chan struct{})
	go func() {
		for e := range ch {
			sink += e.Value
		}
		close(done)
	}()

	b.ReportAllocs()
	for b.Loop() {
		ch <- Event{Value: 1}
	}
	close(ch)
	<-done
	_ = sink
}
