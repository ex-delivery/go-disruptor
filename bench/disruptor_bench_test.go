package bench

import (
	"sync/atomic"
	"testing"

	"github.com/ex-delivery/go-disruptor"
)

type Event struct{ Value int64 }

func newEvent() Event { return Event{} }

func BenchmarkSPSC(b *testing.B) {
	d := disruptor.NewDisruptor(1024, newEvent)
	var sink int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		sink += e.Value
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	b.ReportAllocs()
	for b.Loop() {
		seq := d.Next(1)
		d.Get(seq).Value = 1
		d.Publish(seq, seq)
	}
	d.Stop()
	_ = sink
}

func BenchmarkMPSC(b *testing.B) {
	d := disruptor.NewDisruptor(1024, newEvent, disruptor.WithProducerFunc(disruptor.NewMultiProducer))
	var sink int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		atomic.AddInt64(&sink, e.Value)
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			seq := d.Next(1)
			d.Get(seq).Value = 1
			d.Publish(seq, seq)
		}
	})
	b.StopTimer()
	d.Stop()
	_ = sink
}
