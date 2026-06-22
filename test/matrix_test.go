package test

import (
	"testing"

	"github.com/ex-delivery/go-disruptor"
)

// TestBatchPublish exercises multi-slot claims: Next(n>1) reserves a contiguous
// range filled and published in one Publish(lo, hi).
func TestBatchPublish(t *testing.T) {
	const N = 1 << 12
	const batch = 7
	d := disruptor.NewDisruptor(1024, newEvent)

	var sum int64
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		sum += e.Value
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()

	var want, value int64
	for published := int64(0); published < N; {
		n := min(int64(batch), N-published)
		hi := d.Next(n) // claim n contiguous slots; hi is the upper sequence
		lo := hi - n + 1
		for s := lo; s <= hi; s++ {
			value++
			d.Get(s).Value = value
			want += value
		}
		d.Publish(lo, hi)
		published += n
	}
	d.Stop()

	if sum != want {
		t.Fatalf("sum=%d want=%d (batch publish lost or corrupted events)", sum, want)
	}
}

// TestWaitStrategies runs an SPSC round under each wait strategy.
func TestWaitStrategies(t *testing.T) {
	strategies := map[string]disruptor.WaitStrategy{
		"busyspin": disruptor.BusySpinWait{},
		"yielding": disruptor.YieldingWait{},
		"sleeping": disruptor.SleepingWait{},
	}
	for name, ws := range strategies {
		t.Run(name, func(t *testing.T) {
			const N = 1 << 12
			d := disruptor.NewDisruptor(256, newEvent, disruptor.WithWaitStrategy(ws))
			var sum int64
			c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
				sum += e.Value
				return nil
			}))
			d.RegisterConsumer(c)
			d.Start()

			var want int64
			for i := int64(1); i <= N; i++ {
				seq := d.Next(1)
				d.Get(seq).Value = i
				d.Publish(seq, seq)
				want += i
			}
			d.Stop()
			if sum != want {
				t.Fatalf("%s: sum=%d want=%d", name, sum, want)
			}
		})
	}
}

// TestCapacityRounding checks NewDisruptor rounds capacity up to a power of two.
func TestCapacityRounding(t *testing.T) {
	cases := map[int64]int64{1: 1, 2: 2, 3: 4, 100: 128, 1000: 1024, 1024: 1024}
	for in, want := range cases {
		d := disruptor.NewDisruptor(in, newEvent)
		if got := d.Capacity(); got != want {
			t.Errorf("NewDisruptor(%d).Capacity()=%d want %d", in, got, want)
		}
	}
}

// TestClaimCountGuard checks a non-positive claim count is treated as one slot.
func TestClaimCountGuard(t *testing.T) {
	d := disruptor.NewDisruptor(64, newEvent)
	c := d.Consumer(disruptor.EventHandlerFunc[Event](func(e *Event, seq int64, eob bool) error {
		return nil
	}))
	d.RegisterConsumer(c)
	d.Start()
	defer d.Stop()

	a := d.Next(0)
	d.Publish(a, a)
	b := d.Next(-5)
	d.Publish(b, b)
	if b != a+1 {
		t.Fatalf("clamped claims should be contiguous single slots: a=%d b=%d", a, b)
	}
	seq, ok := d.TryNext(0)
	if !ok || seq != b+1 {
		t.Fatalf("TryNext(0)=%d,%v want %d,true", seq, ok, b+1)
	}
	d.Publish(seq, seq)
}
