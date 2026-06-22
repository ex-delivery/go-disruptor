package disruptor

import (
	"runtime"
	"time"
)

// WaitStrategy decides what a spinning waiter (consumer waiting for data, or
// producer waiting for room) does on each failed poll. Idle is called once per
// failed poll with spin = number of consecutive failed polls so far, letting a
// strategy escalate from busy-spin to yielding to sleeping.
//
// Trade-off: busier strategies cut latency but burn CPU. Idle must never block
// indefinitely — the caller re-checks its condition (and the alert flag) after
// every Idle call.
type WaitStrategy interface {
	Idle(spin int)
}

// BusySpinWait never yields: lowest latency, pins a full core. Use only when a
// core is reserved for the waiter.
type BusySpinWait struct{}

// Idle does nothing — the caller hot-loops.
func (BusySpinWait) Idle(int) {}

// YieldingWait busy-spins a while, then yields the P to the Go scheduler. This
// is the default: near-busy-spin latency without permanently pinning a core.
type YieldingWait struct {
	// Threshold is the number of spins before yielding; <= 0 means 100.
	Threshold int
}

// Idle yields once the spin count crosses the threshold.
func (y YieldingWait) Idle(spin int) {
	t := y.Threshold
	if t <= 0 {
		t = 100
	}
	if spin >= t {
		runtime.Gosched()
	}
}

// SleepingWait escalates busy-spin -> Gosched -> short sleep, trading latency
// for CPU. Good for low-frequency streams where a hot core is wasteful.
type SleepingWait struct {
	SpinThreshold  int           // busy-spin below this (default 100)
	YieldThreshold int           // Gosched below this, then sleep (default 200)
	SleepFor       time.Duration // sleep once escalated (default 1µs)
}

// Idle busy-spins, then yields, then sleeps as the spin count grows.
func (s SleepingWait) Idle(spin int) {
	spinT := s.SpinThreshold
	if spinT <= 0 {
		spinT = 100
	}
	yieldT := s.YieldThreshold
	if yieldT <= 0 {
		yieldT = 200
	}
	switch {
	case spin < spinT:
		// busy spin
	case spin < yieldT:
		runtime.Gosched()
	default:
		d := s.SleepFor
		if d <= 0 {
			d = time.Microsecond
		}
		time.Sleep(d)
	}
}
