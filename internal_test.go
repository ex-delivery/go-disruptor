package disruptor

import "testing"

// White-box tests for unexported helpers that the black-box suite in ./test
// cannot reach. Keep these in package disruptor.

// TestClampCapacity covers the free-slot clamp directly: the figure must be
// bounded to [0, capacity], including the no-gating-consumer case where the raw
// value overflows high and must report the ring as fully free.
func TestClampCapacity(t *testing.T) {
	cases := []struct {
		remaining, capacity, want int64
	}{
		{5, 8, 5},
		{0, 8, 0},
		{8, 8, 8},
		{-3, 8, 0},  // never negative
		{100, 8, 8}, // never above capacity (e.g. no consumers gating)
		{-1, 1, 0},
	}
	for _, tc := range cases {
		if got := clampCapacity(tc.remaining, tc.capacity); got != tc.want {
			t.Errorf("clampCapacity(%d, %d) = %d, want %d", tc.remaining, tc.capacity, got, tc.want)
		}
	}
}

// TestCeilPowerOf2 checks rounding up to the next power of two, minimum 1.
func TestCeilPowerOf2(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{-5, 1},
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{8, 8},
		{1000, 1024},
		{1 << 20, 1 << 20},
	}
	for _, tc := range cases {
		if got := CeilPowerOf2(tc.in); got != tc.want {
			t.Errorf("CeilPowerOf2(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
