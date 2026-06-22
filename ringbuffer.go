package disruptor

// RingBuffer is a fixed-capacity, power-of-two ring of pre-allocated T values.
// Slots are reused as sequences advance; capacity being a power of two lets the
// index be computed with seq&mask instead of a modulo.
type RingBuffer[T any] struct {
	_        cacheLinePad
	mask     int64
	storage  []T
	writeIdx Sequence
	_        cacheLinePad
}

// NewRingBuffer allocates a ring of the given capacity (which MUST be a power of
// two) and fills every slot using factory, so the hot path never allocates.
func NewRingBuffer[T any](capacity int64, factory func() T) *RingBuffer[T] {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		panic("disruptor: capacity must be a power of two")
	}
	storage := make([]T, capacity)
	for i := range capacity {
		storage[i] = factory()
	}
	rb := &RingBuffer[T]{
		mask:    capacity - 1,
		storage: storage,
	}
	rb.writeIdx.Store(-1)
	return rb
}

// WriterIndex returns the ring's published-cursor sequence. Consumers gate on
// it to learn what's safe to read.
func (rb *RingBuffer[T]) WriterIndex() *Sequence { return &rb.writeIdx }

// Get returns a pointer to the slot for seq so the caller can read or mutate it
// in place. The caller is responsible for only touching slots it owns (between
// Next and Publish for producers, within a handler batch for consumers).
func (rb *RingBuffer[T]) Get(seq int64) *T { return &rb.storage[seq&rb.mask] }

// Capacity returns the number of slots in the ring.
func (rb *RingBuffer[T]) Capacity() int64 { return rb.mask + 1 }
