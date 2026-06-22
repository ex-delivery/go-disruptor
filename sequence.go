package disruptor

import "sync/atomic"

// Sequence is a monotonically increasing cursor ("where we've written to" /
// "where we've read to"). It embeds atomic.Int64 and is padded on both sides so
// that a hot Sequence never shares a cache line with neighbouring fields, which
// would otherwise cause false sharing under contention.
//
// A Sequence must never be copied by value (it embeds atomic.Int64); always
// pass *Sequence. `go vet` will flag accidental copies.
type Sequence struct {
	_ cacheLinePad
	atomic.Int64
	_ cacheLinePad
}
