//go:build amd64

package disruptor

// cacheLinePad is sized to two cache lines (128 bytes) on amd64. A line is 64
// bytes, but Intel's adjacent-cache-line ("spatial") prefetcher pulls lines in
// pairs, so 128 bytes of padding is needed to reliably avoid false sharing —
// matching the LMAX Disruptor's choice and the arm64 padding here.
type cacheLinePad struct{ _ [128]byte }
