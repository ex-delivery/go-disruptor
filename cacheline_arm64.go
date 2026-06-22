//go:build arm64

package disruptor

// cacheLinePad is sized to one cache line. On arm64 (incl. Apple Silicon) the
// effective line is 128 bytes, so over-padding to 128 prevents false sharing.
type cacheLinePad struct{ _ [128]byte }
