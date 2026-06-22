//go:build !arm64 && !amd64

package disruptor

// cacheLinePad is sized to one cache line (64 bytes) on architectures other than
// amd64/arm64. On platforms with larger effective lines (e.g. s390x) bump this
// if false sharing shows up in profiles.
type cacheLinePad struct{ _ [64]byte }
