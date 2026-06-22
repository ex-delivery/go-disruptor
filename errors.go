package disruptor

import "errors"

// ErrClosed is returned by NextContext when the disruptor is shutting down (the
// producer back-pressure barrier was alerted) while the producer was still
// waiting for free space. It signals that the claim was abandoned and nothing
// was published.
var ErrClosed = errors.New("disruptor: closed")
