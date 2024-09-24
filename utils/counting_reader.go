// utils/counting_reader.go
package utils

import (
	"io"
	"sync/atomic"
)

type CountingReader struct {
	Reader    io.Reader
	bytesRead int64
}

func (cr *CountingReader) Read(p []byte) (int, error) {
	n, err := cr.Reader.Read(p)
	atomic.AddInt64(&cr.bytesRead, int64(n))
	return n, err
}

func (cr *CountingReader) BytesRead() int64 {
	return atomic.LoadInt64(&cr.bytesRead)
}
