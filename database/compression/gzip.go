package compression

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/gzip"
)

type gzipCompressor struct {
}

func NewGzipCompressor() Compressor {
	return &gzipCompressor{}
}

func (gzipCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (gzipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()
	return io.ReadAll(r)
}
