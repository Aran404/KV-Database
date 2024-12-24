package compression

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/flate"
)

type flateCompressor struct {
}

func NewFlateCompressor() Compressor {
	return &flateCompressor{}
}

func (flateCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (flateCompressor) Decompress(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	return io.ReadAll(r)
}
