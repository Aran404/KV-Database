package compression

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zlib"
)

type zlibCompressor struct {
}

func NewZlibCompressor() Compressor {
	return &zlibCompressor{}
}

func (zlibCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, zlib.BestCompression)
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

func (zlibCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
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
