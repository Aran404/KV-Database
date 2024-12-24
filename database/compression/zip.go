package compression

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zip"
)

type zipCompressor struct {
}

func NewZipCompressor() Compressor {
	return &zipCompressor{}
}

func (zipCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	f, err := w.Create("file")
	if err != nil {
		return nil, err
	}

	if _, err := f.Write(data); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (zipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	if len(r.File) == 0 {
		return nil, io.ErrUnexpectedEOF
	}

	f, err := r.File[0].Open()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	return io.ReadAll(f)
}
