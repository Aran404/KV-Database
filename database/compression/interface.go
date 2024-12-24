package compression

type Compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

type CompressorDesciptor int32

const (
	FLATE_COMPRESSION CompressorDesciptor = iota
	GZIP_COMPRESSION
	ZIP_COMPRESSION
	ZLIB_COMPRESSION
)

var (
	_ Compressor = (*flateCompressor)(nil)
	_ Compressor = (*gzipCompressor)(nil)
	_ Compressor = (*zipCompressor)(nil)
	_ Compressor = (*zlibCompressor)(nil)

	DefaultCompressor = NewGzipCompressor()
	Compressors       = map[string]Compressor{
		"flate": NewFlateCompressor(),
		"gzip":  NewGzipCompressor(),
		"zip":   NewZipCompressor(),
		"zlib":  NewZlibCompressor(),
	}

	CompressorInts = map[CompressorDesciptor]Compressor{
		FLATE_COMPRESSION: NewFlateCompressor(),
		GZIP_COMPRESSION:  NewGzipCompressor(),
		ZIP_COMPRESSION:   NewZipCompressor(),
		ZLIB_COMPRESSION:  NewZlibCompressor(),
	}
)
