package client

import (
	"github.com/Aran404/KV-Database/database/btree"
	"github.com/Aran404/KV-Database/database/compression"
	"github.com/Aran404/KV-Database/database/wal"
)

type Option func(d *Database)

func NewDatabase(opts ...Option) *Database {
	tree := btree.NewTree(nil)
	db := &Database{Tree: tree, WAL: wal.NewWAL(nil)}

	if len(opts) == 0 {
		opts = WithDefaultOptions()
	}
	for _, opt := range opts {
		opt(db)
	}
	return db
}

// WithDefaultOptions returns the default options.
func WithDefaultOptions() []Option {
	return []Option{
		WithWALCompressor(compression.DefaultCompressor),
		WithBTreeCompressor(nil),
		WithUseChecksum(true),
	}
}

// WithCustomDegree sets the tree degree.
func WithCustomDegree(degree int) Option {
	if degree <= 1 {
		panic("KV-Database: degree must be greater than 1")
	}
	return func(d *Database) {
		d.Tree.Tree.Degree = degree
	}
}

// WithUseChecksum sets the WAL to use checksum.
func WithUseChecksum(useChecksum bool) Option {
	return func(d *Database) {
		d.WAL.UseChecksum = useChecksum
	}
}

// WithWALCompressor sets the WAL compressor.
func WithWALCompressor(compressor compression.Compressor) Option {
	if compressor == nil {
		return func(d *Database) {
			d.WAL.Compressor = nil
		}
	}
	return func(d *Database) {
		d.WAL.Compressor = compressor
	}
}

// WithBTreeCompressor sets the BTree compressor.
func WithBTreeCompressor(compressor compression.Compressor) Option {
	if compressor == nil {
		return func(d *Database) {
			d.Tree.Compressor = nil
		}
	}
	return func(d *Database) {
		d.Tree.Compressor = compressor
	}
}

// NewCursor returns a new cursor.
func (db *Database) NewCursor() *Cursor {
	return &Cursor{Cursor: btree.NewCursor(db.Tree)}
}
