package client

import (
	"context"
	"errors"

	"github.com/Aran404/KV-Database/database/btree"
	"github.com/Aran404/KV-Database/database/wal"
)

var (
	ErrKeyExists  = errors.New("key already exists")
	ErrInvalidTTL = errors.New("invalid TTL")
)

type Database struct {
	Tree *btree.BTree
	WAL  *wal.WAL
}

// Cursor is an alias of btree.Cursor, just provided in a STD package
type Cursor struct {
	*btree.Cursor
}

func isContextCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
