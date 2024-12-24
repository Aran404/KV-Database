package btree

import (
	"sync"
	"sync/atomic"

	"github.com/Aran404/KV-Database/database/compression"
	btree "github.com/Aran404/KV-Database/database/gbtree"
	gbtree "github.com/Aran404/KV-Database/database/gbtree"
)

const (
	DEFAULT_DEG_SIZE = 10
)

// BTree is a thread-safe B-Tree that is stored in memory.
type BTree struct {
	Tree *btree.BTree
	// The descriptor could handle this for us aswell
	Compressor compression.Compressor

	// sizeCache is used to cache the size of the tree
	// -1 is sentinel for no current cache
	sizeCache *atomic.Int64
	M         *sync.RWMutex

	ToDelete map[string]struct{}

	// Memory Logs
	WrittenMemory Memory
	ReadMemory    Memory
	DeletedMemory Memory
}

type Cursor struct {
	tree     *BTree
	node     *gbtree.Node[gbtree.Item]
	index    int
	stack    []*gbtree.Node[gbtree.Item]
	childIdx []int
	current  *gbtree.Item
}

type KVDump struct {
	Key          []byte // You may not need the key here, though for serialization it's useful
	Value        []byte
	ExpiresAt    uint64
	IsCompressed bool
}
