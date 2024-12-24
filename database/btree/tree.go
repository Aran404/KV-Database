package btree

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Aran404/KV-Database/database/compression"
	btree "github.com/Aran404/KV-Database/database/gbtree"
)

// NewTree creates a new B-Tree.
// Compressor usually isn't needed for in-memory stores but can be useful if you plan on running a server with little RAM.
func NewTree(compressor compression.Compressor, degree ...uint32) (t *BTree) {
	t = &BTree{
		Compressor: compressor,
		M:          &sync.RWMutex{},
		sizeCache:  &atomic.Int64{},
	}
	if len(degree) > 0 {
		t.Tree = btree.New(int(degree[0]))
	} else {
		t.Tree = btree.New(DEFAULT_DEG_SIZE)
	}
	return
}

// Size returns the number of items currently in the tree.
func (t *BTree) Size() int64 {
	t.M.RLock()
	defer t.M.RUnlock()

	if t.sizeCache.Load() != -1 {
		return t.sizeCache.Load()
	}
	n := int64(t.Tree.Len())
	t.sizeCache.Store(n)
	return n
}

// Close deallocates the tree and node memory.
func (t *BTree) Close() {
	t.M.Lock()
	defer t.M.Unlock()

	if t.Tree != nil {
		t.Tree.Clear(true)
		t.Tree = nil
	}

	t.sizeCache = nil
	t.M = nil
	t.Compressor = nil

	t.WrittenMemory = 0
	t.ReadMemory = 0
	t.DeletedMemory = 0
}

// handleCompression decompresses the value if it is compressed.
// If the value is not compressed, this is a no-op.
func (t *BTree) handleCompression(v *KVDump) {
	// In the case that the decompression fails, the user gets delegated the responsibility.
	if v.IsCompressed && t.Compressor != nil {
		decompressed, err := t.Compressor.Decompress(v.Value)
		if err == nil {
			v.Value = decompressed
		}
	}
}

// Get returns the value for the given key. Returns nil if not found.
func (t *BTree) Get(key []byte) *KVDump {
	t.M.RLock()
	defer t.M.RUnlock()

	if _, ok := t.ToDelete[string(key)]; ok {
		return nil
	}

	if t.Tree == nil {
		panic("KV-Database: Get called after tree was closed")
	}

	n := NodePool.Get().(*KVDump)
	defer NodePool.Put(n)
	n.Key = key

	raw := t.Tree.Get(n)
	atomic.AddUint64((*uint64)(&t.ReadMemory), uint64(len(n.Key)+len(n.Value)))

	if v, ok := raw.(*KVDump); ok {
		// TTL Expired
		if v.ExpiresAt != 0 && v.ExpiresAt < uint64(time.Now().UnixMilli()) {
			t.M.Lock()
			t.ToDelete[string(v.Key)] = struct{}{} // Map writes are not atomic
			t.M.Unlock()
			// Eviction happens in the background
			return nil
		}
		t.handleCompression(v)
		return v
	}
	return nil
}

// GetString returns the value for the given key. Returns nil if not found.
func (t *BTree) GetString(key string) *KVDump {
	return t.Get([]byte(key))
}

// Set sets the value for the given key.
// Returns the previous value if the key exists, and nil if the key was newly inserted.
func (t *BTree) Set(key, value []byte, ttl ...time.Duration) *KVDump {
	t.M.Lock()
	defer t.M.Unlock()

	if t.Tree == nil {
		panic("KV-Database: Set called after tree was closed")
	}

	n := NewKVDump(key, value, ttl...)
	if t.Compressor != nil {
		compressed, err := t.Compressor.Compress(value)
		// Fallback to uncompressed
		if err == nil {
			n.IsCompressed = true
			n.Value = compressed
		}
	}

	t.WrittenMemory += Memory(len(n.Key) + len(n.Value))
	t.sizeCache.Add(1)

	if item := t.Tree.ReplaceOrInsert(n); item != nil {
		if v, ok := item.(*KVDump); ok {
			return v
		}
	}
	delete(t.ToDelete, string(key))
	return nil
}

// SetString sets the value for the given key. Returns nil if fails.
func (t *BTree) SetString(key string, value []byte, ttl ...time.Duration) *KVDump {
	return t.Set([]byte(key), value, ttl...)
}

// Delete deletes the value for the given key. Returns nil if not found.
func (t *BTree) Delete(key []byte) *KVDump {
	t.M.Lock()
	defer t.M.Unlock()

	if t.Tree == nil {
		panic("KV-Database: Set called after tree was closed")
	}
	n := NodePool.Get().(*KVDump)
	defer NodePool.Put(n)
	n.Key = key

	t.DeletedMemory += Memory(len(n.Key) + len(n.Value))
	t.sizeCache.Add(-1)

	raw := t.Tree.Delete(n)
	if v, ok := raw.(*KVDump); ok {
		t.handleCompression(v)
		return v
	}

	delete(t.ToDelete, string(key))
	return nil
}

// DeleteString deletes the value for the given key. Returns nil if fails.
func (t *BTree) DeleteString(key string) *KVDump {
	return t.Delete([]byte(key))
}

// List returns a list of all key-value pairs in the tree.
func (t *BTree) List(ctx context.Context) []*KVDump {
	if ctx == nil {
		panic("KV-Database: List called with nil context")
	}

	t.M.RLock()
	defer t.M.RUnlock()

	if t.Tree == nil {
		panic("KV-Database: List called after tree was closed")
	}
	// Using t.Size() causes a deadlock here
	pairs := make([]*KVDump, 0, t.Tree.Len())
	t.Tree.Ascend(func(it btree.Item) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			v, ok := it.(*KVDump)
			if !ok {
				return true
			}

			t.handleCompression(v)
			pairs = append(pairs, v)
			atomic.AddUint64((*uint64)(&t.ReadMemory), uint64(len(v.Key)+len(v.Value)))
			return true
		}
	})
	return pairs
}

// Range returns a list of key-value pairs within a specified range.
func (t *BTree) Range(ctx context.Context, start, end []byte) []*KVDump {
	if ctx == nil {
		panic("KV-Database: Range called with nil context")
	}

	t.M.RLock()
	defer t.M.RUnlock()

	if t.Tree == nil {
		panic("KV-Database: Range called after tree was closed")
	}

	startKey := NodePool.Get().(*KVDump)
	defer NodePool.Put(startKey)
	endKey := NodePool.Get().(*KVDump)
	defer NodePool.Put(endKey)

	startKey.Key = start
	endKey.Key = end

	pairs := make([]*KVDump, 0)
	t.Tree.AscendRange(startKey, endKey, func(it btree.Item) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			v, ok := it.(*KVDump)
			if !ok {
				return true
			}

			t.handleCompression(v)
			pairs = append(pairs, v)
			atomic.AddUint64((*uint64)(&t.ReadMemory), uint64(len(v.Key)+len(v.Value)))
			return true
		}
	})

	return pairs
}

// Has returns true if the key exists in the tree.
func (t *BTree) Has(key []byte) bool { return t.Get(key) != nil }

// HasString returns true if the key exists in the tree.
func (t *BTree) HasString(key string) bool { return t.Has([]byte(key)) }
