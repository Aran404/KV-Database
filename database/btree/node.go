package btree

import (
	"bytes"
	"sync"
	"time"

	btree "github.com/Aran404/KV-Database/database/gbtree"
)

var (
	// Reduces allocations
	NodePool = sync.Pool{
		New: func() interface{} {
			return &KVDump{}
		},
	}
)

func NewKVDump(key []byte, value []byte, ttl ...time.Duration) *KVDump {
	kv := new(KVDump)
	kv.Key = key
	kv.Value = value

	if len(ttl) > 0 {
		kv.ExpiresAt = uint64(time.Now().UnixMilli()) + uint64(ttl[0].Milliseconds())
	}

	return kv
}

func (kv KVDump) Less(than btree.Item) bool {
	return bytes.Compare(kv.Key, than.(*KVDump).Key) < 0 // panics on type mismatch
}
