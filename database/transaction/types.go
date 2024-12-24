package transactions

import (
	"sync"

	"github.com/Aran404/KV-Database/database/btree"
)

type Transaction struct {
	tree       *btree.BTree
	operations []func()
	rollback   []func()
	m          *sync.Mutex
	cancelled  bool
}
