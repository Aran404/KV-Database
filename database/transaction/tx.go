package transactions

import (
	"context"
	"sync"
	"time"

	"github.com/Aran404/KV-Database/database/btree"
)

func NewTransaction(tree *btree.BTree) *Transaction {
	if tree == nil {
		panic("KV-Database: NewTransaction called with nil tree")
	}

	return &Transaction{
		tree:       tree,
		m:          &sync.Mutex{},
		operations: make([]func(), 0),
		rollback:   make([]func(), 0),
	}
}

// Commit atomically applies all operations to the tree
func (tx *Transaction) Commit(ctx context.Context) {
	if ctx == nil {
		panic("KV-Database: Commit called with nil context")
	}

	tx.m.Lock()
	defer tx.m.Unlock()

	if tx.cancelled {
		panic("KV-Database: Transaction has already been cancelled")
	}

	if tx.operations == nil {
		panic("KV-Database: Transaction has already been committed; create a new transaction")
	}

	for _, op := range tx.operations {
		select {
		case <-ctx.Done():
			return
		default:
			op()
		}
	}

	tx.operations = nil
}

// Rollback atomically reverts all operations to the tree.
// Unlike traditional transactions, a transaction can be rolled back after it has been committed.
func (tx *Transaction) Rollback(ctx context.Context) {
	if ctx == nil {
		panic("KV-Database: Rollback called with nil context")
	}

	tx.m.Lock()
	defer tx.m.Unlock()

	if tx.operations != nil {
		// Rollback prior to committing
		tx.rollback = nil
		tx.operations = nil
		return
	}

	if tx.rollback == nil {
		panic("KV-Database: Transaction has already been rolled back")
	}

	// Rollback in reverse
	for i := len(tx.rollback) - 1; i >= 0; i-- {
		select {
		case <-ctx.Done():
			return
		default:
			tx.rollback[i]()
		}
	}

	tx.rollback = nil
}

// Set queues a set operation
func (tx *Transaction) Set(key, value []byte, ttl ...time.Duration) {
	tx.m.Lock()
	defer tx.m.Unlock()

	prev := tx.tree.Get(key)
	tx.rollback = append(tx.rollback, func() {
		if prev != nil {
			tx.tree.Set(prev.Key, prev.Value)
		} else {
			tx.tree.Delete(key)
		}
	})

	tx.operations = append(tx.operations, func() {
		tx.tree.Set(key, value, ttl...)
	})
}

// Delete queues a delete operation
func (tx *Transaction) Delete(key []byte) {
	tx.m.Lock()
	defer tx.m.Unlock()

	prev := tx.tree.Get(key)
	if prev == nil {
		return
	}

	tx.rollback = append(tx.rollback, func() {
		if prev != nil {
			tx.tree.Set(prev.Key, prev.Value)
		}
	})

	tx.operations = append(tx.operations, func() {
		tx.tree.Delete(key)
	})
}
