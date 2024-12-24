package transactions

import (
	"context"
	"testing"
	"time"

	"github.com/Aran404/KV-Database/database/btree"
	"github.com/stretchr/testify/assert"
)

// TestTransaction_Commit verifies that committing a transaction applies all operations.
func TestTransaction_Commit(t *testing.T) {
	tree := btree.NewTree(nil)
	tx := NewTransaction(tree)

	// Perform transactional operations
	tx.Set([]byte("key1"), []byte("value1"))
	tx.Set([]byte("key2"), []byte("value2"))
	tx.Delete([]byte("nonexistent"))

	// Commit the transaction
	tx.Commit(context.Background())

	// Verify that operations were applied
	assert.Equal(t, "value1", string(tree.Get([]byte("key1")).Value))
	assert.Equal(t, "value2", string(tree.Get([]byte("key2")).Value))
	assert.Nil(t, tree.Get([]byte("nonexistent")))
}

// TestTransaction_Rollback verifies that rolling back a transaction reverts all operations.
func TestTransaction_Rollback(t *testing.T) {
	tree := btree.NewTree(nil)
	tree.Set([]byte("key1"), []byte("original"))
	tree.Set([]byte("key2"), []byte("original"))

	tx := NewTransaction(tree)

	// Perform transactional operations
	tx.Set([]byte("key1"), []byte("new_value"))
	tx.Delete([]byte("key2"))

	// Rollback the transaction
	tx.Rollback(context.Background())

	// Verify that changes were reverted
	assert.Equal(t, "original", string(tree.Get([]byte("key1")).Value))
	assert.Equal(t, "original", string(tree.Get([]byte("key2")).Value))
}

// TestTransaction_ContextCancel verifies that a transaction respects context cancellation.
func TestTransaction_ContextCancel(t *testing.T) {
	tree := btree.NewTree(nil)
	tx := NewTransaction(tree)

	// Perform transactional operations
	tx.Set([]byte("key1"), []byte("value1"))
	tx.Set([]byte("key2"), []byte("value2"))

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediately cancel the context

	// Commit should exit early due to context cancellation
	tx.Commit(ctx)

	// Verify that no changes were made
	assert.Nil(t, tree.Get([]byte("key1")))
	assert.Nil(t, tree.Get([]byte("key2")))
}

// TestTransaction_ConcurrentAccess verifies thread-safety of transaction operations.
func TestTransaction_ConcurrentAccess(t *testing.T) {
	tree := btree.NewTree(nil)
	tx := NewTransaction(tree)

	// Simulate concurrent access
	go tx.Set([]byte("key1"), []byte("value1"))
	go tx.Set([]byte("key2"), []byte("value2"))
	go tx.Delete([]byte("key1"))

	time.Sleep(100 * time.Millisecond) // Allow goroutines to run

	// Commit the transaction
	tx.Commit(context.Background())

	// Verify final state (may vary due to goroutines)
	val := tree.Get([]byte("key1"))
	if val != nil {
		assert.Equal(t, "value1", string(val.Value))
	}
	assert.Equal(t, "value2", string(tree.Get([]byte("key2")).Value))
}

// TestTransaction_SetAndDelete verifies setting and deleting in a single transaction.
func TestTransaction_SetAndDelete(t *testing.T) {
	tree := btree.NewTree(nil)
	tree.Set([]byte("key1"), []byte("original"))

	tx := NewTransaction(tree)

	// Perform operations
	tx.Set([]byte("key1"), []byte("new_value"))
	tx.Delete([]byte("key1"))

	// Commit and verify
	tx.Commit(context.Background())
	assert.Nil(t, tree.Get([]byte("key1")))
}

// TestTransaction_RepeatedCommitOrRollback verifies that repeated commit is disallowed.
func TestTransaction_RepeatedCommitOrRollback(t *testing.T) {
	tree := btree.NewTree(nil)
	tx := NewTransaction(tree)

	tx.Set([]byte("key1"), []byte("value1"))
	tx.Commit(context.Background())

	// Commit again should panic
	assert.Panics(t, func() {
		tx.Commit(context.Background())
	})
}
