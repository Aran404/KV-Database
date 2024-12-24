package btree

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTree(t *testing.T) {
	t1 := NewTree(nil)
	require.NotNil(t, t1, "Expected a valid BTree instance")

	t2 := NewTree(nil, 5)
	require.NotNil(t, t2, "Expected a valid BTree instance with custom degree")
	require.NotNil(t, t2.Tree, "Expected internal tree to be initialized")
}

func TestBTree_Size(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key2"), []byte("value2"))

	assert.Equal(t, int64(2), tree.Size(), "Expected size 2")
}

func TestBTree_Close(t *testing.T) {
	tree := NewTree(nil)
	tree.Close()

	assert.Nil(t, tree.Tree, "Tree should be nil after Close")
}

func TestBTree_Get(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key"), []byte("value"))

	kv := tree.Get([]byte("key"))
	require.NotNil(t, kv, "Expected to find key 'key'")
	assert.Equal(t, []byte("value"), kv.Value, "Expected value 'value'")

	// Test key not found
	assert.Nil(t, tree.Get([]byte("nonexistent")), "Expected nil for nonexistent key")
}

func TestBTree_GetString(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.SetString("key", []byte("value"))

	kv := tree.GetString("key")
	require.NotNil(t, kv, "Expected to find key 'key'")
	assert.Equal(t, []byte("value"), kv.Value, "Expected value 'value'")

	// Test key not found
	assert.Nil(t, tree.GetString("nonexistent"), "Expected nil for nonexistent key")
}

func TestBTree_Set(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	// Test new key insertion
	kv := tree.Set([]byte("key"), []byte("value"))
	assert.Nil(t, kv, "Expected nil for new key insertion")

	// Overwriting the same key
	kv = tree.Set([]byte("key"), []byte("new_value"))
	require.NotNil(t, kv, "Expected previous value for overwritten key")
	assert.Equal(t, []byte("value"), kv.Value, "Expected previous value 'value'")
}

func TestBTree_SetString(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	// Test new key insertion
	kv := tree.SetString("key", []byte("value"))
	assert.Nil(t, kv, "Expected nil for new key insertion")

	// Overwriting the same key
	kv = tree.SetString("key", []byte("new_value"))
	require.NotNil(t, kv, "Expected previous value for overwritten key")
	assert.Equal(t, []byte("value"), kv.Value, "Expected previous value 'value'")
}

func TestBTree_Delete(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key"), []byte("value"))

	kv := tree.Delete([]byte("key"))
	require.NotNil(t, kv, "Expected to delete key 'key'")
	assert.Equal(t, []byte("value"), kv.Value, "Expected deleted value 'value'")

	// Deleting a non-existent key
	assert.Nil(t, tree.Delete([]byte("nonexistent")), "Expected nil for nonexistent key")
}

func TestBTree_DeleteString(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.SetString("key", []byte("value"))

	kv := tree.DeleteString("key")
	require.NotNil(t, kv, "Expected to delete key 'key'")
	assert.Equal(t, []byte("value"), kv.Value, "Expected deleted value 'value'")

	// Deleting a non-existent key
	assert.Nil(t, tree.DeleteString("nonexistent"), "Expected nil for nonexistent key")
}

func TestBTree_List(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key2"), []byte("value2"))

	pairs := tree.List(nil)
	require.Len(t, pairs, 2, "Expected 2 pairs")
	assert.Contains(t, [][]byte{pairs[0].Key, pairs[1].Key}, []byte("key1"), "Expected key 'key1'")
	assert.Contains(t, [][]byte{pairs[0].Key, pairs[1].Key}, []byte("key2"), "Expected key 'key2'")
}

func TestBTree_Range(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	// Insert test data
	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key3"), []byte("value3"))

	// Get range
	ctx := context.Background()
	results := tree.Range(ctx, []byte("key1"), []byte("key3"))
	assert.Len(t, results, 2, "Expected 2 pairs in range")
}

func TestBTree_Has(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key1"), []byte("value1"))

	assert.True(t, tree.Has([]byte("key1")), "Expected key 'key1' to be present")
	assert.False(t, tree.Has([]byte("nonexistent")), "Expected key 'nonexistent' to not be present")
}

func TestBTree_HasString(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.SetString("key1", []byte("value1"))

	assert.True(t, tree.HasString("key1"), "Expected key 'key1' to be present")
	assert.False(t, tree.HasString("nonexistent"), "Expected key 'nonexistent' to not be present")
}

func TestBTree_CancelContext(t *testing.T) {
	tree := NewTree(nil)
	defer tree.Close()

	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key3"), []byte("value3"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := tree.List(ctx)
	assert.Len(t, results, 0, "Expected 0 pairs after context cancel")
}
