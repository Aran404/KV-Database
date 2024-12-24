// Writer provides wrapper methods for writing to the database.
// A few notes on implementation:
// 	1. Writer is thread safe not by instance but by the underlying wrappers.
// 	2. There should only be one writer instance per database.
// 	3. CTX is required for controlling spacing between writes in the WAL and ensuring that writes are properly executed and persisted.

package client

import (
	"context"
	"fmt"
	"time"
)

// Should you write to WAL before checking?
// * You could probably write to WAL first thing then if this check fails, invalidate the log entry.

// Create creates a new key-value pair to the database
// If the key already exists in the database, it will abort and return an error
func (db *Database) Create(ctx context.Context, key, value []byte, ttl ...time.Duration) error {
	if ctx == nil {
		panic("KV-Database: Write called with nil context")
	}

	if len(ttl) > 0 && ttl[0] <= 0 {
		return ErrInvalidTTL
	}

	// Check if key is in memory first
	if db.Tree.Has(key) {
		return ErrKeyExists
	}

	// Check if key is in disk

	// Heavy IO operation
	// Writing TTL to WAL is redundant assuming it takes a significant amount of time to backup the WAL.
	if err := db.WAL.LogSet(key, value); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	if isContextCancelled(ctx) {
		return ctx.Err()
	}

	db.Tree.Set(key, value, ttl...)
	return nil
}

func (db *Database) Read(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *Database) Update(ctx context.Context, key, value []byte) ([]byte, error) {
	return nil, nil
}

func (db *Database) Delete(ctx context.Context, key []byte) error {
	return nil
}
