package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/Aran404/KV-Database/database/btree"
)

var (
	DefaultDeleteExpiredTTLInterval = time.Hour
	DefaultCompactionInterval       = 24 * time.Hour
	DefaultBackupInterval           = 24 * time.Hour
	DefaultIndexRebuildInterval     = 7 * 24 * time.Hour
	DefaultSnapshotInterval         = 30 * time.Minute
	DefaultWALCleanupInterval       = 10 * time.Minute

	DefaultConfig *Config = &Config{
		DeleteExpiredTTLInterval: &DefaultDeleteExpiredTTLInterval,
		CompactionInterval:       &DefaultCompactionInterval,
		BackupInterval:           &DefaultBackupInterval,
		IndexRebuildInterval:     &DefaultIndexRebuildInterval,
		SnapshotInterval:         &DefaultSnapshotInterval,
		WALCleanupInterval:       &DefaultWALCleanupInterval,
	}

	ErrNilTree = errors.New("tree is nil")
)

type CRUD interface {
	Create(ctx context.Context, key, value []byte)
	Read(key []byte) ([]byte, error)
	Update(ctx context.Context, key, value []byte)
	Delete(ctx context.Context, key []byte)
}

type Scheduler interface {
	EvictTTL() error
	Compact() error
	Backup() error
	IndexRebuild() error
	Snapshot() error
	WALCleanup() error
}

type daemonScheduler struct {
	cfg  *Config
	tree *btree.BTree
	ctx  context.Context
}

type Config struct {
	// DeleteExpiredTTLInterval is the interval at which expired entries are removed from memory
	DeleteExpiredTTLInterval *time.Duration
	// CompactionInterval is the interval at which compactions are run
	CompactionInterval *time.Duration
	// BackupInterval is the interval at which backups are run
	BackupInterval *time.Duration
	// IndexRebuildInterval is the interval at which index rebuilds are run
	IndexRebuildInterval *time.Duration
	// SnapshotInterval is the interval at which snapshots are taken
	SnapshotInterval *time.Duration
	// WALCleanupInterval is the interval at which WAL files are cleaned up
	WALCleanupInterval *time.Duration
}

func NewScheduler(cfg *Config) Scheduler {
	if cfg == nil {
		cfg = DefaultConfig
	}
	return &daemonScheduler{cfg: cfg}
}

func (c *Config) InsertDefaults() {
	if c.DeleteExpiredTTLInterval == nil {
		c.DeleteExpiredTTLInterval = &DefaultDeleteExpiredTTLInterval
	}
	if c.CompactionInterval == nil {
		c.CompactionInterval = &DefaultCompactionInterval
	}
	if c.BackupInterval == nil {
		c.BackupInterval = &DefaultBackupInterval
	}
	if c.IndexRebuildInterval == nil {
		c.IndexRebuildInterval = &DefaultIndexRebuildInterval
	}
	if c.SnapshotInterval == nil {
		c.SnapshotInterval = &DefaultSnapshotInterval
	}
	if c.WALCleanupInterval == nil {
		c.WALCleanupInterval = &DefaultWALCleanupInterval
	}
}
