package scheduler

import (
	"context"
	"time"
)

func isContextCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (d *daemonScheduler) inMemoryEviction(ctx context.Context) {
	for {
		if isContextCancelled(d.ctx) {
			return
		}

		if len(d.tree.ToDelete) == 0 {
			time.Sleep(*d.cfg.DeleteExpiredTTLInterval)
			continue
		}

		d.tree.M.Lock()
		for key := range d.tree.ToDelete {
			d.tree.Delete([]byte(key))
		}
		d.tree.M.Unlock()

		time.Sleep(*d.cfg.DeleteExpiredTTLInterval)
	}
}

func (d *daemonScheduler) EvictTTL() error {
	if isContextCancelled(d.ctx) {
		return d.ctx.Err()
	}

	if d.tree == nil {
		return ErrNilTree
	}

	go d.inMemoryEviction(d.ctx)
	return nil
}

func (d *daemonScheduler) Compact() error
func (d *daemonScheduler) Backup() error
func (d *daemonScheduler) IndexRebuild() error
func (d *daemonScheduler) Snapshot() error
func (d *daemonScheduler) WALCleanup() error
