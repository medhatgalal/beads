package uow

import (
	"context"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain/db"
)

type cleanupObservingTx struct {
	contextErr  error
	deadline    time.Time
	hasDeadline bool
}

func (t *cleanupObservingTx) Runner() db.Runner                    { return nil }
func (t *cleanupObservingTx) Commit(context.Context, string) error { return nil }
func (t *cleanupObservingTx) Rollback(context.Context) error       { return nil }
func (t *cleanupObservingTx) RollbackUnlessCommitted(ctx context.Context) {
	t.contextErr = ctx.Err()
	t.deadline, t.hasDeadline = ctx.Deadline()
}

func TestBaseUOWCloseUsesBoundedUncanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	tx := &cleanupObservingTx{}
	(&baseUOW{tx: tx}).Close(ctx)
	if tx.contextErr != nil || !tx.hasDeadline {
		t.Fatalf("cleanup context err=%v has_deadline=%v", tx.contextErr, tx.hasDeadline)
	}
	if remaining := tx.deadline.Sub(started); remaining < uowCleanupTimeout-time.Second || remaining > uowCleanupTimeout+time.Second {
		t.Fatalf("cleanup deadline=%s, want about %s", remaining, uowCleanupTimeout)
	}
}
