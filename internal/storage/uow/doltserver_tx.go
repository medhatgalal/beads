package uow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/steveyegge/beads/internal/storage/domain/db"
)

type doltServerTx struct {
	conn *sql.Conn
	done bool
}

var _ Tx = (*doltServerTx)(nil)

func (t *doltServerTx) Runner() db.Runner {
	return t.conn
}

func (t *doltServerTx) Commit(ctx context.Context, message string) error {
	if t.done {
		return errors.New("uow: commit: already done")
	}
	_, err := t.conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?);", message)
	if err != nil {
		// Leave the transaction open so Close can roll it back with its bounded,
		// cancellation-independent cleanup context. This is required for both
		// serialization failures and ambiguous/non-retryable commit errors: if the
		// commit did not land, returning the session to the pool would strand dirty
		// state for the next START TRANSACTION to commit implicitly.
		return err
	}
	t.done = true
	t.releaseConn()
	return err
}

func (t *doltServerTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	_, err := t.conn.ExecContext(ctx, "ROLLBACK;")
	if err != nil {
		// database/sql returns sql.Conn.Close connections to the pool. Poison the
		// driver connection first so a failed rollback can never be reused with an
		// active transaction.
		t.discardConn()
		return err
	}
	t.releaseConn()
	return err
}

func (t *doltServerTx) RollbackUnlessCommitted(ctx context.Context) {
	if !t.done {
		_ = t.Rollback(ctx)
	}
}

func (t *doltServerTx) releaseConn() {
	if t.conn != nil {
		_ = t.conn.Close()
		t.conn = nil
	}
}

func (t *doltServerTx) discardConn() {
	if t.conn != nil {
		discardSQLConn(t.conn)
		t.conn = nil
	}
}

// discardSQLConn marks a pinned database/sql connection bad before closing its
// handle. Returning driver.ErrBadConn from Raw makes database/sql close the
// physical driver connection instead of putting a potentially dirty session
// back in the pool.
func discardSQLConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
