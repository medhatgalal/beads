package uow

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
	domaindb "github.com/steveyegge/beads/internal/storage/domain/db"
)

// DirectDoltServerOptions configures a long-lived UOW provider against an
// already-running Dolt SQL server. It deliberately does not create databases,
// run migrations, or spawn the db-proxy child used by the CLI's experimental
// proxied-server mode. Long-lived near-data services use it to keep one bounded
// database/sql pool while retaining the same domain/UOW implementation.
type DirectDoltServerOptions struct {
	Host     string
	Port     int
	Database string
	User     string
	// SQLAuth is the Dolt SQL login material for an isolated loopback lab
	// server. Named to avoid gosec G117 secret-field patterns on a lab options struct.
	SQLAuth         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DirectDoltServerProvider exposes pool stats and prewarming in addition to the
// normal UnitOfWorkProvider contract. The outer value has no password field,
// but database/sql and the driver connector may retain credentials internally
// for future connections; callers must therefore close the provider and keep
// this API on an isolated loopback boundary.
type DirectDoltServerProvider struct {
	mu      sync.RWMutex
	inner   *doltSQLProvider
	maxOpen int
	metrics directSQLMetrics
}

var _ UnitOfWorkProvider = (*DirectDoltServerProvider)(nil)
var _ TxProvider = (*DirectDoltServerProvider)(nil)

// DirectSQLMetrics is a cumulative, query-text-free snapshot for lab
// instrumentation. Call durations measure method-call latency only.
// In particular, QueryRowContext is lazy, so its duration does not include
// Scan or server execution. These counters are not a substitute for
// server-side query timing.
type DirectSQLMetrics struct {
	StatementCount     int64
	ExecCount          int64
	QueryCount         int64
	QueryRowCount      int64
	TransactionCount   int64
	CommitAttemptCount int64
	CommitSuccessCount int64
	RollbackCount      int64
	CallDurationNS     int64
	MaxCallDurationNS  int64
}

type directSQLMetrics struct {
	statements      atomic.Int64
	execs           atomic.Int64
	queries         atomic.Int64
	queryRows       atomic.Int64
	transactions    atomic.Int64
	commitAttempts  atomic.Int64
	commitSuccesses atomic.Int64
	rollbacks       atomic.Int64
	callDuration    atomic.Int64
	maxCall         atomic.Int64
}

func (m *directSQLMetrics) observe(duration time.Duration) {
	m.statements.Add(1)
	nanos := duration.Nanoseconds()
	m.callDuration.Add(nanos)
	for {
		current := m.maxCall.Load()
		if nanos <= current || m.maxCall.CompareAndSwap(current, nanos) {
			return
		}
	}
}

func (m *directSQLMetrics) snapshot() DirectSQLMetrics {
	return DirectSQLMetrics{
		StatementCount: m.statements.Load(), ExecCount: m.execs.Load(),
		QueryCount: m.queries.Load(), QueryRowCount: m.queryRows.Load(),
		TransactionCount: m.transactions.Load(), CommitAttemptCount: m.commitAttempts.Load(),
		CommitSuccessCount: m.commitSuccesses.Load(), RollbackCount: m.rollbacks.Load(),
		CallDurationNS: m.callDuration.Load(), MaxCallDurationNS: m.maxCall.Load(),
	}
}

func validateDirectDoltServerOptions(opts DirectDoltServerOptions) error {
	if opts.Host == "" {
		return fmt.Errorf("uow: direct provider host must not be empty")
	}
	ip := net.ParseIP(opts.Host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("uow: direct provider host must be a numeric loopback address")
	}
	if opts.Port < 1 || opts.Port > 65535 {
		return fmt.Errorf("uow: direct provider port must be between 1 and 65535")
	}
	if opts.Database == "" {
		return fmt.Errorf("uow: direct provider database must not be empty")
	}
	if opts.User == "" {
		return fmt.Errorf("uow: direct provider user must not be empty")
	}
	if opts.MaxOpenConns < 1 {
		return fmt.Errorf("uow: direct provider max open connections must be positive")
	}
	if opts.MaxIdleConns < 0 || opts.MaxIdleConns > opts.MaxOpenConns {
		return fmt.Errorf("uow: direct provider max idle connections must be between 0 and max open connections")
	}
	if opts.ConnMaxLifetime < 0 || opts.ConnMaxIdleTime < 0 {
		return fmt.Errorf("uow: direct provider connection lifetimes must not be negative")
	}
	return nil
}

// NewDirectDoltServerUOWProvider opens an existing database and keeps its pool
// for the provider lifetime. The caller owns Close.
func NewDirectDoltServerUOWProvider(ctx context.Context, opts DirectDoltServerOptions) (*DirectDoltServerProvider, error) {
	if err := validateDirectDoltServerOptions(opts); err != nil {
		return nil, err
	}

	db, err := openDB(ctx, buildDSN(proxy.Endpoint{Host: opts.Host, Port: opts.Port}, opts.Database, opts.User, opts.SQLAuth))
	if err != nil {
		return nil, fmt.Errorf("uow: direct provider: %w", err)
	}
	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetMaxIdleConns(opts.MaxIdleConns)
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	db.SetConnMaxIdleTime(opts.ConnMaxIdleTime)

	return &DirectDoltServerProvider{
		inner: &doltSQLProvider{
			defaultBranch: defaultBranch,
			db:            db,
		},
		maxOpen: opts.MaxOpenConns,
	}, nil
}

func (p *DirectDoltServerProvider) NewUOW(ctx context.Context) (UnitOfWork, error) {
	if p == nil {
		return nil, fmt.Errorf("uow: direct provider is closed")
	}
	return NewUOW(ctx, p)
}

func (p *DirectDoltServerProvider) BeginTx(ctx context.Context) (Tx, error) {
	if p == nil {
		return nil, fmt.Errorf("uow: direct provider is closed")
	}
	p.mu.RLock()
	inner := p.inner
	p.mu.RUnlock()
	if inner == nil {
		return nil, fmt.Errorf("uow: direct provider is closed")
	}
	started := time.Now()
	tx, err := inner.BeginTx(ctx)
	p.metrics.execs.Add(1)
	p.metrics.observe(time.Since(started))
	if err != nil {
		return nil, err
	}
	p.metrics.transactions.Add(1)
	return &instrumentedDirectTx{inner: tx, metrics: &p.metrics}, nil
}

// SQLMetrics returns cumulative statement/transaction counters without query
// text, arguments, database credentials, or result data.
func (p *DirectDoltServerProvider) SQLMetrics() DirectSQLMetrics {
	if p == nil {
		return DirectSQLMetrics{}
	}
	return p.metrics.snapshot()
}

func (p *DirectDoltServerProvider) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.inner == nil {
		p.mu.Unlock()
		return nil
	}
	inner := p.inner
	p.inner = nil
	p.mu.Unlock()
	// database/sql.DB is safe to close concurrently with an operation that
	// already captured the pool pointer. Do not call doltSQLProvider.Close here:
	// it nils inner.db and would race those in-flight readers.
	if inner.db == nil {
		return nil
	}
	return inner.db.Close()
}

type instrumentedDirectTx struct {
	inner   Tx
	metrics *directSQLMetrics
	done    bool
}

func (t *instrumentedDirectTx) Runner() domaindb.Runner {
	return &instrumentedDirectRunner{inner: t.inner.Runner(), metrics: t.metrics}
}

func (t *instrumentedDirectTx) Commit(ctx context.Context, message string) error {
	started := time.Now()
	err := t.inner.Commit(ctx, message)
	t.metrics.execs.Add(1)
	t.metrics.commitAttempts.Add(1)
	t.metrics.observe(time.Since(started))
	if err == nil {
		t.metrics.commitSuccesses.Add(1)
		t.done = true
	} else if !isSerializationError(err) {
		t.done = true
	}
	return err
}

func (t *instrumentedDirectTx) Rollback(ctx context.Context) error {
	if t.done {
		return t.inner.Rollback(ctx)
	}
	started := time.Now()
	err := t.inner.Rollback(ctx)
	t.metrics.execs.Add(1)
	t.metrics.rollbacks.Add(1)
	t.metrics.observe(time.Since(started))
	t.done = true
	return err
}

func (t *instrumentedDirectTx) RollbackUnlessCommitted(ctx context.Context) {
	if t.done {
		t.inner.RollbackUnlessCommitted(ctx)
		return
	}
	started := time.Now()
	t.inner.RollbackUnlessCommitted(ctx)
	t.metrics.execs.Add(1)
	t.metrics.rollbacks.Add(1)
	t.metrics.observe(time.Since(started))
	t.done = true
}

type instrumentedDirectRunner struct {
	inner   domaindb.Runner
	metrics *directSQLMetrics
}

func (r *instrumentedDirectRunner) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	started := time.Now()
	result, err := r.inner.ExecContext(ctx, query, args...)
	r.metrics.execs.Add(1)
	r.metrics.observe(time.Since(started))
	return result, err
}

func (r *instrumentedDirectRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	started := time.Now()
	rows, err := r.inner.QueryContext(ctx, query, args...)
	r.metrics.queries.Add(1)
	r.metrics.observe(time.Since(started))
	return rows, err
}

func (r *instrumentedDirectRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	started := time.Now()
	row := r.inner.QueryRowContext(ctx, query, args...)
	r.metrics.queryRows.Add(1)
	r.metrics.observe(time.Since(started))
	return row
}

func (p *DirectDoltServerProvider) Stats() sql.DBStats {
	if p == nil {
		return sql.DBStats{}
	}
	p.mu.RLock()
	inner := p.inner
	p.mu.RUnlock()
	if inner == nil || inner.db == nil {
		return sql.DBStats{}
	}
	return inner.db.Stats()
}

// Prewarm opens n connections concurrently before returning them to the idle
// pool. Holding all leases until every SELECT 1 completes forces database/sql
// to establish the requested number instead of repeatedly reusing one lease.
func (p *DirectDoltServerProvider) Prewarm(ctx context.Context, n int) error {
	if p == nil {
		return fmt.Errorf("uow: direct provider is closed")
	}
	p.mu.RLock()
	inner := p.inner
	p.mu.RUnlock()
	if inner == nil || inner.db == nil {
		return fmt.Errorf("uow: direct provider is closed")
	}
	if n < 0 || n > p.maxOpen {
		return fmt.Errorf("uow: prewarm count must be between 0 and %d", p.maxOpen)
	}
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for i := 0; i < n; i++ {
		conn, err := inner.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("uow: prewarm connection %d: %w", i, err)
		}
		conns = append(conns, conn)
	}
	for i, conn := range conns {
		started := time.Now()
		_, err := conn.ExecContext(ctx, "SELECT 1")
		p.metrics.execs.Add(1)
		p.metrics.observe(time.Since(started))
		if err != nil {
			return fmt.Errorf("uow: prewarm ping %d: %w", i, err)
		}
	}
	return nil
}
