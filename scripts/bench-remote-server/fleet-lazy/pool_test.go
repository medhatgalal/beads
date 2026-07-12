package main

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type fakePool struct {
	closed     atomic.Bool
	closeCalls atomic.Int64
	closeBlock <-chan struct{}
	closeErrs  int64
}

func (p *fakePool) Close() error {
	call := p.closeCalls.Add(1)
	if p.closeBlock != nil {
		<-p.closeBlock
	}
	if call <= p.closeErrs {
		return errors.New("injected close failure")
	}
	p.closed.Store(true)
	return nil
}

func TestLazyManagerCountsClosingPoolUntilCloseSucceeds(t *testing.T) {
	closeBlock := make(chan struct{})
	var factoryCalls atomic.Int64
	manager, err := newLazyPoolManager(1, func(database string) (managedPool, error) {
		factoryCalls.Add(1)
		if database == "beads_perf_lab_scale_000" {
			return &fakePool{closeBlock: closeBlock}, nil
		}
		return &fakePool{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	first, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_000")
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	type acquireResult struct {
		lease *poolLease
		err   error
	}
	result := make(chan acquireResult, 1)
	go func() {
		lease, acquireErr := manager.Acquire(context.Background(), "beads_perf_lab_scale_001")
		result <- acquireResult{lease: lease, err: acquireErr}
	}()

	deadline := time.Now().Add(time.Second)
	for manager.Metrics().ClosingPools != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	metrics := manager.Metrics()
	if metrics.ResidentPools != 1 || metrics.ClosingPools != 1 || factoryCalls.Load() != 1 {
		t.Fatalf("closing resource escaped budget: metrics=%+v factory_calls=%d", metrics, factoryCalls.Load())
	}
	close(closeBlock)
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	got.lease.Release()
	if metrics := manager.Metrics(); metrics.MaxResidentPools > 1 || metrics.MaxObservedOpenConnections > 1 {
		t.Fatalf("manager escaped budget after close: %+v", metrics)
	}
}

func TestLazyManagerRetainsAndRetriesFailedClose(t *testing.T) {
	failed := &fakePool{closeErrs: 1}
	manager, err := newLazyPoolManager(1, func(database string) (managedPool, error) {
		if database == "beads_perf_lab_scale_000" {
			return failed, nil
		}
		return &fakePool{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	lease, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_000")
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_001"); err == nil {
		t.Fatal("eviction unexpectedly ignored close failure")
	}
	metrics := manager.Metrics()
	if metrics.ResidentPools != 1 || metrics.CloseFailedPools != 1 || metrics.CloseFailures != 1 || failed.closed.Load() {
		t.Fatalf("failed close reference was not quarantined: %+v", metrics)
	}
	if err := manager.HibernateIdle(); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if !failed.closed.Load() || manager.Metrics().ResidentPools != 0 {
		t.Fatalf("retry did not release quarantined pool: %+v", manager.Metrics())
	}
}

func TestLazyManagerBroadcastWakesAllWaiters(t *testing.T) {
	manager, err := newLazyPoolManager(2, func(string) (managedPool, error) { return &fakePool{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	a, _ := manager.Acquire(context.Background(), "beads_perf_lab_scale_000")
	b, _ := manager.Acquire(context.Background(), "beads_perf_lab_scale_001")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan *poolLease, 2)
	errs := make(chan error, 2)
	for _, database := range []string{"beads_perf_lab_scale_002", "beads_perf_lab_scale_003"} {
		database := database
		go func() {
			lease, acquireErr := manager.Acquire(ctx, database)
			if acquireErr != nil {
				errs <- acquireErr
				return
			}
			results <- lease
		}()
	}
	deadline := time.Now().Add(time.Second)
	for manager.Metrics().BudgetWaits < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	a.Release()
	b.Release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case lease := <-results:
			lease.Release()
		case <-ctx.Done():
			t.Fatal("a waiter remained stranded despite an idle victim")
		}
	}
}

func (p *fakePool) OpenConnections() int {
	if p.closed.Load() {
		return 0
	}
	return 1
}

func TestLazyManagerEvictsOnlyIdleAndHibernatesToZero(t *testing.T) {
	created := map[string]*fakePool{}
	manager, err := newLazyPoolManager(2, func(database string) (managedPool, error) {
		pool := &fakePool{}
		created[database] = pool
		return pool, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	a, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_000")
	if err != nil {
		t.Fatal(err)
	}
	b, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_001")
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := manager.Acquire(blocked, "beads_perf_lab_scale_002"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fully occupied budget error=%v", err)
	}
	a.Release()
	c, err := manager.Acquire(context.Background(), "beads_perf_lab_scale_002")
	if err != nil {
		t.Fatal(err)
	}
	if !created["beads_perf_lab_scale_000"].closed.Load() {
		t.Fatal("idle LRU pool was not closed before replacement")
	}
	if created["beads_perf_lab_scale_001"].closed.Load() {
		t.Fatal("in-use pool was evicted")
	}
	b.Release()
	c.Release()
	if err := manager.HibernateIdle(); err != nil {
		t.Fatal(err)
	}
	metrics := manager.Metrics()
	if metrics.ResidentPools != 0 || metrics.OpenConnections != 0 || metrics.MaxResidentPools > 2 ||
		metrics.MaxObservedOpenConnections > 2 || metrics.BudgetWaits == 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestLazyManagerStaysBoundedAcrossThreeHundredDatabases(t *testing.T) {
	manager, err := newLazyPoolManager(16, func(string) (managedPool, error) { return &fakePool{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	const operations = 30000
	jobs := make(chan int, 128)
	var wg sync.WaitGroup
	for worker := 0; worker < 64; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				lease, err := manager.Acquire(context.Background(), fleetDatabaseName(index%300))
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				time.Sleep(time.Microsecond)
				lease.Release()
			}
		}()
	}
	for index := 0; index < operations; index++ {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	if err := manager.HibernateIdle(); err != nil {
		t.Fatal(err)
	}
	metrics := manager.Metrics()
	if metrics.MaxResidentPools > 16 || metrics.MaxObservedOpenConnections > 16 ||
		metrics.ResidentPools != 0 || metrics.OpenConnections != 0 {
		t.Fatalf("manager escaped bounds: %+v", metrics)
	}
	if metrics.PoolsCreated < 300 || metrics.Evictions == 0 {
		t.Fatalf("test did not exercise churn: %+v", metrics)
	}
}

func TestValidateLazyConfigFailsClosed(t *testing.T) {
	base := lazyConfig{
		host: "127.0.0.1", port: 13360, databaseN: 100, activeSets: "5,30,100",
		budget: 28, serverCap: 32, workers: 64, queue: 256, operations: 640,
		output: "/private/tmp/beads-fleet-scale-test/lazy.json", execute: true,
		labID: "dc6020ef-b433-41b6-9426-cedd9bf40502",
	}
	if _, err := validateLazyConfig(base); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []lazyConfig{
		func() lazyConfig { cfg := base; cfg.host = "10.0.0.1"; return cfg }(),
		func() lazyConfig { cfg := base; cfg.port = 3306; return cfg }(),
		func() lazyConfig { cfg := base; cfg.port = 3307; return cfg }(),
		func() lazyConfig { cfg := base; cfg.labID = ""; return cfg }(),
		func() lazyConfig { cfg := base; cfg.labID = "DC6020EF-B433-41B6-9426-CEDD9BF40502"; return cfg }(),
		func() lazyConfig { cfg := base; cfg.execute = false; return cfg }(),
		func() lazyConfig { cfg := base; cfg.budget = 0; return cfg }(),
		func() lazyConfig { cfg := base; cfg.serverCap = cfg.budget + 3; return cfg }(),
		func() lazyConfig { cfg := base; cfg.queue = 1; return cfg }(),
		func() lazyConfig { cfg := base; cfg.output = "/tmp/not-approved.json"; return cfg }(),
	}
	for i, cfg := range tests {
		if _, err := validateLazyConfig(cfg); err == nil {
			t.Errorf("case %d unexpectedly passed", i)
		}
	}
}

func TestVerifyLabControlIdentityRequiresExactSyntheticSingleton(t *testing.T) {
	const labID = "dc6020ef-b433-41b6-9426-cedd9bf40502"
	tests := []struct {
		name        string
		rows        *sqlmock.Rows
		queryErr    error
		wantFailure bool
	}{
		{name: "exact", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "synthetic", labID)},
		{name: "absent", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(0, "", ""), wantFailure: true},
		{name: "multiple", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(2, "synthetic", labID), wantFailure: true},
		{name: "environment", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "production", labID), wantFailure: true},
		{name: "other lab", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "synthetic", "80d22c54-066c-4f89-b6e8-d94e19b4e902"), wantFailure: true},
		{name: "missing table", queryErr: errors.New("table not found"), wantFailure: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectation := mock.ExpectQuery(regexp.QuoteMeta(labControlIdentityQuery))
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(test.rows)
			}
			err = verifyLabControlIdentity(context.Background(), db, labID)
			if (err != nil) != test.wantFailure {
				t.Fatalf("verifyLabControlIdentity error = %v, want failure %v", err, test.wantFailure)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServerProcessCountPropagatesRowIterationError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, errors.New("injected row error"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW PROCESSLIST")).WillReturnRows(rows)
	if count, err := serverProcessCount(context.Background(), db); err == nil || count != 0 {
		t.Fatalf("serverProcessCount = %d, %v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForServerProcessBaselineObservesDrain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := regexp.QuoteMeta("SHOW PROCESSLIST")
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count, err := waitForServerProcessBaseline(ctx, db, 1, 200*time.Millisecond)
	if err != nil || count != 1 {
		t.Fatalf("waitForServerProcessBaseline = %d, %v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFleetDatabaseNamesMatchFleetScaleContract(t *testing.T) {
	for index, want := range map[int]string{
		0: "beads_perf_lab_scale_000", 99: "beads_perf_lab_scale_099", 499: "beads_perf_lab_scale_499",
	} {
		if got := fleetDatabaseName(index); got != want || !isFleetDatabase(got) {
			t.Fatalf("database %d = %q, valid=%t, want %q", index, got, isFleetDatabase(got), want)
		}
	}
	for _, invalid := range []string{
		"beads_scale_000", "beads_perf_lab_scale_00", "beads_perf_lab_scale_0000",
		"beads_perf_lab_scale_-01", "beads_perf_lab_scale_500", "production",
	} {
		if isFleetDatabase(invalid) {
			t.Fatalf("invalid fleet database %q passed", invalid)
		}
	}
}

func TestCloseContextDrainsLeaseBeforeClosingPool(t *testing.T) {
	pool := &fakePool{}
	manager, err := newLazyPoolManager(1, func(string) (managedPool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(context.Background(), fleetDatabaseName(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.CloseContext(ctx) }()
	waitForManagerClosed(t, manager)
	if pool.closeCalls.Load() != 0 || pool.closed.Load() {
		t.Fatal("CloseContext closed a pool while its lease was active")
	}
	lease.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("CloseContext did not finish after the lease drained")
	}
	if pool.closeCalls.Load() != 1 || !pool.closed.Load() || manager.Metrics().ResidentPools != 0 {
		t.Fatalf("pool was not closed exactly once after drain: calls=%d metrics=%+v", pool.closeCalls.Load(), manager.Metrics())
	}
}

func TestCloseContextDeadlineLeavesActivePoolOpenForSafeRetry(t *testing.T) {
	pool := &fakePool{}
	manager, err := newLazyPoolManager(1, func(string) (managedPool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(context.Background(), fleetDatabaseName(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = manager.CloseContext(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext error = %v", err)
	}
	if pool.closeCalls.Load() != 0 || pool.closed.Load() || manager.Metrics().ResidentPools != 1 {
		t.Fatalf("timed-out close touched active pool: calls=%d metrics=%+v", pool.closeCalls.Load(), manager.Metrics())
	}
	if _, err := manager.Acquire(context.Background(), fleetDatabaseName(1)); !errors.Is(err, errManagerClosed) {
		t.Fatalf("closed manager admitted a new lease: %v", err)
	}
	lease.Release()
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	if err := manager.CloseContext(retryCtx); err != nil {
		t.Fatalf("retry close after release: %v", err)
	}
	if pool.closeCalls.Load() != 1 || !pool.closed.Load() || manager.Metrics().ResidentPools != 0 {
		t.Fatalf("retry did not close retained pool: calls=%d metrics=%+v", pool.closeCalls.Load(), manager.Metrics())
	}
}

func waitForManagerClosed(t *testing.T, manager *lazyPoolManager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		closed := manager.closed
		manager.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manager did not enter draining state")
}
