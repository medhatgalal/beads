package fleetlazy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	manager, err := NewLazyPoolManager(1, func(database string) (ManagedPool, error) {
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
		lease *PoolLease
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
	manager, err := NewLazyPoolManager(1, func(database string) (ManagedPool, error) {
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
	manager, err := NewLazyPoolManager(2, func(string) (ManagedPool, error) { return &fakePool{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	a, _ := manager.Acquire(context.Background(), "beads_perf_lab_scale_000")
	b, _ := manager.Acquire(context.Background(), "beads_perf_lab_scale_001")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan *PoolLease, 2)
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
	manager, err := NewLazyPoolManager(2, func(database string) (ManagedPool, error) {
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
	manager, err := NewLazyPoolManager(16, func(string) (ManagedPool, error) { return &fakePool{}, nil })
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

func TestCloseContextDrainsLeaseBeforeClosingPool(t *testing.T) {
	pool := &fakePool{}
	manager, err := NewLazyPoolManager(1, func(string) (ManagedPool, error) { return pool, nil })
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
	manager, err := NewLazyPoolManager(1, func(string) (ManagedPool, error) { return pool, nil })
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
	if _, err := manager.Acquire(context.Background(), fleetDatabaseName(1)); !errors.Is(err, ErrManagerClosed) {
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

func waitForManagerClosed(t *testing.T, manager *LazyPoolManager) {
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

func fleetDatabaseName(index int) string {
	return fmt.Sprintf("beads_perf_lab_fleet_%03d", index)
}

func isFleetDatabase(database string) bool {
	var index int
	_, err := fmt.Sscanf(database, "beads_perf_lab_fleet_%d", &index)
	return err == nil && index >= 0 && index < 500 && database == fleetDatabaseName(index)
}
