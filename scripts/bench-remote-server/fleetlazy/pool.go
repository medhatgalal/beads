package fleetlazy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrManagerClosed = errors.New("lazy pool manager is closed")

const defaultManagerCloseTimeout = 5 * time.Second

type ManagedPool interface {
	Close() error
	OpenConnections() int
}

type PoolFactory func(database string) (ManagedPool, error)

type poolEntry struct {
	pool       ManagedPool
	inUse      int
	lastUsed   time.Time
	closing    bool
	closeError error
}

type ManagerMetrics struct {
	Budget                     int   `json:"budget"`
	ResidentPools              int   `json:"resident_pools"`
	OpenConnections            int   `json:"open_connections"`
	MaxResidentPools           int   `json:"max_resident_pools"`
	MaxObservedOpenConnections int   `json:"max_observed_open_connections"`
	PoolsCreated               int64 `json:"pools_created"`
	Evictions                  int64 `json:"evictions"`
	BudgetWaits                int64 `json:"budget_waits"`
	ClosingPools               int   `json:"closing_pools"`
	CloseFailedPools           int   `json:"close_failed_pools"`
	CloseFailures              int64 `json:"close_failures"`
}

// LazyPoolManager gives each resident database at most one SQL connection and
// caps the number of resident pools for the whole cell. An idle LRU pool is
// closed before another database is admitted. Cold databases therefore hold
// no pool and no idle connection.
type LazyPoolManager struct {
	mu      sync.Mutex
	budget  int
	factory PoolFactory
	entries map[string]*poolEntry
	wake    chan struct{}
	closed  bool
	metrics ManagerMetrics
}

func NewLazyPoolManager(budget int, factory PoolFactory) (*LazyPoolManager, error) {
	if budget < 1 || factory == nil {
		return nil, fmt.Errorf("positive pool budget and factory are required")
	}
	return &LazyPoolManager{
		budget: budget, factory: factory, entries: make(map[string]*poolEntry),
		wake: make(chan struct{}), metrics: ManagerMetrics{Budget: budget},
	}, nil
}

type PoolLease struct {
	manager  *LazyPoolManager
	database string
	pool     ManagedPool
	once     sync.Once
}

func (l *PoolLease) Release() {
	if l == nil || l.manager == nil {
		return
	}
	l.once.Do(func() { l.manager.release(l.database) })
}

func (m *LazyPoolManager) Acquire(ctx context.Context, database string) (*PoolLease, error) {
	if database == "" {
		return nil, fmt.Errorf("database is required")
	}
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrManagerClosed
		}
		if entry := m.entries[database]; entry != nil {
			if entry.closeError != nil {
				err := entry.closeError
				m.mu.Unlock()
				return nil, fmt.Errorf("pool for %s is quarantined after close failure: %w", database, err)
			}
			if entry.closing {
				wake := m.wake
				m.metrics.BudgetWaits++
				m.mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-wake:
					continue
				}
			}
			entry.inUse++
			m.observeLocked()
			lease := &PoolLease{manager: m, database: database, pool: entry.pool}
			m.mu.Unlock()
			return lease, nil
		}

		if len(m.entries) < m.budget {
			pool, err := m.factory(database)
			if err != nil {
				m.mu.Unlock()
				return nil, fmt.Errorf("open lazy pool for %s: %w", database, err)
			}
			m.entries[database] = &poolEntry{pool: pool, inUse: 1, lastUsed: time.Now().UTC()}
			m.metrics.PoolsCreated++
			m.observeLocked()
			lease := &PoolLease{manager: m, database: database, pool: pool}
			m.mu.Unlock()
			return lease, nil
		}

		victimName, victim := m.oldestIdleLocked()
		if victim != nil {
			victim.closing = true
			m.metrics.Evictions++
			m.observeLocked()
			m.mu.Unlock()
			if err := m.finishClose(victimName, victim, victim.pool.Close()); err != nil {
				return nil, fmt.Errorf("close evicted pool: %w", err)
			}
			continue
		}

		m.metrics.BudgetWaits++
		wake := m.wake
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (m *LazyPoolManager) release(database string) {
	m.mu.Lock()
	entry := m.entries[database]
	if entry != nil && entry.inUse > 0 {
		entry.inUse--
		entry.lastUsed = time.Now().UTC()
	}
	m.observeLocked()
	m.broadcastLocked()
	m.mu.Unlock()
}

func (m *LazyPoolManager) oldestIdleLocked() (string, *poolEntry) {
	var name string
	var candidate *poolEntry
	for currentName, entry := range m.entries {
		if entry.inUse != 0 || entry.closing {
			continue
		}
		if candidate == nil || entry.lastUsed.Before(candidate.lastUsed) ||
			(entry.lastUsed.Equal(candidate.lastUsed) && currentName < name) {
			name, candidate = currentName, entry
		}
	}
	return name, candidate
}

func (m *LazyPoolManager) HibernateIdle() error {
	m.mu.Lock()
	type target struct {
		name  string
		entry *poolEntry
	}
	var targets []target
	for name, entry := range m.entries {
		if entry.inUse == 0 && !entry.closing {
			entry.closing = true
			targets = append(targets, target{name: name, entry: entry})
		}
	}
	m.observeLocked()
	m.mu.Unlock()
	var errs []error
	for _, target := range targets {
		errs = append(errs, m.finishClose(target.name, target.entry, target.entry.pool.Close()))
	}
	return errors.Join(errs...)
}

func (m *LazyPoolManager) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultManagerCloseTimeout)
	defer cancel()
	return m.CloseContext(ctx)
}

// CloseContext first rejects new acquisitions, then waits for every active
// lease and concurrent eviction close to drain. A deadline leaves all in-use
// pools open and referenced; callers may release leases and retry shutdown.
func (m *LazyPoolManager) CloseContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("close context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	type target struct {
		name  string
		entry *poolEntry
	}
	for {
		m.mu.Lock()
		if !m.closed {
			m.closed = true
			m.broadcastLocked()
		}
		activeLeases := 0
		closing := false
		for _, entry := range m.entries {
			activeLeases += entry.inUse
			closing = closing || entry.closing
		}
		if activeLeases == 0 && !closing {
			var targets []target
			for name, entry := range m.entries {
				entry.closing = true
				targets = append(targets, target{name: name, entry: entry})
			}
			m.observeLocked()
			m.mu.Unlock()
			var errs []error
			for _, target := range targets {
				errs = append(errs, m.finishClose(target.name, target.entry, target.entry.pool.Close()))
			}
			return errors.Join(errs...)
		}
		wake := m.wake
		m.observeLocked()
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("close drain stopped with %d active leases: %w", activeLeases, ctx.Err())
		case <-wake:
		}
	}
}

// finishClose keeps the resource in the budget and in metrics until Close has
// actually succeeded. A failed close is quarantined in-place so the manager
// never loses the only reference to a potentially live connection.
func (m *LazyPoolManager) finishClose(name string, entry *poolEntry, closeErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.entries[name]
	if current != entry {
		return fmt.Errorf("pool identity changed while closing %s", name)
	}
	entry.closing = false
	if closeErr == nil {
		delete(m.entries, name)
	} else {
		entry.closeError = closeErr
		m.metrics.CloseFailures++
	}
	m.observeLocked()
	m.broadcastLocked()
	return closeErr
}

func (m *LazyPoolManager) Metrics() ManagerMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observeLocked()
	return m.metrics
}

func (m *LazyPoolManager) observeLocked() {
	resident := len(m.entries)
	open := 0
	closing := 0
	closeFailed := 0
	for _, entry := range m.entries {
		open += entry.pool.OpenConnections()
		if entry.closing {
			closing++
		}
		if entry.closeError != nil {
			closeFailed++
		}
	}
	m.metrics.ResidentPools = resident
	m.metrics.OpenConnections = open
	m.metrics.ClosingPools = closing
	m.metrics.CloseFailedPools = closeFailed
	if resident > m.metrics.MaxResidentPools {
		m.metrics.MaxResidentPools = resident
	}
	if open > m.metrics.MaxObservedOpenConnections {
		m.metrics.MaxObservedOpenConnections = open
	}
}

// broadcastLocked closes a generation channel and creates the next one. Every
// waiter on the old generation wakes, so multiple releases cannot collapse
// into one notification and strand waiters while an idle victim exists.
func (m *LazyPoolManager) broadcastLocked() {
	close(m.wake)
	m.wake = make(chan struct{})
}
