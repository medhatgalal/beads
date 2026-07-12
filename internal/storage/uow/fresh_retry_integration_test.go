package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

const freshRetryPerfDoltHost = "127.0.0.1"

type freshRetryObservedProvider struct {
	inner *doltSQLProvider

	mu           sync.Mutex
	nextID       int
	events       []string
	commitErrors []error
}

func (p *freshRetryObservedProvider) NewUOW(ctx context.Context) (UnitOfWork, error) {
	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.events = append(p.events, fmt.Sprintf("open:%d:start", id))
	p.mu.Unlock()

	uw, err := p.inner.NewUOW(ctx)
	if err != nil {
		p.recordEvent(fmt.Sprintf("open:%d:error", id))
		return nil, err
	}
	p.recordEvent(fmt.Sprintf("open:%d:ok", id))
	return &freshRetryObservedUOW{UnitOfWork: uw, id: id, provider: p}, nil
}

func (p *freshRetryObservedProvider) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

func (p *freshRetryObservedProvider) recordEvent(event string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *freshRetryObservedProvider) recordCommit(id int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		p.events = append(p.events, fmt.Sprintf("commit:%d:ok", id))
		return
	}

	p.commitErrors = append(p.commitErrors, err)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		p.events = append(p.events, fmt.Sprintf("commit:%d:mysql:%d", id, mysqlErr.Number))
		return
	}
	p.events = append(p.events, fmt.Sprintf("commit:%d:error", id))
}

func (p *freshRetryObservedProvider) snapshot() ([]string, []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...), append([]error(nil), p.commitErrors...)
}

type freshRetryObservedUOW struct {
	UnitOfWork
	id       int
	provider *freshRetryObservedProvider
}

func TestRunWithFreshUOWRetries_CancelledMutationRollsBackBeforeSameConnectionReuse(t *testing.T) {
	providerIface := newTestUOWProvider(t)
	provider, ok := providerIface.(*doltSQLProvider)
	require.True(t, ok, "dedicated real-Dolt provider has unexpected type %T", providerIface)
	provider.db.SetMaxOpenConns(1)
	provider.db.SetMaxIdleConns(1)

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer seedCancel()
	seed, err := provider.NewUOW(seedCtx)
	require.NoError(t, err)
	_, err = seed.RawSQLUseCase().Exec(seedCtx, `
CREATE TABLE fresh_retry_cancel_guard (
  id INT PRIMARY KEY,
  value INT NOT NULL
)`)
	require.NoError(t, err)
	_, err = seed.RawSQLUseCase().Exec(seedCtx, "INSERT INTO fresh_retry_cancel_guard(id, value) VALUES (1, 0)")
	require.NoError(t, err)
	require.NoError(t, seed.Commit(seedCtx, "fresh retry cancel baseline"))
	seed.Close(seedCtx)

	operationCtx, cancelOperation := context.WithCancel(context.Background())
	var mutatedConnectionID int
	err = RunWithFreshUOWRetries(operationCtx, provider, "fresh retry canceled mutation must not commit", func(ctx context.Context, uw UnitOfWork) error {
		connectionID, queryErr := freshRetryScalarInt(ctx, uw, "SELECT CONNECTION_ID()")
		if queryErr != nil {
			return queryErr
		}
		mutatedConnectionID = connectionID
		if _, updateErr := uw.RawSQLUseCase().Exec(ctx,
			"UPDATE fresh_retry_cancel_guard SET value = 99 WHERE id = 1"); updateErr != nil {
			return updateErr
		}
		cancelOperation()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)

	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()
	reader, err := provider.NewUOW(readCtx)
	require.NoError(t, err)
	defer reader.Close(readCtx)
	reusedConnectionID, err := freshRetryScalarInt(readCtx, reader, "SELECT CONNECTION_ID()")
	require.NoError(t, err)
	require.Equal(t, mutatedConnectionID, reusedConnectionID,
		"test did not force reuse of the physical session that held the canceled mutation")
	value, err := freshRetryScalarInt(readCtx, reader,
		"SELECT value FROM fresh_retry_cancel_guard WHERE id = 1")
	require.NoError(t, err)
	require.Equal(t, 0, value, "canceled mutation was implicitly committed by the reused session")
	commitCount, err := freshRetryScalarInt(readCtx, reader,
		"SELECT COUNT(*) FROM dolt_log WHERE message = 'fresh retry canceled mutation must not commit'")
	require.NoError(t, err)
	require.Zero(t, commitCount)
}

func (u *freshRetryObservedUOW) Commit(ctx context.Context, message string) error {
	err := u.UnitOfWork.Commit(ctx, message)
	u.provider.recordCommit(u.id, err)
	return err
}

func (u *freshRetryObservedUOW) Close(ctx context.Context) {
	// Record after Close returns so the event order proves the failed
	// transaction was rolled back and its pinned connection released before
	// RunWithFreshUOWRetries asked the provider for the next transaction.
	u.UnitOfWork.Close(ctx)
	u.provider.recordEvent(fmt.Sprintf("close:%d", u.id))
}

func TestRunWithFreshUOWRetries_RealDoltConflictReplaysOnFreshSnapshot(t *testing.T) {
	port := freshRetryPerfDoltPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint := proxy.Endpoint{Host: freshRetryPerfDoltHost, Port: port}
	adminDB, err := openDB(ctx, buildDSN(endpoint, "", "root", ""))
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeErr := adminDB.Close(); closeErr != nil {
			t.Errorf("close isolated performance admin connection: %v", closeErr)
		}
	})

	database := freshRetryDatabaseName()
	require.True(t, strings.HasPrefix(database, "beads_perf_lab_"))
	require.LessOrEqual(t, len(database), 64)
	_, err = adminDB.ExecContext(ctx, "CREATE DATABASE `"+database+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, dropErr := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`"); dropErr != nil {
			t.Errorf("drop isolated performance database %s: %v", database, dropErr)
		}
		var remaining int
		if checkErr := adminDB.QueryRowContext(cleanupCtx,
			"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", database).Scan(&remaining); checkErr != nil {
			t.Errorf("verify isolated performance database %s was dropped: %v", database, checkErr)
		} else if remaining != 0 {
			t.Errorf("isolated performance database %s remained after cleanup", database)
		}
	})

	db, err := openDB(ctx, buildDSN(endpoint, database, "root", ""))
	require.NoError(t, err)
	db.SetMaxOpenConns(4)
	provider := &doltSQLProvider{defaultBranch: defaultBranch, db: db}
	t.Cleanup(func() { require.NoError(t, provider.Close(context.Background())) })

	_, err = db.ExecContext(ctx, `
CREATE TABLE fresh_retry_counter (
  id INT PRIMARY KEY,
  value INT NOT NULL,
  writer_token VARCHAR(32) NOT NULL
)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO fresh_retry_counter(id, value, writer_token) VALUES (1, 0, 'baseline')")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", "fresh retry baseline")
	require.NoError(t, err)

	// Start and mutate the competitor first, but do not commit it. The helper's
	// first UOW therefore starts from the same committed value (zero). The
	// different writer_token values make the overlapping row edits conflict
	// deterministically rather than allowing Dolt's same-value cell merge.
	competitor, err := provider.NewUOW(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { competitor.Close(context.Background()) })
	competitorBaseline, err := freshRetryCounterValue(ctx, competitor)
	require.NoError(t, err)
	require.Equal(t, 0, competitorBaseline)
	_, err = competitor.RawSQLUseCase().Exec(ctx, `
UPDATE fresh_retry_counter
SET value = value + 1, writer_token = 'competitor'
WHERE id = 1`)
	require.NoError(t, err)

	observed := &freshRetryObservedProvider{inner: provider}
	bodyRuns := 0
	seenValues := make([]int, 0, 2)
	err = RunWithFreshUOWRetries(ctx, observed, "fresh retry helper", func(ctx context.Context, uw UnitOfWork) error {
		bodyRuns++
		value, readErr := freshRetryCounterValue(ctx, uw)
		if readErr != nil {
			return readErr
		}
		seenValues = append(seenValues, value)

		if _, updateErr := uw.RawSQLUseCase().Exec(ctx, `
UPDATE fresh_retry_counter
SET value = value + 1, writer_token = 'helper'
WHERE id = 1`); updateErr != nil {
			return updateErr
		}
		if bodyRuns == 1 {
			// Force the competing transaction to win before the helper's first
			// DOLT_COMMIT. The helper must then receive a typed 1213/1205 and
			// replay this entire callback from the competitor's committed state.
			return competitor.Commit(ctx, "fresh retry competitor")
		}
		return nil
	})
	require.NoError(t, err, "a nothing-to-commit result must not be accepted as success")
	require.Equal(t, 2, bodyRuns)
	require.Equal(t, []int{0, 1}, seenValues, "retry must use a fresh snapshot on top of the competitor")

	events, commitErrors := observed.snapshot()
	require.Len(t, commitErrors, 1)
	var serializationErr *mysql.MySQLError
	require.ErrorAs(t, commitErrors[0], &serializationErr)
	require.Contains(t, []uint16{1205, 1213}, serializationErr.Number)
	require.NotContains(t, strings.ToLower(commitErrors[0].Error()), "nothing to commit")
	require.Len(t, events, 8)
	require.Equal(t, "open:0:start", events[0])
	require.Equal(t, "open:0:ok", events[1])
	require.Regexp(t, `^commit:0:mysql:(1205|1213)$`, events[2])
	require.Equal(t, "close:0", events[3], "failed UOW must close before the replacement UOW opens")
	require.Equal(t, "open:1:start", events[4])
	require.Equal(t, "open:1:ok", events[5])
	require.Equal(t, "commit:1:ok", events[6])
	require.Equal(t, "close:1", events[7])

	var finalValue int
	var finalWriter string
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT value, writer_token FROM fresh_retry_counter WHERE id = 1").Scan(&finalValue, &finalWriter))
	require.Equal(t, 2, finalValue)
	require.Equal(t, "helper", finalWriter)

	var competitorCommits, helperCommits int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dolt_log WHERE message = ?", "fresh retry competitor").Scan(&competitorCommits))
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dolt_log WHERE message = ?", "fresh retry helper").Scan(&helperCommits))
	require.Equal(t, 1, competitorCommits)
	require.Equal(t, 1, helperCommits, "the replayed helper mutation must commit exactly once")
}

type freshRetryLeaseState struct {
	status       types.Status
	assignee     string
	title        string
	leaseExpires sql.NullTime
	heartbeatAt  sql.NullTime
	rowLock      int64
}

// TestUOWLeaseParity_RealDoltClaimUpdateHeartbeatAndEightWriters covers the
// boundary PR #4675 introduced: claims and generic updates performed through
// the proxied-server UOW path must interoperate with the primary issueops lease
// path on a real Dolt transaction engine. The eight-writer phase is deliberately
// a correctness race, not a throughput benchmark: all writers mutate the same
// open snapshot and only one durable claim may survive conflict-and-retry.
func TestUOWLeaseParity_RealDoltClaimUpdateHeartbeatAndEightWriters(t *testing.T) {
	port := freshRetryPerfDoltPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	endpoint := proxy.Endpoint{Host: freshRetryPerfDoltHost, Port: port}
	adminDB, err := openDB(ctx, buildDSN(endpoint, "", "root", ""))
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeErr := adminDB.Close(); closeErr != nil {
			t.Errorf("close isolated lease-parity admin connection: %v", closeErr)
		}
	})

	database := freshRetryDatabaseName()
	require.True(t, strings.HasPrefix(database, "beads_perf_lab_"))
	require.LessOrEqual(t, len(database), 64)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, dropErr := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`"); dropErr != nil {
			t.Errorf("drop isolated lease-parity database %s: %v", database, dropErr)
		}
		var remaining int
		if checkErr := adminDB.QueryRowContext(cleanupCtx,
			"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", database).Scan(&remaining); checkErr != nil {
			t.Errorf("verify isolated lease-parity database %s was dropped: %v", database, checkErr)
		} else if remaining != 0 {
			t.Errorf("isolated lease-parity database %s remained after cleanup", database)
		}
	})

	providerIface, err := openAndInitSchema(ctx, endpoint, database, "root", "")
	require.NoError(t, err)
	provider, ok := providerIface.(*doltSQLProvider)
	require.True(t, ok, "real-Dolt provider has unexpected type %T", providerIface)
	provider.db.SetMaxOpenConns(16)
	provider.db.SetMaxIdleConns(16)
	t.Cleanup(func() { require.NoError(t, provider.Close(context.Background())) })

	const (
		parityID = "perf-lease-parity"
		raceID   = "perf-eight-writer-claim"
	)
	require.NoError(t, RunWithFreshUOWRetries(ctx, provider, "lease parity seed", func(ctx context.Context, uw UnitOfWork) error {
		for _, seed := range []struct{ id, title string }{
			{id: parityID, title: "claim update heartbeat parity"},
			{id: raceID, title: "eight writer claim"},
		} {
			if _, createErr := uw.IssueUseCase().CreateIssue(ctx, domain.CreateIssueParams{
				Issue:      &types.Issue{Title: seed.title, IssueType: types.TypeTask, Priority: 2},
				ExplicitID: seed.id,
			}, "seed"); createErr != nil {
				return createErr
			}
		}
		return nil
	}))

	claimCtx := issueops.WithLeaseTTL(ctx, 30*time.Minute)
	require.NoError(t, RunWithFreshUOWRetries(claimCtx, provider, "lease parity claim", func(ctx context.Context, uw UnitOfWork) error {
		_, claimErr := uw.IssueUseCase().ClaimIssue(ctx, parityID, "alice")
		return claimErr
	}))
	claimed := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.Equal(t, types.StatusInProgress, claimed.status)
	require.Equal(t, "alice", claimed.assignee)
	require.True(t, claimed.leaseExpires.Valid, "UOW claim must stamp lease_expires_at")
	require.True(t, claimed.heartbeatAt.Valid, "UOW claim must stamp heartbeat_at")
	require.NotZero(t, claimed.rowLock, "UOW claim must stamp row_lock")

	require.NoError(t, RunWithFreshUOWRetries(ctx, provider, "lease parity unrelated update", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().UpdateIssue(ctx, parityID, map[string]any{"title": "edited without stealing lease"}, "editor")
	}))
	edited := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.Equal(t, "edited without stealing lease", edited.title)
	require.True(t, edited.leaseExpires.Valid && edited.leaseExpires.Time.Equal(claimed.leaseExpires.Time),
		"unrelated UOW update changed lease expiry: claimed=%v edited=%v", claimed.leaseExpires, edited.leaseExpires)
	require.True(t, edited.heartbeatAt.Valid && edited.heartbeatAt.Time.Equal(claimed.heartbeatAt.Time),
		"unrelated UOW update changed heartbeat: claimed=%v edited=%v", claimed.heartbeatAt, edited.heartbeatAt)
	require.NotEqual(t, claimed.rowLock, edited.rowLock, "unrelated UOW update must rewrite row_lock")

	// The heartbeat path is the primary issueops implementation. Running it in
	// a fresh UOW proves the proxied claim/update writes share its row contract.
	heartbeatCtx := issueops.WithLeaseTTL(ctx, 2*time.Hour)
	require.NoError(t, RunWithFreshUOWRetries(heartbeatCtx, provider, "lease parity heartbeat", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().HeartbeatIssue(ctx, parityID, "alice")
	}))
	heartbeated := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.True(t, heartbeated.leaseExpires.Valid && heartbeated.leaseExpires.Time.After(edited.leaseExpires.Time),
		"heartbeat did not extend lease: edited=%v heartbeat=%v", edited.leaseExpires, heartbeated.leaseExpires)
	// Dolt's schema stores these timestamps at one-second precision, so two
	// operations in the same second may compare equal. The extended expiry and
	// rewritten row_lock prove the heartbeat landed; its timestamp must never go
	// backwards.
	require.True(t, heartbeated.heartbeatAt.Valid && !heartbeated.heartbeatAt.Time.Before(edited.heartbeatAt.Time),
		"heartbeat timestamp moved backwards: edited=%v heartbeat=%v", edited.heartbeatAt, heartbeated.heartbeatAt)
	require.NotEqual(t, edited.rowLock, heartbeated.rowLock, "heartbeat must rewrite row_lock")

	require.NoError(t, RunWithFreshUOWRetries(ctx, provider, "lease parity transfer", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().UpdateIssue(ctx, parityID, map[string]any{"assignee": "bob"}, "dispatcher")
	}))
	transferred := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.Equal(t, types.StatusInProgress, transferred.status)
	require.Equal(t, "bob", transferred.assignee)
	require.True(t, transferred.leaseExpires.Valid, "ownership transfer must stamp a fresh lease")
	require.True(t, transferred.heartbeatAt.Valid, "ownership transfer must stamp a fresh heartbeat")
	require.NotEqual(t, heartbeated.rowLock, transferred.rowLock, "ownership transfer must rewrite row_lock")

	err = RunWithFreshUOWRetries(ctx, provider, "old owner heartbeat must fail", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().HeartbeatIssue(ctx, parityID, "alice")
	})
	require.ErrorIs(t, err, storage.ErrAlreadyClaimed)

	require.NoError(t, RunWithFreshUOWRetries(heartbeatCtx, provider, "new owner heartbeat", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().HeartbeatIssue(ctx, parityID, "bob")
	}))
	rearmed := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.True(t, rearmed.leaseExpires.Valid, "new owner heartbeat must preserve a live lease")
	require.True(t, rearmed.heartbeatAt.Valid, "new owner heartbeat must refresh the heartbeat")
	require.NotEqual(t, transferred.rowLock, rearmed.rowLock, "new owner heartbeat must rewrite row_lock")

	require.NoError(t, RunWithFreshUOWRetries(ctx, provider, "lease parity close update", func(ctx context.Context, uw UnitOfWork) error {
		return uw.IssueUseCase().UpdateIssue(ctx, parityID, map[string]any{"status": string(types.StatusClosed)}, "closer")
	}))
	closed := freshRetryReadLeaseState(t, ctx, provider.db, parityID)
	require.Equal(t, types.StatusClosed, closed.status)
	require.False(t, closed.leaseExpires.Valid, "close update must clear the lease")
	require.False(t, closed.heartbeatAt.Valid, "close update must clear the heartbeat")
	require.NotEqual(t, rearmed.rowLock, closed.rowLock, "close update must rewrite row_lock")

	// Eight different agents claim the same issue from the same committed
	// snapshot. The barrier is inside each first callback, before Commit, so all
	// eight initial UPDATEs have succeeded when commits are released together.
	const writers = 8
	type writerResult struct {
		actor    string
		attempts int
		err      error
	}
	arrived := make(chan string, writers)
	release := make(chan struct{})
	results := make(chan writerResult, writers)
	for i := 0; i < writers; i++ {
		actor := fmt.Sprintf("agent-%d", i)
		go func() {
			attempts := 0
			runErr := RunWithFreshUOWRetries(ctx, provider, "eight writer claim "+actor, func(ctx context.Context, uw UnitOfWork) error {
				attempts++
				_, claimErr := uw.IssueUseCase().ClaimIssue(ctx, raceID, actor)
				if attempts == 1 {
					arrived <- actor
					if claimErr == nil {
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return claimErr
			})
			results <- writerResult{actor: actor, attempts: attempts, err: runErr}
		}()
	}

	arrivalTimer := time.NewTimer(20 * time.Second)
	arrivals := make(map[string]struct{}, writers)
	arrivalTimedOut := false
	for len(arrivals) < writers && !arrivalTimedOut {
		select {
		case actor := <-arrived:
			arrivals[actor] = struct{}{}
		case <-arrivalTimer.C:
			arrivalTimedOut = true
		}
	}
	if !arrivalTimer.Stop() && !arrivalTimedOut {
		<-arrivalTimer.C
	}
	close(release)

	var winner string
	for i := 0; i < writers; i++ {
		result := <-results
		if result.err == nil {
			require.Empty(t, winner, "multiple durable winners: %s and %s", winner, result.actor)
			winner = result.actor
			require.Equal(t, 1, result.attempts, "winning first commit should not replay")
			continue
		}
		require.ErrorIs(t, result.err, storage.ErrAlreadyClaimed, "loser %s returned %v", result.actor, result.err)
		require.GreaterOrEqual(t, result.attempts, 2, "loser %s did not retry from a fresh UOW", result.actor)
	}
	require.False(t, arrivalTimedOut, "only %d/%d first-attempt claims reached the commit barrier", len(arrivals), writers)
	require.NotEmpty(t, winner, "no durable winner from eight simultaneous writers")

	raceState := freshRetryReadLeaseState(t, ctx, provider.db, raceID)
	require.Equal(t, types.StatusInProgress, raceState.status)
	require.Equal(t, winner, raceState.assignee)
	require.True(t, raceState.leaseExpires.Valid)
	require.True(t, raceState.heartbeatAt.Valid)
	require.NotZero(t, raceState.rowLock)

	var claimEvents, claimCommits int
	require.NoError(t, provider.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'claimed'", raceID).Scan(&claimEvents))
	require.NoError(t, provider.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dolt_log WHERE message LIKE 'eight writer claim %'").Scan(&claimCommits))
	require.Equal(t, 1, claimEvents, "failed claim transactions leaked duplicate events")
	require.Equal(t, 1, claimCommits, "eight-writer race must produce exactly one claim commit")
}

func freshRetryReadLeaseState(t *testing.T, ctx context.Context, db *sql.DB, id string) freshRetryLeaseState {
	t.Helper()
	var state freshRetryLeaseState
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT status, COALESCE(assignee, ''), title,
		       lease_expires_at, heartbeat_at, row_lock
		FROM issues WHERE id = ?`, id).Scan(
		&state.status, &state.assignee, &state.title,
		&state.leaseExpires, &state.heartbeatAt, &state.rowLock,
	))
	return state
}

func freshRetryPerfDoltPort(t *testing.T) int {
	t.Helper()
	raw, ok := os.LookupEnv("BEADS_PERF_DOLT_PORT")
	if !ok {
		t.Skip("set BEADS_PERF_DOLT_PORT to an isolated loopback Dolt SQL server port")
	}
	require.NotEmpty(t, raw)
	for _, digit := range raw {
		require.GreaterOrEqual(t, digit, '0', "BEADS_PERF_DOLT_PORT must contain decimal digits only")
		require.LessOrEqual(t, digit, '9', "BEADS_PERF_DOLT_PORT must contain decimal digits only")
	}
	port, err := strconv.Atoi(raw)
	require.NoError(t, err)
	require.Greater(t, port, 0)
	require.LessOrEqual(t, port, 65535)
	require.NotEqual(t, 3307, port, "port 3307 is forbidden for this isolated performance test")

	ip := net.ParseIP(freshRetryPerfDoltHost)
	require.NotNil(t, ip)
	require.True(t, ip.IsLoopback(), "real-Dolt test endpoint must be numeric loopback")
	return port
}

func freshRetryDatabaseName() string {
	return fmt.Sprintf(
		"beads_perf_lab_fresh_retry_%s_%s",
		strconv.FormatInt(int64(os.Getpid()), 36),
		strconv.FormatInt(time.Now().UnixNano(), 36),
	)
}

func freshRetryCounterValue(ctx context.Context, uw UnitOfWork) (int, error) {
	return freshRetryScalarInt(ctx, uw, "SELECT value FROM fresh_retry_counter WHERE id = 1")
}

func freshRetryScalarInt(ctx context.Context, uw UnitOfWork, query string) (int, error) {
	result, err := uw.RawSQLUseCase().Query(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return 0, fmt.Errorf("expected one counter value, got %#v", result.Rows)
	}
	value, err := strconv.Atoi(fmt.Sprint(result.Rows[0][0]))
	if err != nil {
		return 0, fmt.Errorf("parse counter value %v: %w", result.Rows[0][0], err)
	}
	return value, nil
}
