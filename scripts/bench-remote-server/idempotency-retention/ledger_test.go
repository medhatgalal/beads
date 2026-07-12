package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testBinding() Binding {
	return Binding{CellID: "test-cell", TeamID: "test-team", ProjectID: "project-a", LedgerEpoch: 1}
}

func testPolicy() Policy {
	return Policy{
		MaxProducers:           8,
		MaxReceipts:            64,
		MaxReceiptsPerProducer: 16,
		KeepRecentPerProducer:  2,
		MaxOutcomeBytes:        1024,
		MinimumRetryHorizon:    0,
	}
}

func testLedger(t *testing.T, policy Policy) (*canonicalStore, *VerificationRing, *ledger) {
	t.Helper()
	store, err := openCanonicalStore(context.Background(), filepath.Join(t.TempDir(), "canonical.db"), testBinding(), policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	keys, err := NewVerificationRing(3, 1, bytes.Repeat([]byte{0x31}, 32))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := newLedger(store, keys)
	if err != nil {
		t.Fatal(err)
	}
	return store, keys, ledger
}

func signedRequest(t *testing.T, keys *VerificationRing, producer string, epoch, sequence uint64, body string) Request {
	t.Helper()
	request := Request{
		ProjectID:     "project-a",
		ProducerID:    producer,
		SubjectHash:   subjectHashFor(producer),
		LedgerEpoch:   1,
		ProducerEpoch: epoch,
		Sequence:      sequence,
		RequestHash:   sha256.Sum256([]byte(body)),
	}
	request, err := keys.Sign(request)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestExecuteReplayConflictGapAndCompactedGone(t *testing.T) {
	ctx := context.Background()
	policy := testPolicy()
	policy.KeepRecentPerProducer = 0
	store, keys, ledger := testLedger(t, policy)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := createBusinessProbeTable(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	request := signedRequest(t, keys, "producer-a", 1, 1, "body-one")
	var mutations int
	mutate := func(ctx context.Context, tx *sql.Tx) error {
		mutations++
		_, err := tx.ExecContext(ctx, `
INSERT INTO business_probe(operation_key, mutation_count) VALUES('one', 1)`)
		return err
	}
	decision, err := ledger.Execute(ctx, request, Outcome{Code: "created", Payload: []byte(`{"id":"one"}`)}, now, mutate)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Disposition != DispositionExecuted || mutations != 1 {
		t.Fatalf("first decision=%s mutations=%d", decision.Disposition, mutations)
	}
	decision, err = ledger.Execute(ctx, request, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Disposition != DispositionReplay || decision.Outcome.Code != "created" || mutations != 1 {
		t.Fatalf("replay=%+v mutations=%d", decision, mutations)
	}
	conflict := request
	conflict.RequestHash = sha256.Sum256([]byte("different"))
	conflict, err = keys.Sign(conflict)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ledger.Execute(ctx, conflict, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionConflict || mutations != 1 {
		t.Fatalf("conflict=%+v err=%v mutations=%d", decision, err, mutations)
	}
	gap := signedRequest(t, keys, "producer-a", 1, 3, "gap")
	decision, err = ledger.Execute(ctx, gap, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionGap || mutations != 1 {
		t.Fatalf("gap=%+v err=%v mutations=%d", decision, err, mutations)
	}
	if _, err := store.Compact(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	decision, err = ledger.Execute(ctx, request, Outcome{Code: "wrong"}, now.Add(2*time.Second), func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionGone || decision.Reason != "sequence_compacted" || mutations != 1 {
		t.Fatalf("compacted retry=%+v err=%v mutations=%d", decision, err, mutations)
	}
	if err := store.QuickCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityBackpressuresUntilRetryHorizonExpires(t *testing.T) {
	ctx := context.Background()
	policy := testPolicy()
	policy.MaxReceipts = 2
	policy.MaxReceiptsPerProducer = 2
	policy.KeepRecentPerProducer = 0
	policy.MinimumRetryHorizon = time.Hour
	store, keys, ledger := testLedger(t, policy)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		request := signedRequest(t, keys, "producer-a", 1, sequence, fmt.Sprintf("body-%d", sequence))
		if decision, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, now, nil); err != nil || decision.Disposition != DispositionExecuted {
			t.Fatalf("seed sequence %d decision=%+v err=%v", sequence, decision, err)
		}
	}
	third := signedRequest(t, keys, "producer-a", 1, 3, "body-3")
	var mutations int
	_, err := ledger.Execute(ctx, third, Outcome{Code: "ok"}, now.Add(time.Minute), func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if !errors.Is(err, ErrCapacity) || mutations != 0 {
		t.Fatalf("capacity err=%v mutations=%d", err, mutations)
	}
	_, next, floor, err := store.head(ctx, "project-a", "producer-a")
	if err != nil || next != 3 || floor != 0 {
		t.Fatalf("head after capacity next=%d floor=%d err=%v", next, floor, err)
	}
	decision, err := ledger.Execute(ctx, third, Outcome{Code: "ok"}, now.Add(2*time.Hour), func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionExecuted || mutations != 1 {
		t.Fatalf("post-horizon decision=%+v err=%v mutations=%d", decision, err, mutations)
	}
	if err := store.QuickCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBusinessMutationReceiptAndSequenceRollbackTogether(t *testing.T) {
	ctx := context.Background()
	store, keys, ledger := testLedger(t, testPolicy())
	now := time.Now().UTC()
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := createBusinessProbeTable(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	request := signedRequest(t, keys, "producer-a", 1, 1, "rollback")
	_, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, now, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO business_probe(operation_key, mutation_count) VALUES('rollback', 1)`); err != nil {
			return err
		}
		return errors.New("injected mutation failure")
	})
	if err == nil {
		t.Fatal("injected mutation failure unexpectedly committed")
	}
	var businessRows int64
	if err := store.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM business_probe WHERE operation_key='rollback'`).Scan(&businessRows); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, next, floor, err := store.head(ctx, "project-a", "producer-a")
	if err != nil || businessRows != 0 || stats.Receipts != 0 || next != 1 || floor != 0 {
		t.Fatalf("business=%d stats=%+v next=%d floor=%d err=%v", businessRows, stats, next, floor, err)
	}
	decision, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, now, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
INSERT INTO business_probe(operation_key, mutation_count) VALUES('rollback', 1)`)
		return err
	})
	if err != nil || decision.Disposition != DispositionExecuted {
		t.Fatalf("retry decision=%+v err=%v", decision, err)
	}
	if err := store.QuickCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionNeverCrossesANewerReceiptInThePrefix(t *testing.T) {
	ctx := context.Background()
	policy := testPolicy()
	policy.KeepRecentPerProducer = 0
	policy.MinimumRetryHorizon = time.Hour
	store, keys, ledger := testLedger(t, policy)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, base); err != nil {
		t.Fatal(err)
	}
	commitTimes := []time.Time{base, base.Add(2 * time.Hour), base}
	for index, committedAt := range commitTimes {
		sequence := uint64(index + 1)
		request := signedRequest(t, keys, "producer-a", 1, sequence, fmt.Sprintf("body-%d", sequence))
		decision, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, committedAt, nil)
		if err != nil || decision.Disposition != DispositionExecuted {
			t.Fatalf("sequence %d decision=%+v err=%v", sequence, decision, err)
		}
	}
	if _, err := store.Compact(ctx, base.Add(90*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	_, next, floor, err := store.head(ctx, "project-a", "producer-a")
	if err != nil || next != 4 || floor != 1 {
		t.Fatalf("next=%d floor=%d err=%v", next, floor, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Receipts != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if err := store.QuickCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestProducerEpochFenceAndBoundedVerificationRing(t *testing.T) {
	ctx := context.Background()
	store, keys, ledger := testLedger(t, testPolicy())
	now := time.Now().UTC()
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	old := signedRequest(t, keys, "producer-a", 1, 1, "old")
	if _, err := ledger.Execute(ctx, old, Outcome{Code: "ok"}, now, nil); err != nil {
		t.Fatal(err)
	}
	for epoch := uint64(2); epoch <= 4; epoch++ {
		if err := keys.Rotate(epoch, bytes.Repeat([]byte{byte(0x40 + epoch)}, 32)); err != nil {
			t.Fatal(err)
		}
	}
	if err := keys.Verify(old); !errors.Is(err, ErrVerification) {
		t.Fatalf("retired key verification err=%v", err)
	}
	if err := store.AdvanceProducerEpoch(ctx, "project-a", "producer-a", 1, 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	old, err := keys.Sign(old)
	if err != nil {
		t.Fatal(err)
	}
	var mutations int
	decision, err := ledger.Execute(ctx, old, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionGone || decision.Reason != "producer_epoch_retired" || mutations != 0 {
		t.Fatalf("retired epoch=%+v err=%v mutations=%d", decision, err, mutations)
	}
	future := signedRequest(t, keys, "producer-a", 3, 1, "future")
	decision, err = ledger.Execute(ctx, future, Outcome{Code: "wrong"}, now, nil)
	if err != nil || decision.Disposition != DispositionFenced {
		t.Fatalf("future epoch=%+v err=%v", decision, err)
	}
	spoofed := signedRequest(t, keys, "producer-a", 2, 1, "current")
	spoofed.SubjectHash = subjectHashFor("another-subject")
	spoofed, err = keys.Sign(spoofed)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ledger.Execute(ctx, spoofed, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionUnauthorized || mutations != 0 {
		t.Fatalf("spoofed subject=%+v err=%v mutations=%d", decision, err, mutations)
	}
	current := signedRequest(t, keys, "producer-a", 2, 1, "current")
	decision, err = ledger.Execute(ctx, current, Outcome{Code: "ok"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionExecuted || mutations != 1 {
		t.Fatalf("current epoch=%+v err=%v mutations=%d", decision, err, mutations)
	}
	active, oldest, count := keys.Bounds()
	if active != 4 || oldest != 2 || count != 3 {
		t.Fatalf("ring active=%d oldest=%d count=%d", active, oldest, count)
	}
}

func TestConcurrentSameRequestMutatesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store, keys, ledger := testLedger(t, testPolicy())
	now := time.Now().UTC()
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	request := signedRequest(t, keys, "producer-a", 1, 1, "same")
	var mutationCalls atomic.Int64
	const writers = 8
	decisions := make(chan Disposition, writers)
	errorsSeen := make(chan error, writers)
	var wait sync.WaitGroup
	for range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			decision, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, now, func(context.Context, *sql.Tx) error {
				mutationCalls.Add(1)
				return nil
			})
			if err != nil {
				errorsSeen <- err
				return
			}
			decisions <- decision.Disposition
		}()
	}
	wait.Wait()
	close(decisions)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent execute: %v", err)
	}
	var executed, replayed int
	for decision := range decisions {
		switch decision {
		case DispositionExecuted:
			executed++
		case DispositionReplay:
			replayed++
		default:
			t.Errorf("unexpected concurrent disposition %q", decision)
		}
	}
	if executed != 1 || replayed != writers-1 || mutationCalls.Load() != 1 {
		t.Fatalf("executed=%d replayed=%d mutations=%d", executed, replayed, mutationCalls.Load())
	}
}

func TestProducerAndBindingBoundsFailClosed(t *testing.T) {
	ctx := context.Background()
	policy := testPolicy()
	policy.MaxProducers = 2
	path := filepath.Join(t.TempDir(), "canonical.db")
	store, err := openCanonicalStore(ctx, path, testBinding(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterProducer(ctx, "project-b", "foreign", subjectHashFor("foreign"), 1, 1, time.Now().UTC()); err == nil {
		t.Fatal("foreign project registered in the repository-bound store")
	}
	keys, err := NewVerificationRing(3, 1, bytes.Repeat([]byte{0x31}, 32))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := newLedger(store, keys)
	if err != nil {
		t.Fatal(err)
	}
	foreign := signedRequest(t, keys, "foreign", 1, 1, "foreign")
	foreign.ProjectID = "project-b"
	foreign, err = keys.Sign(foreign)
	if err != nil {
		t.Fatal(err)
	}
	var foreignMutations int
	decision, err := ledger.Execute(ctx, foreign, Outcome{Code: "wrong"}, time.Now().UTC(), func(context.Context, *sql.Tx) error {
		foreignMutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionUnauthorized || foreignMutations != 0 {
		t.Fatalf("foreign project decision=%+v err=%v mutations=%d", decision, err, foreignMutations)
	}
	for index := 0; index < 2; index++ {
		if err := store.RegisterProducer(ctx, "project-a", producerName(index), subjectHashFor(producerName(index)), 1, 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RegisterProducer(ctx, "project-a", "overflow", subjectHashFor("overflow"), 1, 1, time.Now().UTC()); !errors.Is(err, ErrProducerLimit) {
		t.Fatalf("producer limit err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wrongBinding := testBinding()
	wrongBinding.TeamID = "another-team"
	if reopened, err := openCanonicalStore(ctx, path, wrongBinding, policy); err == nil {
		_ = reopened.Close()
		t.Fatal("store opened under another team binding")
	}
	wrongPolicy := policy
	wrongPolicy.MaxReceipts++
	if reopened, err := openCanonicalStore(ctx, path, testBinding(), wrongPolicy); err == nil {
		_ = reopened.Close()
		t.Fatal("store opened under another persisted policy")
	}
}

func TestLedgerEpochRotationReclaimsAllTombstonesAndFencesEveryOldRequest(t *testing.T) {
	ctx := context.Background()
	store, keys, ledger := testLedger(t, testPolicy())
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
		t.Fatal(err)
	}
	old := signedRequest(t, keys, "producer-a", 1, 1, "old-ledger-request")
	if decision, err := ledger.Execute(ctx, old, Outcome{Code: "ok"}, now, nil); err != nil || decision.Disposition != DispositionExecuted {
		t.Fatalf("seed decision=%+v err=%v", decision, err)
	}
	if err := store.RotateLedgerEpoch(ctx, 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Producers != 0 || stats.Receipts != 0 {
		t.Fatalf("rotated stats=%+v err=%v", stats, err)
	}
	var mutations int
	old, err = keys.Sign(old)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ledger.Execute(ctx, old, Outcome{Code: "wrong"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionGone || decision.Reason != "ledger_epoch_retired" ||
		decision.CurrentLedgerEpoch != 2 || mutations != 0 {
		t.Fatalf("old ledger retry=%+v err=%v mutations=%d", decision, err, mutations)
	}
	if err := store.RegisterProducer(ctx, "project-a", "stale-registration", subjectHashFor("stale-registration"), 1, 1, now); err == nil {
		t.Fatal("stale control plane registered a producer under a retired ledger epoch")
	}
	if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("replacement-subject"), 2, 1, now); err != nil {
		t.Fatal(err)
	}
	current := signedRequest(t, keys, "producer-a", 1, 1, "new-ledger-request")
	current.LedgerEpoch = 2
	current.SubjectHash = subjectHashFor("replacement-subject")
	current, err = keys.Sign(current)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ledger.Execute(ctx, current, Outcome{Code: "ok"}, now, func(context.Context, *sql.Tx) error {
		mutations++
		return nil
	})
	if err != nil || decision.Disposition != DispositionExecuted || mutations != 1 {
		t.Fatalf("new ledger decision=%+v err=%v mutations=%d", decision, err, mutations)
	}
	future := current
	future.LedgerEpoch = 3
	future.Sequence = 2
	future.RequestHash = sha256.Sum256([]byte("future-ledger"))
	future, err = keys.Sign(future)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ledger.Execute(ctx, future, Outcome{Code: "wrong"}, now, nil)
	if err != nil || decision.Disposition != DispositionFenced || decision.Reason != "ledger_epoch_not_activated" {
		t.Fatalf("future ledger decision=%+v err=%v", decision, err)
	}
	if reopened, err := openCanonicalStore(ctx, store.path, testBinding(), testPolicy()); err == nil {
		_ = reopened.Close()
		t.Fatal("rotated store reopened with stale expected ledger epoch")
	}
	currentBinding := testBinding()
	currentBinding.LedgerEpoch = 2
	reopened, err := openCanonicalStore(ctx, store.path, currentBinding, testPolicy())
	if err != nil {
		t.Fatalf("reopen with current ledger epoch: %v", err)
	}
	_ = reopened.Close()
	if err := store.QuickCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func crashPolicy() Policy {
	policy := testPolicy()
	policy.KeepRecentPerProducer = 2
	return policy
}

func TestCompactionSurvivesProcessCrashAtEveryTransactionBoundary(t *testing.T) {
	for _, test := range []struct {
		phase        CompactPhase
		wantFloor    uint64
		wantReceipts int64
	}{
		{phase: CompactAfterWatermarks, wantFloor: 0, wantReceipts: 10},
		{phase: CompactAfterDeletes, wantFloor: 0, wantReceipts: 10},
		{phase: CompactBeforeCommit, wantFloor: 0, wantReceipts: 10},
		{phase: CompactAfterCommit, wantFloor: 8, wantReceipts: 2},
	} {
		t.Run(string(test.phase), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "crash.db")
			store, err := openCanonicalStore(ctx, path, testBinding(), crashPolicy())
			if err != nil {
				t.Fatal(err)
			}
			keys, err := NewVerificationRing(3, 1, bytes.Repeat([]byte{0x31}, 32))
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := newLedger(store, keys)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			if err := store.RegisterProducer(ctx, "project-a", "producer-a", subjectHashFor("producer-a"), 1, 1, now); err != nil {
				t.Fatal(err)
			}
			for sequence := uint64(1); sequence <= 10; sequence++ {
				request := signedRequest(t, keys, "producer-a", 1, sequence, fmt.Sprintf("body-%d", sequence))
				if _, err := ledger.Execute(ctx, request, Outcome{Code: "ok"}, now, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestCompactionCrashHelper$")
			command.Env = append(os.Environ(),
				"BEADS_RETENTION_CRASH_HELPER=1",
				"BEADS_RETENTION_CRASH_PATH="+path,
				"BEADS_RETENTION_CRASH_PHASE="+string(test.phase))
			if err := command.Run(); err == nil {
				t.Fatal("crash helper unexpectedly exited successfully")
			}
			store, err = openCanonicalStore(ctx, path, testBinding(), crashPolicy())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, _, floor, err := store.head(ctx, "project-a", "producer-a")
			if err != nil {
				t.Fatal(err)
			}
			stats, err := store.Stats(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if floor != test.wantFloor || stats.Receipts != test.wantReceipts {
				t.Fatalf("floor=%d receipts=%d want floor=%d receipts=%d", floor, stats.Receipts, test.wantFloor, test.wantReceipts)
			}
			if err := store.QuickCheck(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionCrashHelper(t *testing.T) {
	if os.Getenv("BEADS_RETENTION_CRASH_HELPER") != "1" {
		return
	}
	store, err := openCanonicalStore(context.Background(), os.Getenv("BEADS_RETENTION_CRASH_PATH"), testBinding(), crashPolicy())
	if err != nil {
		os.Exit(80)
	}
	phase := CompactPhase(os.Getenv("BEADS_RETENTION_CRASH_PHASE"))
	_, _ = store.Compact(context.Background(), time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC), func(current CompactPhase) error {
		if current == phase {
			os.Exit(90)
		}
		return nil
	})
	os.Exit(81)
}
