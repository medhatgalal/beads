package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	storagepkg "github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const testProjectID = "11111111-2222-4333-8444-555555555555"

const (
	testDatabase = "beads_perf_lab_gateway_test"
	testSubject  = "test-agent"
)

var (
	testToken     = []byte("0123456789abcdef0123456789abcdef")
	testStableKey = []byte("abcdef0123456789abcdef0123456789")
)

func openTestOperationQueue(ctx context.Context, path string) (*operationQueue, error) {
	return openOperationQueue(ctx, path, queueBinding{
		ProjectID: testProjectID, Database: testDatabase,
		IdempotencyKeyID: hmacHex(testStableKey, "binding\x00idempotency-v1"),
		SubjectID:        hmacHex(testStableKey, "subject\x00"+testSubject),
	})
}

func newTestGatewayService(ctx context.Context, token []byte, queue *operationQueue, backend operationBackend) (*gatewayService, error) {
	service, err := newGatewayService(ctx, testProjectID, gatewayAuth{
		BearerToken: token, StableKey: testStableKey, Subject: testSubject,
	}, queue, backend)
	if service != nil {
		service.allowLegacyOperations = true
	}
	return service, err
}

type fakeBackend struct {
	mu               sync.Mutex
	receipts         map[string]*doltReceipt
	executes         atomic.Int64
	execErr          error
	lookupErr        error
	lookupErrByKey   map[string]error
	block            <-chan struct{}
	execFailures     atomic.Int64
	lookupFailures   atomic.Int64
	durabilityErr    error
	durabilityChecks atomic.Int64
}

type memoryCanonicalMetadata struct {
	values  map[string]string
	queries []string
}

func (m *memoryCanonicalMetadata) RawSQLUseCase() domain.RawSQLUseCase { return m }

func (m *memoryCanonicalMetadata) Exec(_ context.Context, query string, args ...any) (int64, error) {
	m.queries = append(m.queries, query)
	if strings.Contains(strings.ToUpper(query), "REPLACE") {
		return 0, errors.New("replace is forbidden")
	}
	if len(args) != 2 {
		return 0, errors.New("expected metadata key and value")
	}
	key, keyOK := args[0].(string)
	value, valueOK := args[1].(string)
	if !keyOK || !valueOK {
		return 0, errors.New("metadata key and value must be strings")
	}
	if _, exists := m.values[key]; exists {
		return 0, errors.New("duplicate key")
	}
	m.values[key] = value
	return 1, nil
}

func (m *memoryCanonicalMetadata) Query(_ context.Context, _ string, args ...any) (*domain.RawSQLResult, error) {
	if len(args) != 1 {
		return nil, errors.New("expected metadata key")
	}
	key, ok := args[0].(string)
	if !ok {
		return nil, errors.New("metadata key must be a string")
	}
	value, exists := m.values[key]
	if !exists {
		return &domain.RawSQLResult{}, nil
	}
	return &domain.RawSQLResult{Rows: [][]any{{value}}}, nil
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{receipts: make(map[string]*doltReceipt), lookupErrByKey: make(map[string]error)}
}

func (f *fakeBackend) Attestation(context.Context) (labAttestation, error) {
	return labAttestation{
		Version: 1, Environment: "synthetic", LabID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		DatabaseName: testDatabase, ProjectID: testProjectID,
	}, nil
}
func (f *fakeBackend) Ping(context.Context) (any, error) { return map[string]any{"status": "ok"}, nil }
func (f *fakeBackend) Show(context.Context, string) (any, error) {
	return map[string]any{"id": "test-1"}, nil
}
func (f *fakeBackend) List(context.Context, int) (any, error)  { return []any{}, nil }
func (f *fakeBackend) Ready(context.Context, int) (any, error) { return []any{}, nil }
func (f *fakeBackend) PoolStats() any                          { return map[string]any{"max": 8} }
func (f *fakeBackend) Close(context.Context) error             { return nil }

func (f *fakeBackend) VerifyTerminalDurability(_ context.Context, op operation, receipt *doltReceipt) error {
	f.durabilityChecks.Add(1)
	if receipt == nil {
		return errors.New("canonical outcome is required")
	}
	if err := validateReceipt(*receipt, op); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.durabilityErr
}

func (f *fakeBackend) Execute(ctx context.Context, op operation) (*doltReceipt, error) {
	f.executes.Add(1)
	if decrementIfPositive(&f.execFailures) {
		return nil, errors.New("connection reset during operation")
	}
	if f.execErr != nil {
		return nil, f.execErr
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if receipt := f.receipts[op.KeyHash]; receipt != nil {
		copy := *receipt
		if err := validateReceipt(copy, op); err != nil {
			return nil, err
		}
		copy.Reconciled = true
		return &copy, nil
	}
	result, _ := json.Marshal(map[string]any{"operation_id": op.ID, "ok": true})
	status := statusSucceeded
	eventPayload, _ := json.Marshal(durableOutboxPayload{
		OperationID: op.ID, Kind: op.Kind, Status: status,
		AffectedIDs: []string{"test-issue"}, ResultHash: digestHex(result),
	})
	version := 1
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		version = 2
	}
	receipt := &doltReceipt{
		Version: version, OperationID: op.ID, ProjectID: op.ProjectID,
		KeyHash: op.KeyHash, SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Status: status, Result: result, CommittedAt: time.Now().UTC(),
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0, EventType: op.Kind + ".succeeded", Payload: eventPayload, CreatedAt: time.Now().UTC()},
	}
	f.receipts[op.KeyHash] = receipt
	return receipt, nil
}

func (f *fakeBackend) LookupReceipt(_ context.Context, op operation) (*doltReceipt, error) {
	if decrementIfPositive(&f.lookupFailures) {
		return nil, errors.New("receipt lookup temporarily unavailable")
	}
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.lookupErrByKey[op.KeyHash]; err != nil {
		return nil, err
	}
	receipt := f.receipts[op.KeyHash]
	if receipt == nil {
		return nil, nil
	}
	copy := *receipt
	if err := validateReceipt(copy, op); err != nil {
		return nil, err
	}
	return &copy, nil
}

func (f *fakeBackend) LegacyOutcomeExists(_ context.Context, projectID, keyHash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	receipt := f.receipts[keyHash]
	if receipt == nil {
		return false, nil
	}
	if receipt.Version != 1 || receipt.ProjectID != projectID || receipt.KeyHash != keyHash ||
		receipt.OperationID == "" || receipt.SubjectHash == "" || receipt.RequestHash == "" || receipt.Kind == "" {
		return false, errReceiptIdentityMismatch
	}
	op := operation{
		ID: receipt.OperationID, ProjectID: receipt.ProjectID, KeyHash: receipt.KeyHash,
		SubjectHash: receipt.SubjectHash, RequestHash: receipt.RequestHash, Kind: receipt.Kind,
	}
	if err := validateReceipt(*receipt, op); err != nil {
		return false, err
	}
	return true, nil
}

func (f *fakeBackend) PersistPermanentFailure(
	_ context.Context, op operation, code, message string,
) (*doltReceipt, error) {
	if !isTerminalIdempotencyNamespace(op.KeyHash) {
		return nil, errors.New("terminal-v2 idempotency required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing := f.receipts[op.KeyHash]; existing != nil {
		copy := *existing
		if err := validateReceipt(copy, op); err != nil {
			return nil, err
		}
		return &copy, nil
	}
	result, _ := json.Marshal(map[string]string{"status": statusFailed, "error_code": code})
	eventPayload, _ := json.Marshal(durableOutboxPayload{
		OperationID: op.ID, Kind: op.Kind, Status: statusFailed,
		ResultHash: digestHex(result), ErrorCode: code,
	})
	now := time.Now().UTC()
	outcome := &doltReceipt{
		Version: 2, OperationID: op.ID, ProjectID: op.ProjectID,
		KeyHash: op.KeyHash, SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Status: statusFailed, Result: result, ErrorCode: code, ErrorMessage: message,
		CommittedAt: now,
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0, EventType: op.Kind + ".failed",
			Payload: eventPayload, CreatedAt: now},
	}
	f.receipts[op.KeyHash] = outcome
	return outcome, nil
}

func decrementIfPositive(value *atomic.Int64) bool {
	for {
		current := value.Load()
		if current <= 0 {
			return false
		}
		if value.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

func TestOperationQueueConcurrentDuplicateAdmission(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	input := operation{ProjectID: testProjectID, KeyHash: "concurrent-key", SubjectHash: "subject", RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] concurrent"}`)}
	const workers = 32
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := queue.admit(ctx, input)
			if err != nil {
				errs <- err
				return
			}
			ids <- op.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent admit: %v", err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("duplicate admission IDs differ: %q vs %q", id, first)
		}
	}
}

func TestOperationQueueIdempotencyAndOutbox(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	input := operation{
		ProjectID: testProjectID, KeyHash: "key-a", SubjectHash: "subject-a",
		RequestHash: "request-a", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] test"}`),
	}
	first, err := queue.admit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.admit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || !second.IdempotentHit {
		t.Fatalf("duplicate admission = %#v, first = %#v", second, first)
	}
	conflict := input
	conflict.RequestHash = "request-b"
	if _, err := queue.admit(ctx, conflict); err != errIdempotencyConflict {
		t.Fatalf("conflicting admission error = %v", err)
	}
	conflict = input
	conflict.SubjectHash = "subject-b"
	if _, err := queue.admit(ctx, conflict); err != errIdempotencyConflict {
		t.Fatalf("cross-subject admission error = %v", err)
	}
	leased, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok || leased.ID != first.ID || leased.Attempts != 1 {
		t.Fatalf("lease = %#v, ok=%v err=%v", leased, ok, err)
	}
	result := json.RawMessage(`{"ok":true}`)
	eventBody := json.RawMessage(`{"affected_ids":["test-1"]}`)
	receipt := &doltReceipt{
		Version: 1, OperationID: first.ID, ProjectID: first.ProjectID,
		KeyHash: first.KeyHash, SubjectHash: first.SubjectHash, RequestHash: first.RequestHash, Kind: first.Kind,
		Result: result, Outbox: outboxEvent{
			OperationID: first.ID, Sequence: 0, EventType: "issue.create.succeeded", Payload: eventBody, CreatedAt: time.Now().UTC(),
		}}
	if err := queue.markSucceeded(ctx, leased, receipt); err != nil {
		t.Fatal(err)
	}
	stored, err := queue.get(ctx, first.ID)
	if err != nil || stored.Status != statusSucceeded || !bytes.Equal(stored.Result, result) {
		t.Fatalf("stored = %#v err=%v", stored, err)
	}
	events, err := queue.outbox(ctx, first.ID)
	if err != nil || len(events) != 1 || events[0].EventType != "issue.create.succeeded" {
		t.Fatalf("events = %#v err=%v", events, err)
	}
}

func TestQueueAtomicOutboxAndDurableSettings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "operations.db")
	queue, err := openTestOperationQueue(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var journal string
	var synchronous, foreignKeys int
	if err := queue.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if err := queue.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if err := queue.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if strings.ToLower(journal) != "wal" || synchronous != 2 || foreignKeys != 1 {
		t.Fatalf("queue durability settings journal=%s synchronous=%d foreign_keys=%d", journal, synchronous, foreignKeys)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("queue mode=%#o", info.Mode().Perm())
	}

	op, err := queue.admit(ctx, operation{
		ProjectID: testProjectID, KeyHash: "atomic-outbox", SubjectHash: "subject",
		RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] atomic"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	op, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease ok=%v err=%v", ok, err)
	}
	if _, err := queue.db.ExecContext(ctx, `
CREATE TRIGGER fail_outbox_insert BEFORE INSERT ON outbox
BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END`); err != nil {
		t.Fatal(err)
	}
	receipt := fakeReceiptFor(op)
	if err := queue.markSucceeded(ctx, op, receipt); err == nil {
		t.Fatal("injected outbox failure was ignored")
	}
	stored, err := queue.get(ctx, op.ID)
	if err != nil || stored.Status != statusRunning {
		t.Fatalf("success update was not rolled back: %#v err=%v", stored, err)
	}
	if _, err := queue.db.ExecContext(ctx, "DROP TRIGGER fail_outbox_insert"); err != nil {
		t.Fatal(err)
	}
	if err := queue.markSucceeded(ctx, op, receipt); err != nil {
		t.Fatal(err)
	}
	if err := queue.quickCheck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	queue, err = openTestOperationQueue(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	stored, err = queue.get(ctx, op.ID)
	if err != nil || stored.Status != statusSucceeded {
		t.Fatalf("reopened operation=%#v err=%v", stored, err)
	}
	events, err := queue.outbox(ctx, op.ID)
	if err != nil || len(events) != 1 {
		t.Fatalf("reopened outbox=%#v err=%v", events, err)
	}
}

func TestGatewayHTTPIdempotentSubmission(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	token := []byte("0123456789abcdef0123456789abcdef")
	service, err := newTestGatewayService(ctx, token, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] gateway test"}}`
	first := submitRequest(service, token, "stable-key-0001", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	var firstOp operation
	if err := json.Unmarshal(first.Body.Bytes(), &firstOp); err != nil {
		t.Fatal(err)
	}
	second := submitRequest(service, token, "stable-key-0001", body)
	if second.Code != http.StatusOK {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	var secondOp operation
	if err := json.Unmarshal(second.Body.Bytes(), &secondOp); err != nil {
		t.Fatal(err)
	}
	if firstOp.ID != secondOp.ID || backend.executes.Load() != 1 {
		t.Fatalf("first=%s second=%s executes=%d", firstOp.ID, secondOp.ID, backend.executes.Load())
	}
	conflict := submitRequest(service, token, "stable-key-0001", `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] changed"}}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
}

func TestGatewayHTTPConcurrentDuplicateSubmissionExecutesOnce(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	block := make(chan struct{})
	backend.block = block
	token := []byte("0123456789abcdef0123456789abcdef")
	service, err := newTestGatewayService(ctx, token, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	const clients = 32
	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] concurrent gateway test"}}`
	responses := make(chan *httptest.ResponseRecorder, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- submitRequest(service, token, "stable-concurrent-key", body)
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for backend.executes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.executes.Load() != 1 {
		close(block)
		wg.Wait()
		t.Fatalf("backend executes before release = %d", backend.executes.Load())
	}
	close(block)
	wg.Wait()
	close(responses)
	var operationID string
	for response := range responses {
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		var op operation
		if err := json.Unmarshal(response.Body.Bytes(), &op); err != nil {
			t.Fatal(err)
		}
		if operationID == "" {
			operationID = op.ID
		}
		if op.ID != operationID {
			t.Fatalf("operation ID %q differs from %q", op.ID, operationID)
		}
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("backend executed %d times", backend.executes.Load())
	}
	events, err := queue.outbox(ctx, operationID)
	if err != nil || len(events) != 1 {
		t.Fatalf("outbox events=%#v err=%v", events, err)
	}
}

func TestGatewayErrorsDoNotExposeBackendDetails(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	const sentinel = "password=do-not-leak database=foreign_team issue=secret-title"
	backend.execErr = errors.New(sentinel)
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	response := submitRequest(service, testToken, "redaction-key-0001",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] redaction"}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), sentinel) || strings.Contains(response.Body.String(), "do-not-leak") {
		t.Fatalf("backend detail leaked in response: %s", response.Body.String())
	}
	var op operation
	if err := json.Unmarshal(response.Body.Bytes(), &op); err != nil {
		t.Fatal(err)
	}
	if op.Status != statusFailed || op.ErrorMessage != "operation failed" {
		t.Fatalf("redacted operation=%#v", op)
	}
	stored, err := queue.get(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored.ErrorMessage, "do-not-leak") || stored.ErrorMessage != "operation failed" {
		t.Fatalf("backend detail leaked in queue: %#v", stored)
	}
	if got := sanitizeError(errors.New(sentinel)); strings.Contains(got, "do-not-leak") {
		t.Fatalf("sanitizeError leaked %q", got)
	}
}

func TestGatewayReconcilesUnknownWhileWorkerIsBusy(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	block := make(chan struct{})
	backend.block = block
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			close(block)
		}
		service.Close()
		_ = queue.Close()
	}()

	busy, err := queue.admit(ctx, operation{
		ProjectID: testProjectID, KeyHash: "busy-key", SubjectHash: service.subjectHash,
		RequestHash: "busy-request", Kind: "issue.create",
		Payload: json.RawMessage(`{"title":"[beads-perf-lab] busy"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	service.signal()
	deadline := time.Now().Add(2 * time.Second)
	for backend.executes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.executes.Load() != 1 {
		t.Fatal("worker did not start the blocking operation")
	}
	if stored, err := queue.get(ctx, busy.ID); err != nil || stored.Status != statusRunning {
		t.Fatalf("busy operation=%#v err=%v", stored, err)
	}

	unknown, err := queue.admit(ctx, operation{
		ProjectID: testProjectID, KeyHash: "unknown-during-busy", SubjectHash: service.subjectHash,
		RequestHash: "unknown-request", Kind: "issue.create",
		Payload: json.RawMessage(`{"title":"[beads-perf-lab] unknown"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	unknown, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease unknown ok=%v err=%v", ok, err)
	}
	if err := queue.markUnknown(ctx, unknown, "operation result unknown"); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.receipts[unknown.KeyHash] = fakeReceiptFor(unknown)
	backend.mu.Unlock()

	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := queue.get(ctx, unknown.ID)
		if err == nil && stored.Status == statusSucceeded {
			close(block)
			closed = true
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	stored, _ := queue.get(ctx, unknown.ID)
	t.Fatalf("unknown operation was starved behind busy worker: %#v", stored)
}

func TestGatewayRecoversRunningOperationFromReceipt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "operations.db")
	queue, err := openTestOperationQueue(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	op, err := queue.admit(ctx, operation{
		ProjectID: testProjectID, KeyHash: "recover-key", SubjectHash: "subject",
		RequestHash: "request", Kind: "graph.apply", Payload: json.RawMessage(`{"nodes":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease ok=%v err=%v", ok, err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	backend := newFakeBackend()
	result := json.RawMessage(`{"recovered":true}`)
	backend.receipts[op.KeyHash] = &doltReceipt{
		Version: 1, OperationID: op.ID, ProjectID: op.ProjectID, KeyHash: op.KeyHash,
		SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind, Result: result,
		Outbox: outboxEvent{OperationID: op.ID, EventType: "graph.apply.succeeded", Payload: json.RawMessage(`{"recovered":true}`), CreatedAt: time.Now().UTC()},
	}
	queue, err = openTestOperationQueue(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	stored, err := queue.get(ctx, leased.ID)
	if err != nil || stored.Status != statusSucceeded || !bytes.Equal(stored.Result, result) {
		t.Fatalf("recovered = %#v err=%v", stored, err)
	}
	if backend.executes.Load() != 0 {
		t.Fatalf("recovery re-executed mutation %d time(s)", backend.executes.Load())
	}
}

func TestGatewayLeavesAmbiguousReceiptLookupUnknown(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	op, err := queue.admit(ctx, operation{ProjectID: testProjectID, KeyHash: "ambiguous", SubjectHash: "subject", RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] ambiguous"}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease ok=%v err=%v", ok, err)
	}
	backend := newFakeBackend()
	backend.execErr = errors.New("connection reset after commit")
	backend.lookupErr = errors.New("receipt database unavailable")
	service := &gatewayService{ctx: context.Background(), queue: queue, backend: backend}
	service.execute(op)
	stored, err := queue.get(ctx, op.ID)
	if err != nil || stored.Status != statusUnknown || stored.ErrorCode != "needs_reconcile" {
		t.Fatalf("ambiguous stored=%#v err=%v", stored, err)
	}
}

func TestUnknownOperationReconcilesWithoutRestart(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.execFailures.Store(1)
	backend.lookupFailures.Store(1)
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	op, err := queue.admit(ctx, operation{ProjectID: testProjectID, KeyHash: "auto-reconcile", SubjectHash: "subject", RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] reconcile"}`)})
	if err != nil {
		t.Fatal(err)
	}
	service.signal()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := queue.get(ctx, op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status == statusSucceeded {
			if backend.executes.Load() != 2 {
				t.Fatalf("execute count=%d, want 2", backend.executes.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stored, _ := queue.get(ctx, op.ID)
	t.Fatalf("operation did not reconcile: %#v", stored)
}

func TestQueueTransitionLockDoesNotStrandRunning(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "operations.db")
	queue, err := openTestOperationQueue(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.db.ExecContext(ctx, "PRAGMA busy_timeout=50"); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	block := make(chan struct{})
	backend.block = block
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	op, err := queue.admit(ctx, operation{
		ProjectID: testProjectID, KeyHash: "transition-lock", SubjectHash: "subject",
		RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] transition lock"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	service.signal()
	deadline := time.Now().Add(2 * time.Second)
	for backend.executes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.executes.Load() != 1 {
		t.Fatal("backend did not begin execution")
	}
	locker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	conn, err := locker.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	close(block)
	time.Sleep(250 * time.Millisecond)
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := queue.get(ctx, op.ID)
		if err == nil && stored.Status == statusSucceeded {
			if service.transitionRetries.Load() == 0 {
				t.Fatal("test did not exercise a transition retry")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stored, _ := queue.get(ctx, op.ID)
	t.Fatalf("transition did not recover after lock release: %#v", stored)
}

func TestReconcileUnknownContinuesAfterLookupFailure(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	backend := newFakeBackend()
	makeUnknown := func(key string) operation {
		op, err := queue.admit(ctx, operation{
			ProjectID: testProjectID, KeyHash: key, SubjectHash: "subject",
			RequestHash: "request-" + key, Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] reconcile"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		op, ok, err := queue.leaseNext(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("lease key=%s ok=%v err=%v", key, ok, err)
		}
		if err := queue.markUnknown(ctx, op, "test ambiguity"); err != nil {
			t.Fatal(err)
		}
		return op
	}
	first := makeUnknown("lookup-fails")
	second := makeUnknown("lookup-succeeds")
	backend.lookupErrByKey[first.KeyHash] = errors.New("receipt lookup unavailable")
	backend.receipts[second.KeyHash] = fakeReceiptFor(second)
	service := &gatewayService{queue: queue, backend: backend}
	if err := service.reconcileUnknown(ctx); err == nil {
		t.Fatal("reconciliation should report the failed lookup")
	}
	firstStored, err := queue.get(ctx, first.ID)
	if err != nil || firstStored.Status != statusUnknown {
		t.Fatalf("first operation=%#v err=%v", firstStored, err)
	}
	secondStored, err := queue.get(ctx, second.ID)
	if err != nil || secondStored.Status != statusSucceeded {
		t.Fatalf("second operation=%#v err=%v", secondStored, err)
	}
}

func TestShutdownCancelsWorkerAndConcurrentCloseIsSafe(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	backend := newFakeBackend()
	backend.block = make(chan struct{})
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	request := authorizedRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/operations", strings.NewReader(
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] shutdown"}}`), testToken)
	request.Header.Set("Idempotency-Key", "shutdown-key-0001")
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", response.Code, response.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for backend.executes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("backend execute count=%d before shutdown", backend.executes.Load())
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.Close()
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Close did not return")
	}
	for _, secret := range [][]byte{service.token, service.stableKey} {
		for _, value := range secret {
			if value != 0 {
				t.Fatal("service secret was not zeroed")
			}
		}
	}
}

func TestDrainWaitsForBlockedTerminalHandlerBeforeDependencyClose(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.block = make(chan struct{})
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequest := context.WithCancel(ctx)
	request := authorizedRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/terminal-operations",
		strings.NewReader(`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] drain"}}`), testToken).WithContext(requestCtx)
	request.Header.Set("Idempotency-Key", "drain-terminal-key")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		service.ServeHTTP(response, request)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for backend.executes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("blocked handler never reached backend")
	}
	service.beginDrain()
	short, shortCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := service.waitHandlers(short); !errors.Is(err, context.DeadlineExceeded) {
		shortCancel()
		t.Fatalf("drain returned before active handler: %v", err)
	}
	shortCancel()

	rejected := httptest.NewRecorder()
	service.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("new request during drain status=%d", rejected.Code)
	}
	cancelRequest()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled terminal handler did not return")
	}
	drain, drainCancel := context.WithTimeout(ctx, time.Second)
	if err := service.waitHandlers(drain); err != nil {
		drainCancel()
		t.Fatalf("drain after cancellation: %v", err)
	}
	drainCancel()
	service.Close()
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTransientClassificationUsesTypedErrors(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		driver.ErrBadConn,
		&net.DNSError{IsTimeout: true},
		&mysql.MySQLError{Number: 1205, Message: "lock wait timeout"},
		&mysql.MySQLError{Number: 1213, Message: "transaction conflict"},
		errors.New("deadlock found when trying to get lock"),
	} {
		if !isTransient(err) {
			t.Fatalf("error %T %v was not classified transient", err, err)
		}
	}
	if isTransient(errors.New("validation failed")) {
		t.Fatal("permanent-looking validation error classified transient")
	}
}

func TestDeterministicDomainErrorsAreCanonicalPermanent(t *testing.T) {
	for _, err := range []error{
		sql.ErrNoRows,
		errors.Join(errors.New("wrapped"), storagepkg.ErrNotFound),
		errors.Join(errors.New("wrapped"), storagepkg.ErrAlreadyClaimed),
		errors.Join(errors.New("wrapped"), storagepkg.ErrNotClaimable),
	} {
		classified := classifyDeterministicOperationError(err)
		if !isPermanent(classified) || isTransient(classified) {
			t.Fatalf("deterministic error %v classified as %v", err, classified)
		}
	}
	unclassified := errors.New("backend invariant failed")
	if got := classifyDeterministicOperationError(unclassified); got != unclassified || isPermanent(got) {
		t.Fatalf("unclassified failure was manufactured permanent: %v", got)
	}
}

func fakeReceiptFor(op operation) *doltReceipt {
	result := json.RawMessage(`{"ok":true}`)
	eventPayload := json.RawMessage(`{"ok":true}`)
	return &doltReceipt{
		Version: 1, OperationID: op.ID, ProjectID: op.ProjectID,
		KeyHash: op.KeyHash, SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Result: result, CommittedAt: time.Now().UTC(),
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0, EventType: op.Kind + ".succeeded", Payload: eventPayload, CreatedAt: time.Now().UTC()},
	}
}

func terminalSuccessOutcomeForTest(op operation, result json.RawMessage) *doltReceipt {
	payload, _ := json.Marshal(durableOutboxPayload{
		OperationID: op.ID, Kind: op.Kind, Status: statusSucceeded,
		AffectedIDs: []string{"test-issue"}, ResultHash: digestHex(result),
	})
	now := time.Now().UTC()
	return &doltReceipt{
		Version: 2, OperationID: op.ID, ProjectID: op.ProjectID, KeyHash: op.KeyHash,
		SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Status: statusSucceeded, Result: result, CommittedAt: now,
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0,
			EventType: op.Kind + ".succeeded", Payload: payload, CreatedAt: now},
	}
}

func TestValidateReceiptBindsOperationAndOutbox(t *testing.T) {
	op := operation{ID: "op-1", ProjectID: testProjectID, KeyHash: "key", SubjectHash: "subject", RequestHash: "request", Kind: "graph.apply"}
	valid := doltReceipt{
		Version: 1, OperationID: op.ID, ProjectID: op.ProjectID, KeyHash: op.KeyHash,
		SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0, EventType: "graph.apply.succeeded"},
	}
	if err := validateReceipt(valid, op); err != nil {
		t.Fatalf("valid receipt: %v", err)
	}
	mutations := []struct {
		name string
		edit func(*doltReceipt)
	}{
		{name: "version", edit: func(r *doltReceipt) { r.Version = 2 }},
		{name: "operation", edit: func(r *doltReceipt) { r.OperationID = "wrong" }},
		{name: "project", edit: func(r *doltReceipt) { r.ProjectID = "wrong" }},
		{name: "key", edit: func(r *doltReceipt) { r.KeyHash = "wrong" }},
		{name: "subject", edit: func(r *doltReceipt) { r.SubjectHash = "wrong" }},
		{name: "request", edit: func(r *doltReceipt) { r.RequestHash = "wrong" }},
		{name: "kind", edit: func(r *doltReceipt) { r.Kind = "wrong" }},
		{name: "outbox operation", edit: func(r *doltReceipt) { r.Outbox.OperationID = "wrong" }},
		{name: "outbox sequence", edit: func(r *doltReceipt) { r.Outbox.Sequence = 1 }},
		{name: "outbox type", edit: func(r *doltReceipt) { r.Outbox.EventType = "wrong" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			got := valid
			mutation.edit(&got)
			if err := validateReceipt(got, op); err == nil {
				t.Fatal("receipt identity mutation accepted")
			}
		})
	}
}

func TestValidateTerminalOutcomeBindsStatusResultAndOutbox(t *testing.T) {
	op := operation{
		ID: "11111111-2222-8333-8444-555555555555", ProjectID: testProjectID,
		KeyHash:     terminalIdempotencyKeyHash(testStableKey, "structural-terminal-key"),
		SubjectHash: "subject", RequestHash: "request", Kind: "issue.create",
	}
	result := json.RawMessage(`{"ok":true}`)
	payload, err := json.Marshal(durableOutboxPayload{
		OperationID: op.ID, Kind: op.Kind, Status: statusSucceeded,
		AffectedIDs: []string{"test-issue"}, ResultHash: digestHex(result),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	valid := doltReceipt{
		Version: 2, OperationID: op.ID, ProjectID: op.ProjectID, KeyHash: op.KeyHash,
		SubjectHash: op.SubjectHash, RequestHash: op.RequestHash, Kind: op.Kind,
		Status: statusSucceeded, Result: result, CommittedAt: now,
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0,
			EventType: op.Kind + ".succeeded", Payload: payload, CreatedAt: now},
	}
	if err := validateReceipt(valid, op); err != nil {
		t.Fatalf("valid terminal outcome: %v", err)
	}
	mutations := []struct {
		name string
		edit func(*doltReceipt)
	}{
		{name: "invalid result", edit: func(r *doltReceipt) { r.Result = json.RawMessage(`{`) }},
		{name: "scalar result", edit: func(r *doltReceipt) { r.Result = json.RawMessage(`true`) }},
		{name: "missing committed timestamp", edit: func(r *doltReceipt) { r.CommittedAt = time.Time{} }},
		{name: "missing outbox timestamp", edit: func(r *doltReceipt) { r.Outbox.CreatedAt = time.Time{} }},
		{name: "unstructured outbox", edit: func(r *doltReceipt) { r.Outbox.Payload = json.RawMessage(`{"operation_id":"wrong"}`) }},
		{name: "outbox result mismatch", edit: func(r *doltReceipt) { r.Result = json.RawMessage(`{"ok":false}`) }},
		{name: "success with error", edit: func(r *doltReceipt) { r.ErrorCode = "wrong" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			got := valid
			mutation.edit(&got)
			if err := validateReceipt(got, op); err == nil {
				t.Fatal("malformed terminal outcome accepted")
			}
		})
	}
}

func TestCanonicalOutcomeIsInsertOnceAndComparedOnConflict(t *testing.T) {
	ctx := context.Background()
	op := operation{
		ID: "11111111-2222-8333-8444-555555555555", ProjectID: testProjectID,
		KeyHash:     terminalIdempotencyKeyHash(testStableKey, "insert-once-terminal-key"),
		SubjectHash: "subject", RequestHash: "request-a", Kind: "issue.create",
	}
	outcome := terminalSuccessOutcomeForTest(op, json.RawMessage(`{"ok":true}`))
	metadata := &memoryCanonicalMetadata{values: make(map[string]string)}
	stored, inserted, err := persistOutcomeInUOW(ctx, metadata, op, outcome)
	if err != nil || !inserted || stored.OperationID != op.ID {
		t.Fatalf("first insert stored=%#v inserted=%v err=%v", stored, inserted, err)
	}
	if len(metadata.values) != 2 {
		t.Fatalf("canonical metadata rows=%d want=2", len(metadata.values))
	}
	stored, inserted, err = persistOutcomeInUOW(ctx, metadata, op, outcome)
	if err != nil || inserted || stored.OperationID != op.ID || stored.RequestHash != op.RequestHash {
		t.Fatalf("idempotent conflict stored=%#v inserted=%v err=%v", stored, inserted, err)
	}
	for _, query := range metadata.queries {
		if strings.Contains(strings.ToUpper(query), "REPLACE") {
			t.Fatalf("overwrite statement used: %q", query)
		}
	}

	conflict := op
	conflict.RequestHash = "request-b"
	conflictingOutcome := terminalSuccessOutcomeForTest(conflict, json.RawMessage(`{"ok":false}`))
	if _, _, err := persistOutcomeInUOW(ctx, metadata, conflict, conflictingOutcome); !errors.Is(err, errReceiptIdentityMismatch) {
		t.Fatalf("different request reused immutable key: %v", err)
	}
}

func TestQueueBindingFreshReopenAndMismatches(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "operations.db")
	binding := queueBinding{
		ProjectID: testProjectID, Database: testDatabase,
		IdempotencyKeyID: hmacHex(testStableKey, "binding\x00idempotency-v1"),
		SubjectID:        hmacHex(testStableKey, "subject\x00"+testSubject),
	}
	queue, err := openOperationQueue(ctx, path, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	queue, err = openOperationQueue(ctx, path, binding)
	if err != nil {
		t.Fatalf("reopen matching binding: %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		edit func(*queueBinding)
	}{
		{name: "project", edit: func(b *queueBinding) { b.ProjectID = "99999999-2222-4333-8444-555555555555" }},
		{name: "database", edit: func(b *queueBinding) { b.Database = "beads_perf_lab_other" }},
		{name: "stable key", edit: func(b *queueBinding) { b.IdempotencyKeyID = "different" }},
		{name: "subject", edit: func(b *queueBinding) { b.SubjectID = "different" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mismatch := binding
			tt.edit(&mismatch)
			if queue, err := openOperationQueue(ctx, path, mismatch); err == nil {
				_ = queue.Close()
				t.Fatal("queue binding mismatch accepted")
			}
		})
	}
}

func TestQueueBindingRejectsLegacyAndForeignState(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*operationQueue, string) error
	}{
		{
			name: "unbound non-empty",
			mutate: func(queue *operationQueue, _ string) error {
				_, err := queue.db.ExecContext(ctx, "DELETE FROM gateway_binding")
				return err
			},
		},
		{
			name: "foreign project row",
			mutate: func(queue *operationQueue, id string) error {
				_, err := queue.db.ExecContext(ctx, "UPDATE operations SET project_id=? WHERE id=?", "99999999-2222-4333-8444-555555555555", id)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "operations.db")
			queue, err := openTestOperationQueue(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			op, err := queue.admit(ctx, operation{
				ProjectID: testProjectID, KeyHash: "binding-key", SubjectHash: "subject",
				RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{"title":"[beads-perf-lab] binding"}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(queue, op.ID); err != nil {
				t.Fatal(err)
			}
			if err := queue.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := openTestOperationQueue(ctx, path); err == nil {
				_ = reopened.Close()
				t.Fatal("unsafe queue state accepted")
			}
		})
	}
}

func TestQueueRejectsForeignOperationAndSymlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	queue, err := openTestOperationQueue(ctx, filepath.Join(dir, "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := queue.admit(ctx, operation{ProjectID: "wrong"}); err == nil {
		t.Fatal("foreign project admission accepted")
	}
	if err := os.Symlink(filepath.Join(dir, "operations.db"), filepath.Join(dir, "queue-link.db")); err != nil {
		t.Fatal(err)
	}
	if linked, err := openTestOperationQueue(ctx, filepath.Join(dir, "queue-link.db")); err == nil {
		_ = linked.Close()
		t.Fatal("symlink queue accepted")
	}
}

func TestUOWBackendRejectsWrongProjectBeforeProviderAccess(t *testing.T) {
	backend := &uowBackend{projectID: testProjectID}
	op := operation{ProjectID: "wrong"}
	if _, err := backend.Execute(context.Background(), op); err == nil {
		t.Fatal("wrong-project execute accepted")
	}
	if _, err := backend.LookupReceipt(context.Background(), op); err == nil {
		t.Fatal("wrong-project receipt lookup accepted")
	}
	if _, err := backend.PersistPermanentFailure(context.Background(), op, "failed", "failed"); err == nil {
		t.Fatal("wrong-project permanent outcome accepted")
	}
}

func TestGatewayConfiguredSubjectAndBearerRotation(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] rotated token"}}`
	first := submitRequest(service, testToken, "rotation-key-0001", body)
	if first.Code != http.StatusOK {
		t.Fatalf("initial submit status=%d body=%s", first.Code, first.Body.String())
	}
	var firstOp operation
	if err := json.Unmarshal(first.Body.Bytes(), &firstOp); err != nil {
		t.Fatal(err)
	}
	service.Close()

	rotated := []byte("fedcba9876543210fedcba9876543210")
	service, err = newTestGatewayService(ctx, rotated, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	second := submitRequest(service, rotated, "rotation-key-0001", body)
	if second.Code != http.StatusOK {
		t.Fatalf("rotated submit status=%d body=%s", second.Code, second.Body.String())
	}
	var secondOp operation
	if err := json.Unmarshal(second.Body.Bytes(), &secondOp); err != nil {
		t.Fatal(err)
	}
	if firstOp.ID != secondOp.ID || backend.executes.Load() != 1 {
		t.Fatalf("token rotation broke idempotency: first=%s second=%s executions=%d", firstOp.ID, secondOp.ID, backend.executes.Load())
	}

	oldToken := authorizedRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/ping", nil, testToken)
	oldResponse := httptest.NewRecorder()
	service.ServeHTTP(oldResponse, oldToken)
	if oldResponse.Code != http.StatusUnauthorized {
		t.Fatalf("old bearer status=%d", oldResponse.Code)
	}
	wrongSubject := authorizedRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/ping", nil, rotated)
	wrongSubject.Header.Set("X-Beads-Subject", "forged-agent")
	wrongResponse := httptest.NewRecorder()
	service.ServeHTTP(wrongResponse, wrongSubject)
	if wrongResponse.Code != http.StatusForbidden {
		t.Fatalf("forged subject status=%d", wrongResponse.Code)
	}
	withoutSubject := authorizedRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/ping", nil, rotated)
	withoutSubject.Header.Del("X-Beads-Subject")
	withoutResponse := httptest.NewRecorder()
	service.ServeHTTP(withoutResponse, withoutSubject)
	if withoutResponse.Code != http.StatusOK {
		t.Fatalf("server-owned subject without assertion status=%d", withoutResponse.Code)
	}
}

func TestValidateGatewayConfigIsolation(t *testing.T) {
	valid := gatewayConfig{
		Listen: "127.0.0.1:7707", RuntimeRoot: "/var/lib/beads-perf-lab", Host: "127.0.0.1", Port: 13360,
		Database: "beads_perf_lab_gateway_test", ProjectID: testProjectID,
		LabID:        "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		Subject:      testSubject,
		PasswordFile: "/var/lib/beads-perf-lab/private/password", TokenFile: "/var/lib/beads-perf-lab/private/token",
		IdempotencyKeyFile: "/var/lib/beads-perf-lab/private/idempotency-key",
		QueuePath:          "/var/lib/beads-perf-lab/gateway/operations.db", MaxOpen: 8, MaxIdle: 8,
	}
	if err := validateGatewayConfig(valid); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []struct {
		name string
		edit func(*gatewayConfig)
	}{
		{name: "non-loopback listen", edit: func(c *gatewayConfig) { c.Listen = "0.0.0.0:7707" }},
		{name: "runtime root", edit: func(c *gatewayConfig) { c.RuntimeRoot = "/private/tmp" }},
		{name: "non-loopback Dolt", edit: func(c *gatewayConfig) { c.Host = "10.0.0.1" }},
		{name: "production SQL port", edit: func(c *gatewayConfig) { c.Port = 3306 }},
		{name: "production Dolt port", edit: func(c *gatewayConfig) { c.Port = 3307 }},
		{name: "database", edit: func(c *gatewayConfig) { c.Database = "production" }},
		{name: "project", edit: func(c *gatewayConfig) { c.ProjectID = "" }},
		{name: "noncanonical project", edit: func(c *gatewayConfig) { c.ProjectID = "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE" }},
		{name: "lab", edit: func(c *gatewayConfig) { c.LabID = "" }},
		{name: "subject", edit: func(c *gatewayConfig) { c.Subject = "" }},
		{name: "same auth paths", edit: func(c *gatewayConfig) { c.IdempotencyKeyFile = c.TokenFile }},
		{name: "path escape", edit: func(c *gatewayConfig) { c.QueuePath = "/tmp/queue.db" }},
		{name: "pool", edit: func(c *gatewayConfig) { c.MaxIdle = 9 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid
			tt.edit(&got)
			if err := validateGatewayConfig(got); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestGatewaySQLPrincipalIsExactlyProjectBound(t *testing.T) {
	first, err := labidentity.SQLUser(testProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if first != strings.ReplaceAll(testProjectID, "-", "") || len(first) != 32 {
		t.Fatalf("gateway SQL user = %q", first)
	}
	second, err := labidentity.SQLUser("99999999-2222-4333-8444-555555555555")
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("cross-project gateway binding reused a SQL principal")
	}
}

func TestGatewayAttestationIsAuthenticatedAndSigned(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := newTestGatewayService(ctx, testToken, queue, newFakeBackend())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	unauthorized := httptest.NewRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/attestation", nil)
	unauthorizedResponse := httptest.NewRecorder()
	service.ServeHTTP(unauthorizedResponse, unauthorized)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized attestation status=%d", unauthorizedResponse.Code)
	}

	request := authorizedRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/attestation", nil, testToken)
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("attestation status=%d body=%s", response.Code, response.Body.String())
	}
	var attestation labAttestation
	if err := json.Unmarshal(response.Body.Bytes(), &attestation); err != nil {
		t.Fatal(err)
	}
	if attestation.DatabaseName != testDatabase || attestation.ProjectID != testProjectID ||
		attestation.Environment != "synthetic" || attestation.Signature == "" {
		t.Fatalf("attestation=%#v", attestation)
	}
	wantSignature := signLabAttestation(testToken, labAttestation{
		Version: attestation.Version, Environment: attestation.Environment, LabID: attestation.LabID,
		DatabaseName: attestation.DatabaseName, ProjectID: attestation.ProjectID,
	})
	if !hmac.Equal([]byte(attestation.Signature), []byte(wantSignature)) {
		t.Fatal("attestation signature mismatch")
	}
}

func TestValidateLabAttestationFailsClosed(t *testing.T) {
	valid := labAttestation{
		Version: 1, Environment: "synthetic", LabID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		DatabaseName: testDatabase, ProjectID: testProjectID,
	}
	if err := validateLabAttestation(valid, testProjectID, testDatabase, valid.LabID); err != nil {
		t.Fatalf("valid attestation: %v", err)
	}
	tests := []struct {
		name string
		edit func(*labAttestation)
	}{
		{name: "environment", edit: func(a *labAttestation) { a.Environment = "production" }},
		{name: "database", edit: func(a *labAttestation) { a.DatabaseName = "production" }},
		{name: "project", edit: func(a *labAttestation) { a.ProjectID = "99999999-2222-4333-8444-555555555555" }},
		{name: "lab", edit: func(a *labAttestation) { a.LabID = "ffffffff-bbbb-4ccc-8ddd-eeeeeeeeeeee" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid
			tt.edit(&got)
			if err := validateLabAttestation(got, testProjectID, testDatabase, valid.LabID); err == nil {
				t.Fatal("mismatched attestation accepted")
			}
		})
	}
}

func TestGatewayAuthAndProjectGuard(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := newTestGatewayService(ctx, testToken, queue, newFakeBackend())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/ping", nil)
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.Code)
	}
	request = authorizedRequest(http.MethodGet, "/v1/projects/"+testProjectID+"/ping", nil, []byte("0123456789abcdef0123456789abcdef"))
	request.Header.Set("X-Beads-Project-ID", "wrong")
	response = httptest.NewRecorder()
	service.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("wrong project status=%d", response.Code)
	}
}

func TestValidateGraphRejectsCycleAndNonSyntheticData(t *testing.T) {
	valid := graphPayload{
		Nodes: []graphNode{{Key: "a", Title: "[beads-perf-lab] a"}, {Key: "b", Title: "[beads-perf-lab] b"}},
		Edges: []graphEdge{{FromKey: "b", ToKey: "a", Type: types.DepBlocks}},
	}
	if err := validateGraph(valid); err != nil {
		t.Fatalf("valid graph: %v", err)
	}
	cycle := valid
	cycle.Edges = append(cycle.Edges, graphEdge{FromKey: "a", ToKey: "b", Type: types.DepBlocks})
	if err := validateGraph(cycle); err == nil {
		t.Fatal("cycle accepted")
	}
	badTitle := valid
	badTitle.Nodes = append([]graphNode(nil), valid.Nodes...)
	badTitle.Nodes[0].Title = "not synthetic"
	if err := validateGraph(badTitle); err == nil {
		t.Fatal("non-synthetic title accepted")
	}
}

func TestValidateUpdateRejectsInvalidStatus(t *testing.T) {
	invalid := types.Status("garbage")
	if err := validateUpdatePayload(updatePayload{Status: &invalid}); err == nil || !isPermanent(err) {
		t.Fatalf("invalid status error=%v", err)
	}
	valid := types.StatusInProgress
	if err := validateUpdatePayload(updatePayload{Status: &valid}); err != nil {
		t.Fatalf("valid status: %v", err)
	}
}

func TestDurableMetadataSizeCaps(t *testing.T) {
	if err := validateDurableMetadataSizes(make([]byte, maxMetadataValueBytes), make([]byte, maxOutboxMarkerValueBytes)); err != nil {
		t.Fatalf("boundary sizes rejected: %v", err)
	}
	if err := validateDurableMetadataSizes(make([]byte, maxMetadataValueBytes+1), nil); err == nil || !isPermanent(err) {
		t.Fatalf("oversize receipt error=%v", err)
	}
	if err := validateDurableMetadataSizes(nil, make([]byte, maxOutboxMarkerValueBytes+1)); err == nil || !isPermanent(err) {
		t.Fatalf("oversize outbox error=%v", err)
	}
}

func TestDeterministicTerminalOperationID(t *testing.T) {
	project := testProjectID
	subject := hmacHex(testStableKey, "subject\x00"+testSubject)
	keyHash := terminalIdempotencyKeyHash(testStableKey, "terminal-deterministic-key")
	first := deterministicTerminalOperationID(testStableKey, project, subject, keyHash)
	second := deterministicTerminalOperationID(testStableKey, project, subject, keyHash)
	if first != second {
		t.Fatalf("deterministic IDs differ: %q vs %q", first, second)
	}
	parsed, err := uuid.Parse(first)
	if err != nil || parsed.Version() != 8 || parsed.Variant() != uuid.RFC4122 {
		t.Fatalf("deterministic ID is not an RFC 4122 UUIDv8: %q err=%v", first, err)
	}
	mutations := []string{
		deterministicTerminalOperationID(testStableKey, "99999999-2222-4333-8444-555555555555", subject, keyHash),
		deterministicTerminalOperationID(testStableKey, project, subject+"x", keyHash),
		deterministicTerminalOperationID(testStableKey, project, subject, keyHash+"x"),
		deterministicTerminalOperationID([]byte("fedcba9876543210fedcba9876543210"), project, subject, keyHash),
	}
	for _, mutated := range mutations {
		if mutated == first {
			t.Fatalf("identity mutation retained operation ID %q", first)
		}
	}
}

func TestTerminalOperationsAreAuthenticatedDeterministicAndConflictSafe(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal deterministic"}}`
	unauthorized := httptest.NewRequest(http.MethodPost,
		"/v1/projects/"+testProjectID+"/terminal-operations", strings.NewReader(body))
	unauthorized.Header.Set("Idempotency-Key", "terminal-auth-key")
	unauthorizedResponse := httptest.NewRecorder()
	service.ServeHTTP(unauthorizedResponse, unauthorized)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized terminal status=%d", unauthorizedResponse.Code)
	}

	first := submitTerminalRequest(ctx, service, testToken, "terminal-deterministic-key", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first terminal status=%d body=%s", first.Code, first.Body.String())
	}
	var firstOp operation
	if err := json.Unmarshal(first.Body.Bytes(), &firstOp); err != nil {
		t.Fatal(err)
	}
	wantID := deterministicTerminalOperationID(testStableKey, testProjectID, service.subjectHash,
		terminalIdempotencyKeyHash(testStableKey, "terminal-deterministic-key"))
	if firstOp.ID != wantID || !firstOp.terminal() {
		t.Fatalf("terminal operation=%#v wantID=%q", firstOp, wantID)
	}
	second := submitTerminalRequest(ctx, service, testToken, "terminal-deterministic-key", body)
	if second.Code != http.StatusOK {
		t.Fatalf("idempotent terminal status=%d body=%s", second.Code, second.Body.String())
	}
	var secondOp operation
	if err := json.Unmarshal(second.Body.Bytes(), &secondOp); err != nil {
		t.Fatal(err)
	}
	if secondOp.ID != firstOp.ID || backend.executes.Load() != 1 {
		t.Fatalf("idempotent terminal first=%s second=%s executes=%d", firstOp.ID, secondOp.ID, backend.executes.Load())
	}
	conflict := submitTerminalRequest(ctx, service, testToken, "terminal-deterministic-key",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] changed"}}`)
	if conflict.Code != http.StatusConflict || strings.Contains(conflict.Body.String(), firstOp.RequestHash) {
		t.Fatalf("terminal conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
}

func TestLegacyOperationSubmissionIsDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newGatewayService(ctx, testProjectID, gatewayAuth{
		BearerToken: testToken, StableKey: testStableKey, Subject: testSubject,
	}, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	response := submitRequest(service, testToken, "legacy-disabled-key",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] legacy disabled"}}`)
	if response.Code != http.StatusGone || !strings.Contains(response.Body.String(), "legacy_lane_disabled") {
		t.Fatalf("legacy response status=%d body=%s", response.Code, response.Body.String())
	}
	var rows int
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 || backend.executes.Load() != 0 {
		t.Fatalf("legacy lane mutated rows=%d executes=%d", rows, backend.executes.Load())
	}
}

func TestTerminalRetryBudgetStopsPersistentFailureAcrossReconcilerTicks(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.execErr = errors.New("connection reset by peer")
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	const key = "terminal-retry-exhausted-key"
	const body = `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] retry exhausted"}}`
	response := submitTerminalRequest(ctx, service, testToken, key, body)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "retry_exhausted") {
		t.Fatalf("first response status=%d body=%s", response.Code, response.Body.String())
	}
	opID := deterministicTerminalOperationID(testStableKey, testProjectID, service.subjectHash,
		terminalIdempotencyKeyHash(testStableKey, key))
	op, err := queue.get(ctx, opID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != statusUnknown || op.ErrorCode != "retry_exhausted" || op.Attempts != 3 {
		t.Fatalf("exhausted operation=%#v", op)
	}
	if backend.executes.Load() != 3 {
		t.Fatalf("executes=%d, want 3", backend.executes.Load())
	}
	time.Sleep(2200 * time.Millisecond)
	if backend.executes.Load() != 3 {
		t.Fatalf("reconciler restarted exhausted work: executes=%d", backend.executes.Load())
	}
	unknown, err := queue.unknown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 0 {
		t.Fatalf("retry-exhausted operation remained auto-reconcilable: %#v", unknown)
	}

	started := time.Now()
	retry := submitTerminalRequest(ctx, service, testToken, key, body)
	if retry.Code != http.StatusServiceUnavailable || !strings.Contains(retry.Body.String(), "retry_exhausted") {
		t.Fatalf("same-key retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	if time.Since(started) > time.Second || backend.executes.Load() != 3 {
		t.Fatalf("same-key retry was not fail-fast: elapsed=%s executes=%d", time.Since(started), backend.executes.Load())
	}
}

func TestRetryExhaustedProjectionHasBoundedDeadLetterQuota(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	var firstID string
	for i := 0; i <= maxRetryExhaustedRows; i++ {
		keyHash := terminalIdempotencyKeyHash(testStableKey, fmt.Sprintf("dead-letter-%03d", i))
		in := operation{
			ID:        deterministicTerminalOperationID(testStableKey, testProjectID, "subject", keyHash),
			ProjectID: testProjectID, KeyHash: keyHash, SubjectHash: "subject",
			RequestHash: fmt.Sprintf("request-%03d", i), Kind: "issue.create",
			Payload: json.RawMessage(fmt.Sprintf(`{"title":"[beads-perf-lab] dead letter %03d"}`, i)),
		}
		op, err := queue.admit(ctx, in)
		if err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
		if i == 0 {
			firstID = op.ID
		}
		if _, err := queue.db.ExecContext(ctx,
			"UPDATE operations SET status='running' WHERE id=?", op.ID); err != nil {
			t.Fatal(err)
		}
		if err := queue.markRetryExhausted(ctx, op, "persistent synthetic failure"); err != nil {
			t.Fatalf("mark %d: %v", i, err)
		}
	}
	var count int
	if err := queue.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM operations WHERE status='unknown' AND error_code='retry_exhausted'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != maxRetryExhaustedRows {
		t.Fatalf("retry-exhausted rows=%d, want %d", count, maxRetryExhaustedRows)
	}
	if _, err := queue.get(ctx, firstID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("oldest dead letter was not compacted: %v", err)
	}
}

func TestTerminalProjectionRowBoundPrunesAndCanonicalReplayRestores(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	var first operation
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("terminal-projection-row-%d", i)
		body := fmt.Sprintf(`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] projection row %d"}}`, i)
		response := submitTerminalRequest(ctx, service, testToken, key, body)
		if response.Code != http.StatusOK {
			t.Fatalf("submit %d status=%d body=%s", i, response.Code, response.Body.String())
		}
		if i == 0 {
			if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
				t.Fatal(err)
			}
		}
	}
	if backend.executes.Load() != 3 {
		t.Fatalf("backend executes=%d, want 3", backend.executes.Load())
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pruneTerminalProjection(ctx, tx, testProjectID, "", 3, 1<<30, 0); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var operations, outbox int
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE key_hash LIKE 'terminal-v2/%'").Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox").Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if operations != 2 || outbox != 2 {
		t.Fatalf("bounded projection operations=%d outbox=%d, want 2/2", operations, outbox)
	}
	if _, err := queue.get(ctx, first.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("oldest projection was not pruned: %v", err)
	}

	replayed := submitTerminalRequest(ctx, service, testToken, "terminal-projection-row-0",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] projection row 0"}}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("canonical replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	var restored operation
	if err := json.Unmarshal(replayed.Body.Bytes(), &restored); err != nil {
		t.Fatal(err)
	}
	if restored.ID != first.ID || !restored.IdempotentHit || backend.executes.Load() != 3 {
		t.Fatalf("restored=%#v executes=%d", restored, backend.executes.Load())
	}
}

func TestTerminalProjectionByteBoundDeletesCompleteRows(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	for i := 0; i < 2; i++ {
		response := submitTerminalRequest(ctx, service, testToken, fmt.Sprintf("terminal-projection-bytes-%d", i),
			fmt.Sprintf(`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] projection bytes %d"}}`, i))
		if response.Code != http.StatusOK {
			t.Fatalf("submit %d status=%d body=%s", i, response.Code, response.Body.String())
		}
	}
	var bytesBefore int64
	if err := queue.db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(length(op.payload_json)+length(COALESCE(op.result_json,X''))+
  COALESCE((SELECT SUM(length(ob.payload_json)+length(ob.event_type)) FROM outbox ob WHERE ob.operation_id=op.id),0)),0)
FROM operations op WHERE op.key_hash LIKE 'terminal-v2/%'`).Scan(&bytesBefore); err != nil {
		t.Fatal(err)
	}
	if bytesBefore < 2 {
		t.Fatalf("unexpected projection bytes=%d", bytesBefore)
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pruneTerminalProjection(ctx, tx, testProjectID, "", 100, bytesBefore-1, 0); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var operations, outbox int
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations").Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox").Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || outbox != 1 {
		t.Fatalf("byte-bounded projection operations=%d outbox=%d, want 1/1", operations, outbox)
	}
}

func TestCanonicalOutcomeRepairsConflictingLocalProjectionAcrossServices(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	firstQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	firstService, err := newTestGatewayService(ctx, testToken, firstQueue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		firstService.Close()
		_ = firstQueue.Close()
	}()

	key := "two-service-conflicting-local-key"
	canonicalBody := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] canonical winner"}}`
	firstResponse := submitTerminalRequest(ctx, firstService, testToken, key, canonicalBody)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("canonical submit status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	var canonical operation
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &canonical); err != nil {
		t.Fatal(err)
	}

	secondQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "second.db"))
	if err != nil {
		t.Fatal(err)
	}
	loserPayload, err := canonicalJSON(json.RawMessage(`{"title":"[beads-perf-lab] losing local request"}`))
	if err != nil {
		t.Fatal(err)
	}
	keyHash := terminalIdempotencyKeyHash(testStableKey, key)
	subjectHash := hmacHex(testStableKey, "subject\x00"+testSubject)
	loser, err := secondQueue.admit(ctx, operation{
		ID:        deterministicTerminalOperationID(testStableKey, testProjectID, subjectHash, keyHash),
		ProjectID: testProjectID, KeyHash: keyHash, SubjectHash: subjectHash,
		RequestHash: digestHex(bytes.Join([][]byte{[]byte(testProjectID), []byte("issue.create"), loserPayload}, []byte{0})),
		Kind:        "issue.create", Payload: loserPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	loser, ok, err := secondQueue.leaseNext(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease losing projection ok=%v err=%v", ok, err)
	}
	if err := secondQueue.markUnknown(ctx, loser, "simulated commit-time identity conflict"); err != nil {
		t.Fatal(err)
	}

	secondService, err := newTestGatewayService(ctx, testToken, secondQueue, backend)
	if err != nil {
		t.Fatalf("conflicting local projection bricked service startup: %v", err)
	}
	storedLoser, err := secondQueue.get(ctx, loser.ID)
	if err != nil || storedLoser.ErrorCode != "canonical_identity_conflict" || storedLoser.Status != statusUnknown {
		t.Fatalf("losing projection was not quarantined: op=%#v err=%v", storedLoser, err)
	}

	replayed := submitTerminalRequest(ctx, secondService, testToken, key, canonicalBody)
	if replayed.Code != http.StatusOK {
		t.Fatalf("canonical replay through losing service status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	var repaired operation
	if err := json.Unmarshal(replayed.Body.Bytes(), &repaired); err != nil {
		t.Fatal(err)
	}
	if repaired.ID != canonical.ID || repaired.RequestHash != canonical.RequestHash || repaired.Status != statusSucceeded {
		t.Fatalf("canonical projection was not restored: canonical=%#v repaired=%#v", canonical, repaired)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("losing service re-executed canonical operation: executes=%d", backend.executes.Load())
	}
	secondService.Close()

	// The repaired queue must remain restartable; the old losing worker identity
	// can no longer transition the canonical row.
	secondService, err = newTestGatewayService(ctx, testToken, secondQueue, backend)
	if err != nil {
		t.Fatalf("repaired service restart: %v", err)
	}
	defer func() {
		secondService.Close()
		_ = secondQueue.Close()
	}()
}

func TestLegacyAndTerminalIdempotencyBoundaryFailsClosed(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	key := "shared-cross-lane-key"
	legacyBody := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] legacy lane"}}`
	terminalBody := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal lane"}}`
	legacy := submitRequest(service, testToken, key, legacyBody)
	if legacy.Code != http.StatusOK {
		t.Fatalf("legacy status=%d body=%s", legacy.Code, legacy.Body.String())
	}
	terminal := submitTerminalRequest(ctx, service, testToken, key, terminalBody)
	if terminal.Code != http.StatusConflict || !strings.Contains(terminal.Body.String(), "idempotency_migration_required") {
		t.Fatalf("terminal status=%d body=%s", terminal.Code, terminal.Body.String())
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("cross-lane request executed: executes=%d", backend.executes.Load())
	}
	legacyHash := hmacHex(testStableKey, "key\x00"+key)
	terminalHash := terminalIdempotencyKeyHash(testStableKey, key)
	if legacyHash == terminalHash || isTerminalIdempotencyNamespace(legacyHash) || !isTerminalIdempotencyNamespace(terminalHash) {
		t.Fatalf("idempotency namespaces are not explicit: legacy=%q terminal=%q", legacyHash, terminalHash)
	}
	backend.mu.Lock()
	legacyOutcome := backend.receipts[legacyHash]
	terminalOutcome := backend.receipts[terminalHash]
	backend.mu.Unlock()
	if legacyOutcome == nil || legacyOutcome.Version != 1 || terminalOutcome != nil {
		t.Fatalf("lane outcomes legacy=%#v terminal=%#v", legacyOutcome, terminalOutcome)
	}
	service.Close()
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	// Queue loss must not erase the migration fence: the verified v1 receipt in
	// Dolt still prevents terminal-v2 from silently executing the same key.
	rebuilt, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "rebuilt.db"))
	if err != nil {
		t.Fatal(err)
	}
	rebuiltService, err := newTestGatewayService(ctx, testToken, rebuilt, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		rebuiltService.Close()
		_ = rebuilt.Close()
	}()
	rebuiltResponse := submitTerminalRequest(ctx, rebuiltService, testToken, key, terminalBody)
	if rebuiltResponse.Code != http.StatusConflict ||
		!strings.Contains(rebuiltResponse.Body.String(), "idempotency_migration_required") {
		t.Fatalf("rebuilt migration fence status=%d body=%s", rebuiltResponse.Code, rebuiltResponse.Body.String())
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("rebuilt migration fence executed backend: executes=%d", backend.executes.Load())
	}
}

func TestTerminalOperationsNeverReturnAccepted(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.block = make(chan struct{})
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	requestCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	response := submitTerminalRequest(requestCtx, service, testToken, "terminal-timeout-key",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal timeout"}}`)
	if response.Code == http.StatusAccepted {
		t.Fatalf("terminal lane returned forbidden 202 body=%s", response.Body.String())
	}
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
		t.Fatalf("terminal timeout status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	if strings.Contains(response.Body.String(), "terminal-timeout-key") {
		t.Fatalf("terminal no-ack response leaked key: %s", response.Body.String())
	}
}

func TestControlCapacityRemainsAvailableWhenWorkRequestsAreSaturated(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := newTestGatewayService(ctx, testToken, queue, newFakeBackend())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for len(service.requests) > 0 {
			<-service.requests
		}
		service.Close()
		_ = queue.Close()
	}()
	for i := 0; i < cap(service.requests); i++ {
		service.requests <- struct{}{}
	}

	overloaded := submitTerminalRequest(ctx, service, testToken, "terminal-work-limit-key",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] work limit"}}`)
	if overloaded.Code != http.StatusServiceUnavailable || overloaded.Header().Get("Retry-After") == "" ||
		!strings.Contains(overloaded.Body.String(), "request_limit") {
		t.Fatalf("terminal work saturation status=%d headers=%v body=%s",
			overloaded.Code, overloaded.Header(), overloaded.Body.String())
	}

	health := httptest.NewRecorder()
	service.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health probe was starved: status=%d body=%s", health.Code, health.Body.String())
	}
	ready := httptest.NewRecorder()
	service.ServeHTTP(ready, authorizedRequest(http.MethodGet,
		"/v1/projects/"+testProjectID+"/readyz", nil, testToken))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready probe was starved: status=%d body=%s", ready.Code, ready.Body.String())
	}
}

func TestTerminalQueueSaturationIsRetrySafeNoAcknowledgement(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	now := time.Now().UTC().UnixNano()
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxPendingOperations; i++ {
		id := uuid.NewString()
		_, err = tx.ExecContext(ctx, `
INSERT INTO operations (
  id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
  status, attempts, available_at_ns, lease_until_ns, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, 'issue.create', '{}', 'accepted', 0, ?, 0, ?, ?)`,
			id, testProjectID, "legacy/"+id, "subject", "request-"+id, now, now, now)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	service := &gatewayService{queue: queue}
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/terminal-operations", nil)
	response := httptest.NewRecorder()
	_, ok := service.admitPreparedOperation(response, request, operation{
		ID: uuid.NewString(), ProjectID: testProjectID, KeyHash: "terminal-v2/full",
		SubjectHash: "subject", RequestHash: "request", Kind: "issue.create", Payload: json.RawMessage(`{}`),
	}, true)
	if ok || response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" ||
		!strings.Contains(response.Body.String(), "queue_full") {
		t.Fatalf("terminal queue saturation ok=%v status=%d headers=%v body=%s",
			ok, response.Code, response.Header(), response.Body.String())
	}
}

func TestTerminalUnclassifiedBackendFailureIsNotAcknowledgedPermanent(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.execErr = errors.New("unclassified backend invariant failure")
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	requestCtx, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
	defer cancel()
	response := submitTerminalRequest(requestCtx, service, testToken, "terminal-ambiguous-error-key",
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal ambiguous error"}}`)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
		t.Fatalf("ambiguous terminal status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	backend.mu.Lock()
	outcomes := len(backend.receipts)
	backend.mu.Unlock()
	if outcomes != 0 {
		t.Fatalf("ambiguous failure was made canonical: outcomes=%d", outcomes)
	}
}

func TestTerminalOperationsRecoverReceiptAfterEmptyQueueRebuild(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal rebuild"}}`
	key := "terminal-rebuild-key"

	firstQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	firstService, err := newTestGatewayService(ctx, testToken, firstQueue, backend)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse := submitTerminalRequest(ctx, firstService, testToken, key, body)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first terminal status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	var first operation
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	firstService.Close()
	if err := firstQueue.Close(); err != nil {
		t.Fatal(err)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("initial executes=%d", backend.executes.Load())
	}

	secondQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "rebuilt-empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	secondService, err := newTestGatewayService(ctx, testToken, secondQueue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		secondService.Close()
		_ = secondQueue.Close()
	}()
	conflictResponse := submitTerminalRequest(ctx, secondService, testToken, key,
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] conflicting rebuild"}}`)
	if conflictResponse.Code != http.StatusConflict {
		t.Fatalf("rebuilt conflict status=%d body=%s", conflictResponse.Code, conflictResponse.Body.String())
	}
	secondResponse := submitTerminalRequest(ctx, secondService, testToken, key, body)
	if secondResponse.Code != http.StatusOK {
		t.Fatalf("rebuilt terminal status=%d body=%s", secondResponse.Code, secondResponse.Body.String())
	}
	var second operation
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.Status != statusSucceeded || !second.IdempotentHit {
		t.Fatalf("recovered operation first=%#v second=%#v", first, second)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("receipt recovery re-executed backend: executes=%d", backend.executes.Load())
	}
	events, err := secondQueue.outbox(ctx, second.ID)
	if err != nil || len(events) != 1 {
		t.Fatalf("recovered outbox=%#v err=%v", events, err)
	}
}

func TestTerminalPermanentFailureRecoversAfterEmptyQueueRebuild(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	backend.execErr = permanentf("synthetic request is invalid")
	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal failed rebuild"}}`
	key := "terminal-failed-rebuild-key"

	firstQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	firstService, err := newTestGatewayService(ctx, testToken, firstQueue, backend)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse := submitTerminalRequest(ctx, firstService, testToken, key, body)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first failed terminal status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	var first operation
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != statusFailed || first.ErrorCode != "operation_failed" {
		t.Fatalf("first failed outcome=%#v", first)
	}
	firstService.Close()
	if err := firstQueue.Close(); err != nil {
		t.Fatal(err)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("initial failed executes=%d", backend.executes.Load())
	}

	secondQueue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "rebuilt-empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	secondService, err := newTestGatewayService(ctx, testToken, secondQueue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		secondService.Close()
		_ = secondQueue.Close()
	}()
	conflict := submitTerminalRequest(ctx, secondService, testToken, key,
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] different failed rebuild"}}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("rebuilt failed conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	replayed := submitTerminalRequest(ctx, secondService, testToken, key, body)
	if replayed.Code != http.StatusOK {
		t.Fatalf("rebuilt failed status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	var second operation
	if err := json.Unmarshal(replayed.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.Status != statusFailed || !second.IdempotentHit {
		t.Fatalf("failed projection recovery first=%#v second=%#v", first, second)
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("failed outcome recovery executed backend: executes=%d", backend.executes.Load())
	}
	events, err := secondQueue.outbox(ctx, second.ID)
	if err != nil || len(events) != 1 || events[0].EventType != "issue.create.failed" {
		t.Fatalf("failed recovered outbox=%#v err=%v", events, err)
	}
}

func TestTerminalQueueOnlyFailureIsNeverAcknowledged(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"title":"[beads-perf-lab] queue-only terminal"}`)
	key := "terminal-queue-only-key"
	keyHash := terminalIdempotencyKeyHash(testStableKey, key)
	subjectHash := hmacHex(testStableKey, "subject\x00"+testSubject)
	op, err := queue.admit(ctx, operation{
		ID:        deterministicTerminalOperationID(testStableKey, testProjectID, subjectHash, keyHash),
		ProjectID: testProjectID, KeyHash: keyHash, SubjectHash: subjectHash,
		RequestHash: digestHex(bytes.Join([][]byte{[]byte(testProjectID), []byte("issue.create"), payload}, []byte{0})),
		Kind:        "issue.create", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err := queue.leaseNext(ctx, time.Minute)
	if err != nil || !ok || leased.ID != op.ID {
		t.Fatalf("lease=%#v ok=%v err=%v", leased, ok, err)
	}
	if err := queue.markFailed(ctx, leased, "operation_failed", "operation failed"); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()
	response := submitTerminalRequest(ctx, service, testToken, key,
		`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] queue-only terminal"}}`)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
		t.Fatalf("queue-only failure status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
}

func TestTerminalOperationsRedactFailures(t *testing.T) {
	ctx := context.Background()
	const sentinel = "password=terminal-secret database=foreign issue=private"
	for _, test := range []struct {
		name        string
		configure   func(*fakeBackend)
		wantStatus  int
		wantOpState string
	}{
		{name: "durable failure", configure: func(b *fakeBackend) { b.execErr = permanentf(sentinel) }, wantStatus: http.StatusOK, wantOpState: statusFailed},
		{name: "lookup unavailable", configure: func(b *fakeBackend) { b.lookupErr = errors.New(sentinel) }, wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
			if err != nil {
				t.Fatal(err)
			}
			backend := newFakeBackend()
			test.configure(backend)
			service, err := newTestGatewayService(ctx, testToken, queue, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				service.Close()
				_ = queue.Close()
			}()
			response := submitTerminalRequest(ctx, service, testToken, "terminal-redaction-key",
				`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] terminal redaction"}}`)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if strings.Contains(response.Body.String(), sentinel) || strings.Contains(response.Body.String(), "terminal-secret") {
				t.Fatalf("terminal response leaked backend detail: %s", response.Body.String())
			}
			if test.wantOpState != "" {
				var op operation
				if err := json.Unmarshal(response.Body.Bytes(), &op); err != nil {
					t.Fatal(err)
				}
				if op.Status != test.wantOpState || op.ErrorMessage != "operation rejected" {
					t.Fatalf("redacted terminal operation=%#v", op)
				}
			}
		})
	}
}

func submitRequest(service *gatewayService, token []byte, key, body string) *httptest.ResponseRecorder {
	request := authorizedRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/operations?wait_ms=2000", strings.NewReader(body), token)
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	return response
}

func submitTerminalRequest(
	ctx context.Context, service *gatewayService, token []byte, key, body string,
) *httptest.ResponseRecorder {
	request := authorizedRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/terminal-operations", strings.NewReader(body), token)
	request = request.WithContext(ctx)
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	return response
}

func authorizedRequest(method, target string, body io.Reader, token []byte) *http.Request {
	request := httptest.NewRequest(method, target, body)
	request.Header.Set("Authorization", "Bearer "+string(token))
	request.Header.Set("X-Beads-Subject", "test-agent")
	request.Header.Set("X-Beads-Project-ID", testProjectID)
	return request
}
