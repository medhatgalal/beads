package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var errIdempotencyConflict = errors.New("idempotency key is already bound to another request or identity")
var errQueueFull = errors.New("gateway operation queue is full")
var errStaleQueueIdentity = errors.New("queue operation identity was superseded by canonical state")

const (
	maxPendingOperations       = 1000
	maxPendingBytes            = 64 << 20
	maxTerminalProjectionRows  = 20_000
	maxTerminalProjectionBytes = 128 << 20
	maxRetryExhaustedRows      = 100
)

type operationQueue struct {
	db      *sql.DB
	binding queueBinding
}

type queueBinding struct {
	ProjectID        string
	Database         string
	IdempotencyKeyID string
	SubjectID        string
}

func openOperationQueue(ctx context.Context, path string, binding queueBinding) (*operationQueue, error) {
	if binding.ProjectID == "" || binding.Database == "" || binding.IdempotencyKeyID == "" || binding.SubjectID == "" {
		return nil, fmt.Errorf("queue binding fields are required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("queue path: %w", err)
	}
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("queue path must not be a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect queue path: %w", err)
	}
	file, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create queue: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure queue: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close queue bootstrap file: %w", err)
	}
	u := &url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()

	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open queue: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	queue := &operationQueue{db: db, binding: binding}
	if err := queue.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return queue, nil
}

func (q *operationQueue) init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  subject_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json BLOB NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('accepted','running','retry_wait','succeeded','failed','unknown')),
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at_ns INTEGER NOT NULL,
  lease_until_ns INTEGER NOT NULL DEFAULT 0,
  result_json BLOB,
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at_ns INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  UNIQUE(project_id, key_hash)
);
CREATE INDEX IF NOT EXISTS operations_runnable_idx
  ON operations(status, available_at_ns, created_at_ns);
CREATE INDEX IF NOT EXISTS operations_project_runnable_idx
  ON operations(project_id, status, available_at_ns, created_at_ns);
CREATE TABLE IF NOT EXISTS outbox (
  operation_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload_json BLOB NOT NULL,
  created_at_ns INTEGER NOT NULL,
  PRIMARY KEY(operation_id, sequence),
  FOREIGN KEY(operation_id) REFERENCES operations(id)
);
CREATE TABLE IF NOT EXISTS gateway_binding (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  schema_version INTEGER NOT NULL CHECK(schema_version = 1),
  project_id TEXT NOT NULL,
  database_name TEXT NOT NULL,
  idempotency_key_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);
`
	if _, err := q.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize queue: %w", err)
	}
	return q.verifyBinding(ctx)
}

func (q *operationQueue) verifyBinding(ctx context.Context) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("queue binding begin: %w", err)
	}
	defer tx.Rollback()

	var schemaVersion int
	var projectID, database, keyID, subjectID string
	err = tx.QueryRowContext(ctx, `
SELECT schema_version, project_id, database_name, idempotency_key_id, subject_id
FROM gateway_binding WHERE singleton = 1`).Scan(&schemaVersion, &projectID, &database, &keyID, &subjectID)
	if errors.Is(err, sql.ErrNoRows) {
		var operations int64
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations").Scan(&operations); err != nil {
			return fmt.Errorf("queue legacy state check: %w", err)
		}
		if operations != 0 {
			return fmt.Errorf("unbound non-empty queue must not be adopted")
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO gateway_binding(singleton, schema_version, project_id, database_name, idempotency_key_id, subject_id, created_at_ns)
VALUES(1, 1, ?, ?, ?, ?, ?)`, q.binding.ProjectID, q.binding.Database, q.binding.IdempotencyKeyID, q.binding.SubjectID, time.Now().UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("queue binding insert: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("queue binding read: %w", err)
	} else if schemaVersion != 1 || projectID != q.binding.ProjectID || database != q.binding.Database ||
		keyID != q.binding.IdempotencyKeyID || subjectID != q.binding.SubjectID {
		return fmt.Errorf("queue binding mismatch")
	}

	var foreignRows int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE project_id <> ?", q.binding.ProjectID).Scan(&foreignRows); err != nil {
		return fmt.Errorf("queue foreign state check: %w", err)
	}
	if foreignRows != 0 {
		return fmt.Errorf("queue contains operations for another project")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("queue binding commit: %w", err)
	}
	return nil
}

func (q *operationQueue) Close() error {
	if q == nil || q.db == nil {
		return nil
	}
	return q.db.Close()
}

func (q *operationQueue) quickCheck(ctx context.Context) error {
	var result string
	if err := q.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("queue quick check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("queue quick check returned %q", result)
	}
	return nil
}

func (q *operationQueue) admit(ctx context.Context, in operation) (operation, error) {
	if in.ProjectID != q.binding.ProjectID {
		return operation{}, fmt.Errorf("operation project does not match queue binding")
	}
	now := time.Now().UTC()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return operation{}, fmt.Errorf("admit begin: %w", err)
	}
	defer tx.Rollback()

	existing, err := scanOperation(tx.QueryRowContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations WHERE project_id = ? AND key_hash = ?`, in.ProjectID, in.KeyHash))
	switch {
	case err == nil:
		if existing.SubjectHash != in.SubjectHash || existing.RequestHash != in.RequestHash || existing.Kind != in.Kind {
			return operation{}, errIdempotencyConflict
		}
		existing.IdempotentHit = true
		return existing, nil
	case !errors.Is(err, sql.ErrNoRows):
		return operation{}, fmt.Errorf("admit lookup: %w", err)
	}
	if isTerminalIdempotencyNamespace(in.KeyHash) {
		if err := pruneTerminalProjection(ctx, tx, q.binding.ProjectID, "", maxTerminalProjectionRows,
			maxTerminalProjectionBytes, int64(len(in.Payload))); err != nil {
			return operation{}, fmt.Errorf("admit terminal projection bound: %w", err)
		}
	}
	var pendingCount, pendingBytes int64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(length(payload_json)), 0)
FROM operations WHERE project_id = ? AND status NOT IN ('succeeded','failed')`, q.binding.ProjectID).Scan(&pendingCount, &pendingBytes); err != nil {
		return operation{}, fmt.Errorf("admit capacity: %w", err)
	}
	if pendingCount >= maxPendingOperations || pendingBytes+int64(len(in.Payload)) > maxPendingBytes {
		return operation{}, errQueueFull
	}

	if in.ID == "" {
		in.ID = uuid.NewString()
	}
	in.Status = statusAccepted
	in.Attempts = 0
	in.AvailableAt = now
	in.CreatedAt = now
	in.UpdatedAt = now
	_, err = tx.ExecContext(ctx, `
INSERT INTO operations (
  id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
  status, attempts, available_at_ns, lease_until_ns, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?, ?)`,
		in.ID, in.ProjectID, in.KeyHash, in.SubjectHash, in.RequestHash, in.Kind, []byte(in.Payload),
		in.Status, now.UnixNano(), now.UnixNano(), now.UnixNano())
	if err != nil {
		return operation{}, fmt.Errorf("admit insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return operation{}, fmt.Errorf("admit commit: %w", err)
	}
	return in, nil
}

func (q *operationQueue) get(ctx context.Context, id string) (operation, error) {
	return scanOperation(q.db.QueryRowContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations WHERE id = ? AND project_id = ?`, id, q.binding.ProjectID))
}

func (q *operationQueue) getByKey(ctx context.Context, projectID, keyHash string) (operation, error) {
	if projectID != q.binding.ProjectID {
		return operation{}, fmt.Errorf("operation project does not match queue binding")
	}
	return scanOperation(q.db.QueryRowContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations WHERE project_id = ? AND key_hash = ?`, projectID, keyHash))
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOperation(row rowScanner) (operation, error) {
	var op operation
	var payload, result []byte
	var availableNS, leaseNS, createdNS, updatedNS int64
	err := row.Scan(
		&op.ID, &op.ProjectID, &op.KeyHash, &op.SubjectHash, &op.RequestHash, &op.Kind, &payload,
		&op.Status, &op.Attempts, &availableNS, &leaseNS, &result,
		&op.ErrorCode, &op.ErrorMessage, &createdNS, &updatedNS,
	)
	if err != nil {
		return operation{}, err
	}
	op.Payload = append(op.Payload[:0], payload...)
	op.Result = append(op.Result[:0], result...)
	op.AvailableAt = fromUnixNano(availableNS)
	op.LeaseUntil = fromUnixNano(leaseNS)
	op.CreatedAt = fromUnixNano(createdNS)
	op.UpdatedAt = fromUnixNano(updatedNS)
	return op, nil
}

func sameOperationIdentity(a, b operation) bool {
	return a.ID == b.ID && a.ProjectID == b.ProjectID && a.KeyHash == b.KeyHash &&
		a.SubjectHash == b.SubjectHash && a.RequestHash == b.RequestHash && a.Kind == b.Kind
}

func (q *operationQueue) classifyTransitionMiss(ctx context.Context, op operation, transition string) error {
	current, err := q.get(ctx, op.ID)
	if err != nil {
		return fmt.Errorf("%s inspect current operation: %w", transition, err)
	}
	if !sameOperationIdentity(current, op) {
		return errStaleQueueIdentity
	}
	return fmt.Errorf("%s transition lost from status %q", transition, current.Status)
}

func fromUnixNano(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

func (q *operationQueue) leaseNext(ctx context.Context, lease time.Duration) (operation, bool, error) {
	now := time.Now().UTC()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return operation{}, false, fmt.Errorf("lease begin: %w", err)
	}
	defer tx.Rollback()

	op, err := scanOperation(tx.QueryRowContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations
WHERE project_id = ? AND status IN ('accepted','retry_wait') AND available_at_ns <= ?
ORDER BY created_at_ns, id LIMIT 1`, q.binding.ProjectID, now.UnixNano()))
	if errors.Is(err, sql.ErrNoRows) {
		return operation{}, false, nil
	}
	if err != nil {
		return operation{}, false, fmt.Errorf("lease select: %w", err)
	}
	op.Status = statusRunning
	op.Attempts++
	op.LeaseUntil = now.Add(lease)
	op.UpdatedAt = now
	res, err := tx.ExecContext(ctx, `
UPDATE operations
SET status = 'running', attempts = ?, lease_until_ns = ?, updated_at_ns = ?
WHERE id = ? AND project_id = ? AND status IN ('accepted','retry_wait')`,
		op.Attempts, op.LeaseUntil.UnixNano(), now.UnixNano(), op.ID, q.binding.ProjectID)
	if err != nil {
		return operation{}, false, fmt.Errorf("lease update: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return operation{}, false, fmt.Errorf("lease lost: rows=%d err=%v", n, err)
	}
	if err := tx.Commit(); err != nil {
		return operation{}, false, fmt.Errorf("lease commit: %w", err)
	}
	return op, true, nil
}

// restoreCanonicalOutcome makes an already-validated Dolt terminal-v2 outcome
// authoritative over the rebuildable SQLite projection. A losing local request
// may have the same deterministic operation ID and key but a different request
// hash; replacing that row is safe only because the caller supplies an exact
// canonical receipt and mark/retry/fail transitions are identity-fenced.
func (q *operationQueue) restoreCanonicalOutcome(
	ctx context.Context, in operation, receipt *doltReceipt,
) (operation, error) {
	if in.ProjectID != q.binding.ProjectID || !isTerminalIdempotencyNamespace(in.KeyHash) || in.ID == "" {
		return operation{}, fmt.Errorf("canonical restore requires terminal-v2 identity")
	}
	if receipt == nil {
		return operation{}, fmt.Errorf("canonical restore requires an outcome")
	}
	if err := validateReceipt(*receipt, in); err != nil {
		return operation{}, err
	}
	targetStatus := receipt.Status
	if targetStatus != statusSucceeded && targetStatus != statusFailed {
		return operation{}, fmt.Errorf("canonical restore status is invalid")
	}
	now := time.Now().UTC()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return operation{}, fmt.Errorf("canonical restore begin: %w", err)
	}
	defer tx.Rollback()

	existing, err := scanOperation(tx.QueryRowContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations WHERE project_id=? AND key_hash=?`, q.binding.ProjectID, in.KeyHash))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		in.Status = targetStatus
		in.Attempts = 0
		in.AvailableAt = now
		in.LeaseUntil = time.Time{}
		in.Result = append(json.RawMessage(nil), receipt.Result...)
		in.ErrorCode = receipt.ErrorCode
		in.ErrorMessage = receipt.ErrorMessage
		in.CreatedAt = now
		in.UpdatedAt = now
		_, err = tx.ExecContext(ctx, `
INSERT INTO operations (
  id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
  status, attempts, available_at_ns, lease_until_ns, result_json,
  error_code, error_message, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?, ?, ?, ?, ?)`,
			in.ID, in.ProjectID, in.KeyHash, in.SubjectHash, in.RequestHash, in.Kind, []byte(in.Payload),
			in.Status, now.UnixNano(), []byte(in.Result), in.ErrorCode, in.ErrorMessage,
			now.UnixNano(), now.UnixNano())
		if err != nil {
			return operation{}, fmt.Errorf("canonical restore insert operation: %w", err)
		}
	case err != nil:
		return operation{}, fmt.Errorf("canonical restore read projection: %w", err)
	default:
		if existing.ID != in.ID {
			return operation{}, fmt.Errorf("canonical restore operation ID conflicts with local projection")
		}
		_, err = tx.ExecContext(ctx, `
UPDATE operations SET subject_hash=?, request_hash=?, kind=?, payload_json=?, status=?,
  result_json=?, error_code=?, error_message=?, available_at_ns=?, lease_until_ns=0, updated_at_ns=?
WHERE id=? AND project_id=? AND key_hash=?`, in.SubjectHash, in.RequestHash, in.Kind,
			[]byte(in.Payload), targetStatus, []byte(receipt.Result), receipt.ErrorCode, receipt.ErrorMessage,
			now.UnixNano(), now.UnixNano(), in.ID, q.binding.ProjectID, in.KeyHash)
		if err != nil {
			return operation{}, fmt.Errorf("canonical restore replace projection: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE operation_id=?", in.ID); err != nil {
			return operation{}, fmt.Errorf("canonical restore clear projected outbox: %w", err)
		}
	}

	event := receipt.Outbox
	if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox(operation_id, sequence, event_type, payload_json, created_at_ns)
VALUES(?, ?, ?, ?, ?)`, event.OperationID, event.Sequence, event.EventType,
		[]byte(event.Payload), event.CreatedAt.UnixNano()); err != nil {
		return operation{}, fmt.Errorf("canonical restore insert outbox: %w", err)
	}
	if err := pruneTerminalProjection(ctx, tx, q.binding.ProjectID, in.ID, maxTerminalProjectionRows,
		maxTerminalProjectionBytes, 0); err != nil {
		return operation{}, fmt.Errorf("canonical restore projection bound: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return operation{}, fmt.Errorf("canonical restore commit: %w", err)
	}
	return q.get(ctx, in.ID)
}

func (q *operationQueue) markSucceeded(ctx context.Context, op operation, receipt *doltReceipt) error {
	if receipt != nil && receipt.Status == statusFailed {
		return fmt.Errorf("failed canonical outcome cannot use success projection")
	}
	return q.markOutcome(ctx, op, receipt)
}

// markOutcome projects an immutable Dolt outcome into rebuildable SQLite. Both
// the operation row and outbox row are compared on conflict; neither is an
// upsert that can silently replace different canonical state.
func (q *operationQueue) markOutcome(ctx context.Context, op operation, receipt *doltReceipt) error {
	if op.ProjectID != q.binding.ProjectID {
		return fmt.Errorf("operation project does not match queue binding")
	}
	if receipt == nil {
		return fmt.Errorf("success receipt is required")
	}
	if err := validateReceipt(*receipt, op); err != nil {
		return err
	}
	targetStatus := receipt.Status
	if targetStatus == "" {
		targetStatus = statusSucceeded
	}
	now := time.Now().UTC()
	result := receipt.Result
	event := receipt.Outbox
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("success begin: %w", err)
	}
	defer tx.Rollback()
	current := operation{ID: op.ID, ProjectID: q.binding.ProjectID}
	var currentResult []byte
	if err := tx.QueryRowContext(ctx, `
SELECT key_hash, subject_hash, request_hash, kind, status, result_json
FROM operations WHERE id=? AND project_id=?`, op.ID, q.binding.ProjectID).Scan(
		&current.KeyHash, &current.SubjectHash, &current.RequestHash, &current.Kind,
		&current.Status, &currentResult,
	); err != nil {
		return fmt.Errorf("success current state: %w", err)
	}
	if !sameOperationIdentity(current, op) {
		return errStaleQueueIdentity
	}
	if current.Status == targetStatus {
		var currentCode, currentMessage string
		if err := tx.QueryRowContext(ctx, `
SELECT error_code, error_message FROM operations WHERE id=? AND project_id=?`,
			op.ID, q.binding.ProjectID).Scan(&currentCode, &currentMessage); err != nil {
			return fmt.Errorf("canonical outcome current failure fields: %w", err)
		}
		var eventType string
		var payload []byte
		var sequence int
		err := tx.QueryRowContext(ctx, `
SELECT sequence, event_type, payload_json FROM outbox
WHERE operation_id=? AND sequence=0`, op.ID).Scan(&sequence, &eventType, &payload)
		if err != nil || !bytes.Equal(currentResult, result) || currentCode != receipt.ErrorCode ||
			currentMessage != receipt.ErrorMessage || sequence != event.Sequence ||
			eventType != event.EventType || !bytes.Equal(payload, event.Payload) {
			return fmt.Errorf("canonical operation projection mismatch")
		}
		return nil
	}
	if current.Status == statusSucceeded || current.Status == statusFailed {
		return fmt.Errorf("canonical outcome conflicts with terminal projection %q", current.Status)
	}
	if current.Status != statusAccepted && current.Status != statusRunning && current.Status != statusUnknown {
		return fmt.Errorf("canonical outcome transition from invalid state %q", current.Status)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE operations SET status=?, result_json=?, error_code=?, error_message=?,
	  lease_until_ns=0, updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status IN ('accepted','running','unknown')`,
		targetStatus, []byte(result), receipt.ErrorCode, receipt.ErrorMessage,
		now.UnixNano(), op.ID, q.binding.ProjectID, op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("canonical outcome update: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("canonical outcome transition lost: rows=%d err=%v", n, err)
	}
	insert, err := tx.ExecContext(ctx, `
INSERT INTO outbox(operation_id, sequence, event_type, payload_json, created_at_ns)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(operation_id, sequence) DO NOTHING`, event.OperationID, event.Sequence,
		event.EventType, []byte(event.Payload), event.CreatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("canonical outcome outbox insert: %w", err)
	}
	if n, err := insert.RowsAffected(); err != nil {
		return fmt.Errorf("canonical outcome outbox insert rows: %w", err)
	} else if n == 0 {
		var sequence int
		var eventType string
		var payload []byte
		if err := tx.QueryRowContext(ctx, `
SELECT sequence, event_type, payload_json FROM outbox
WHERE operation_id=? AND sequence=?`, event.OperationID, event.Sequence).Scan(&sequence, &eventType, &payload); err != nil ||
			sequence != event.Sequence || eventType != event.EventType || !bytes.Equal(payload, event.Payload) {
			return fmt.Errorf("canonical outcome outbox conflict")
		}
	}
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		if err := pruneTerminalProjection(ctx, tx, q.binding.ProjectID, op.ID, maxTerminalProjectionRows,
			maxTerminalProjectionBytes, 0); err != nil {
			return fmt.Errorf("canonical outcome projection bound: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("canonical outcome commit: %w", err)
	}
	return nil
}

// pruneTerminalProjection bounds only terminal-v2 SQLite rows. Their canonical
// outcome lives in Dolt and the deterministic terminal endpoint can reconstruct
// a pruned row without re-executing the operation. Nonterminal and legacy rows
// are never deleted here. preserveID protects the response currently being
// finalized from being selected as the oldest row.
func pruneTerminalProjection(
	ctx context.Context, tx *sql.Tx, projectID, preserveID string, maxRows, maxBytes int64, incomingBytes int64,
) error {
	if projectID == "" || maxRows < 1 || maxBytes < 1 || incomingBytes < 0 || incomingBytes > maxBytes {
		return fmt.Errorf("invalid terminal projection bound")
	}
	var rows, bytesUsed int64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(
  length(op.payload_json) + length(COALESCE(op.result_json, X'')) +
  length(op.error_code) + length(op.error_message) +
  COALESCE((SELECT SUM(length(ob.payload_json) + length(ob.event_type))
            FROM outbox ob WHERE ob.operation_id=op.id), 0)
), 0)
FROM operations op
WHERE op.project_id=? AND op.key_hash LIKE 'terminal-v2/%'
  AND op.status IN ('succeeded','failed')`, projectID).Scan(&rows, &bytesUsed); err != nil {
		return fmt.Errorf("count terminal projection: %w", err)
	}
	for rows >= maxRows || bytesUsed+incomingBytes > maxBytes {
		var id string
		var rowBytes int64
		err := tx.QueryRowContext(ctx, `
SELECT op.id,
  length(op.payload_json) + length(COALESCE(op.result_json, X'')) +
  length(op.error_code) + length(op.error_message) +
  COALESCE((SELECT SUM(length(ob.payload_json) + length(ob.event_type))
            FROM outbox ob WHERE ob.operation_id=op.id), 0)
FROM operations op
WHERE op.project_id=? AND op.key_hash LIKE 'terminal-v2/%'
  AND op.status IN ('succeeded','failed') AND op.id<>?
ORDER BY op.updated_at_ns, op.id LIMIT 1`, projectID, preserveID).Scan(&id, &rowBytes)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("terminal projection cannot satisfy bound while preserving current outcome")
		}
		if err != nil {
			return fmt.Errorf("select terminal projection victim: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE operation_id=?", id); err != nil {
			return fmt.Errorf("delete terminal projection outbox: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
DELETE FROM operations WHERE id=? AND key_hash LIKE 'terminal-v2/%'
  AND status IN ('succeeded','failed')`, id)
		if err != nil {
			return fmt.Errorf("delete terminal projection operation: %w", err)
		}
		if deleted, err := result.RowsAffected(); err != nil || deleted != 1 {
			return fmt.Errorf("delete terminal projection operation rows=%d err=%v", deleted, err)
		}
		rows--
		bytesUsed -= rowBytes
		if bytesUsed < 0 {
			return fmt.Errorf("terminal projection byte accounting underflow")
		}
	}
	return nil
}

func (q *operationQueue) markFailed(ctx context.Context, op operation, code, message string) error {
	now := time.Now().UTC()
	res, err := q.db.ExecContext(ctx, `
	UPDATE operations SET status='failed', error_code=?, error_message=?, lease_until_ns=0,
	  updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status IN ('running','failed')`, code, truncate(message, 1024), now.UnixNano(),
		op.ID, q.binding.ProjectID, op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "mark failed")
		}
		return fmt.Errorf("mark failed transition lost: rows=%d err=%v", n, err)
	}
	return nil
}

func (q *operationQueue) markRetry(ctx context.Context, op operation, message string, delay time.Duration) error {
	now := time.Now().UTC()
	res, err := q.db.ExecContext(ctx, `
	UPDATE operations SET status='retry_wait', error_code='transient', error_message=?,
	  available_at_ns=?, lease_until_ns=0, updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status IN ('running','retry_wait')`, truncate(message, 1024),
		now.Add(delay).UnixNano(), now.UnixNano(), op.ID, q.binding.ProjectID,
		op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("mark retry: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "mark retry")
		}
		return fmt.Errorf("mark retry transition lost: rows=%d err=%v", n, err)
	}
	return nil
}

func (q *operationQueue) markUnknownRunning(ctx context.Context) error {
	now := time.Now().UTC()
	_, err := q.db.ExecContext(ctx, `
UPDATE operations SET status='unknown', error_code='needs_reconcile',
  error_message='worker stopped with an ambiguous outcome', updated_at_ns=?
WHERE project_id=? AND status='running'`, now.UnixNano(), q.binding.ProjectID)
	if err != nil {
		return fmt.Errorf("mark running unknown: %w", err)
	}
	return nil
}

func (q *operationQueue) markRetryExhausted(ctx context.Context, op operation, message string) error {
	now := time.Now().UTC()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mark retry exhausted begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `
UPDATE operations SET status='unknown', error_code='retry_exhausted', error_message=?,
  lease_until_ns=0, updated_at_ns=?
WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
  AND status IN ('running','unknown')`, truncate(message, 1024), now.UnixNano(),
		op.ID, q.binding.ProjectID, op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("mark retry exhausted: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "mark retry exhausted")
		}
		return fmt.Errorf("mark retry exhausted transition lost: rows=%d err=%v", n, err)
	}
	rows, err := tx.QueryContext(ctx, `
SELECT id FROM operations
WHERE project_id=? AND status='unknown' AND error_code='retry_exhausted'
ORDER BY updated_at_ns DESC, id DESC LIMIT -1 OFFSET ?`, q.binding.ProjectID, maxRetryExhaustedRows)
	if err != nil {
		return fmt.Errorf("list expired retry-exhausted projections: %w", err)
	}
	var expired []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan expired retry-exhausted projection: %w", err)
		}
		expired = append(expired, id)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close expired retry-exhausted projection rows: %w", err)
	}
	for _, id := range expired {
		if _, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE operation_id=?", id); err != nil {
			return fmt.Errorf("delete retry-exhausted outbox: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM operations WHERE id=? AND project_id=? AND status='unknown' AND error_code='retry_exhausted'`,
			id, q.binding.ProjectID); err != nil {
			return fmt.Errorf("delete retry-exhausted projection: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark retry exhausted commit: %w", err)
	}
	return nil
}

func (q *operationQueue) markUnknown(ctx context.Context, op operation, message string) error {
	now := time.Now().UTC()
	res, err := q.db.ExecContext(ctx, `
	UPDATE operations SET status='unknown', error_code='needs_reconcile', error_message=?,
	  lease_until_ns=0, updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status IN ('running','unknown')`, truncate(message, 1024), now.UnixNano(),
		op.ID, q.binding.ProjectID, op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("mark unknown: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "mark unknown")
		}
		return fmt.Errorf("mark unknown transition lost: rows=%d err=%v", n, err)
	}
	return nil
}

func (q *operationQueue) unknown(ctx context.Context) ([]operation, error) {
	rows, err := q.db.QueryContext(ctx, `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind, payload_json,
       status, attempts, available_at_ns, lease_until_ns, result_json,
       error_code, error_message, created_at_ns, updated_at_ns
FROM operations
WHERE project_id=? AND status='unknown' AND error_code='needs_reconcile'
ORDER BY created_at_ns, id`, q.binding.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("unknown query: %w", err)
	}
	defer rows.Close()
	var out []operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, fmt.Errorf("unknown scan: %w", err)
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (q *operationQueue) requeueReconciled(ctx context.Context, op operation) error {
	now := time.Now().UTC()
	res, err := q.db.ExecContext(ctx, `
	UPDATE operations SET status='accepted', available_at_ns=?, lease_until_ns=0,
	  error_code='', error_message='', updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status='unknown' AND error_code='needs_reconcile'`, now.UnixNano(), now.UnixNano(), op.ID, q.binding.ProjectID,
		op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("requeue reconciled: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "requeue reconciled")
		}
		return fmt.Errorf("requeue reconciled transition lost: rows=%d err=%v", n, err)
	}
	return nil
}

func (q *operationQueue) quarantineIdentityConflict(ctx context.Context, op operation) error {
	now := time.Now().UTC()
	res, err := q.db.ExecContext(ctx, `
	UPDATE operations SET status='unknown', error_code='canonical_identity_conflict',
	  error_message='local projection conflicts with canonical identity', lease_until_ns=0, updated_at_ns=?
	WHERE id=? AND project_id=? AND key_hash=? AND subject_hash=? AND request_hash=? AND kind=?
	  AND status IN ('accepted','running','retry_wait','unknown')`, now.UnixNano(), op.ID,
		q.binding.ProjectID, op.KeyHash, op.SubjectHash, op.RequestHash, op.Kind)
	if err != nil {
		return fmt.Errorf("quarantine canonical identity conflict: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err == nil && n == 0 {
			return q.classifyTransitionMiss(ctx, op, "quarantine canonical identity conflict")
		}
		return fmt.Errorf("quarantine canonical identity conflict lost: rows=%d err=%v", n, err)
	}
	return nil
}

func (q *operationQueue) outbox(ctx context.Context, operationID string) ([]outboxEvent, error) {
	rows, err := q.db.QueryContext(ctx, `
SELECT ob.operation_id, ob.sequence, ob.event_type, ob.payload_json, ob.created_at_ns
FROM outbox ob
JOIN operations op ON op.id = ob.operation_id
WHERE ob.operation_id=? AND op.project_id=? ORDER BY ob.sequence`, operationID, q.binding.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("outbox query: %w", err)
	}
	defer rows.Close()
	var events []outboxEvent
	for rows.Next() {
		var event outboxEvent
		var payload []byte
		var createdNS int64
		if err := rows.Scan(&event.OperationID, &event.Sequence, &event.EventType, &payload, &createdNS); err != nil {
			return nil, fmt.Errorf("outbox scan: %w", err)
		}
		event.Payload = append(event.Payload[:0], payload...)
		event.CreatedAt = fromUnixNano(createdNS)
		events = append(events, event)
	}
	return events, rows.Err()
}

func truncate(s string, n int) string { //nolint:unparam // call sites share helper with explicit limit
	if len(s) <= n {
		return s
	}
	return s[:n]
}
