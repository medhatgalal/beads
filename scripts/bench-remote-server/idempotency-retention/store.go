package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type canonicalStore struct {
	db      *sql.DB
	path    string
	binding Binding
	policy  Policy
}

func openCanonicalStore(ctx context.Context, path string, binding Binding, policy Policy) (*canonicalStore, error) {
	if binding.CellID == "" || binding.TeamID == "" || binding.ProjectID == "" || binding.LedgerEpoch == 0 || binding.LedgerEpoch > math.MaxInt64 {
		return nil, fmt.Errorf("cell, team, project, and expected ledger-epoch bindings are required")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("canonical store path: %w", err)
	}
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("canonical store path must not be a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect canonical store: %w", err)
	}
	file, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create canonical store: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure canonical store: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close canonical store bootstrap: %w", err)
	}

	u := &url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "wal_autocheckpoint(1000)")
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open canonical store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &canonicalStore{db: db, path: abs, binding: binding, policy: policy}
	if err := store.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *canonicalStore) init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS ledger_binding (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  schema_version INTEGER NOT NULL CHECK(schema_version = 1),
  cell_id TEXT NOT NULL,
  team_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  ledger_epoch INTEGER NOT NULL CHECK(ledger_epoch > 0),
  max_producers INTEGER NOT NULL,
  max_receipts INTEGER NOT NULL,
  max_receipts_per_producer INTEGER NOT NULL,
  keep_recent_per_producer INTEGER NOT NULL,
  max_outcome_bytes INTEGER NOT NULL,
  minimum_retry_horizon_ns INTEGER NOT NULL,
  created_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS producer_heads (
  project_id TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  subject_hash BLOB NOT NULL CHECK(length(subject_hash) = 32),
  producer_epoch INTEGER NOT NULL CHECK(producer_epoch > 0),
  next_sequence INTEGER NOT NULL CHECK(next_sequence > 0),
  compacted_through INTEGER NOT NULL CHECK(compacted_through >= 0),
  created_at_ns INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  PRIMARY KEY(project_id, producer_id)
);
CREATE TABLE IF NOT EXISTS receipts (
  project_id TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  producer_epoch INTEGER NOT NULL CHECK(producer_epoch > 0),
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  request_hash BLOB NOT NULL CHECK(length(request_hash) = 32),
  outcome_code TEXT NOT NULL,
  outcome_payload BLOB NOT NULL CHECK(length(outcome_payload) <= 65536),
  committed_at_ns INTEGER NOT NULL,
  PRIMARY KEY(project_id, producer_id, producer_epoch, sequence),
  FOREIGN KEY(project_id, producer_id)
    REFERENCES producer_heads(project_id, producer_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS receipts_compaction_idx
  ON receipts(committed_at_ns, project_id, producer_id, producer_epoch, sequence);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize canonical store: %w", err)
	}
	return s.verifyBinding(ctx)
}

func (s *canonicalStore) verifyBinding(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("binding begin: %w", err)
	}
	defer tx.Rollback()
	var (
		schemaVersion, maxOutcome                                            int
		cellID, teamID, projectID                                            string
		ledgerEpoch                                                          uint64
		maxProducers, maxReceipts, maxPer, keepRecent, minimumRetryHorizonNS int64
	)
	err = tx.QueryRowContext(ctx, `
SELECT schema_version, cell_id, team_id, project_id, ledger_epoch, max_producers, max_receipts,
       max_receipts_per_producer, keep_recent_per_producer, max_outcome_bytes,
       minimum_retry_horizon_ns
FROM ledger_binding WHERE singleton = 1`).Scan(
		&schemaVersion, &cellID, &teamID, &projectID, &ledgerEpoch, &maxProducers, &maxReceipts,
		&maxPer, &keepRecent, &maxOutcome, &minimumRetryHorizonNS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		var rows int64
		if err := tx.QueryRowContext(ctx, `
SELECT (SELECT COUNT(*) FROM producer_heads) + (SELECT COUNT(*) FROM receipts)`).Scan(&rows); err != nil {
			return fmt.Errorf("binding legacy state check: %w", err)
		}
		if rows != 0 {
			return fmt.Errorf("unbound non-empty canonical store must not be adopted")
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO ledger_binding(
  singleton, schema_version, cell_id, team_id, project_id, ledger_epoch, max_producers, max_receipts,
  max_receipts_per_producer, keep_recent_per_producer, max_outcome_bytes,
  minimum_retry_horizon_ns, created_at_ns
) VALUES(1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			s.binding.CellID, s.binding.TeamID, s.binding.ProjectID, s.binding.LedgerEpoch, s.policy.MaxProducers, s.policy.MaxReceipts,
			s.policy.MaxReceiptsPerProducer, s.policy.KeepRecentPerProducer,
			s.policy.MaxOutcomeBytes, s.policy.MinimumRetryHorizon.Nanoseconds(),
			time.Now().UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("insert canonical binding: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("read canonical binding: %w", err)
	} else if schemaVersion != 1 || cellID != s.binding.CellID || teamID != s.binding.TeamID || projectID != s.binding.ProjectID || ledgerEpoch != s.binding.LedgerEpoch ||
		maxProducers != s.policy.MaxProducers || maxReceipts != s.policy.MaxReceipts ||
		maxPer != s.policy.MaxReceiptsPerProducer || keepRecent != s.policy.KeepRecentPerProducer ||
		maxOutcome != s.policy.MaxOutcomeBytes || minimumRetryHorizonNS != s.policy.MinimumRetryHorizon.Nanoseconds() {
		return fmt.Errorf("canonical store binding or persisted policy mismatch")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("binding commit: %w", err)
	}
	return nil
}

func (s *canonicalStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *canonicalStore) RegisterProducer(ctx context.Context, projectID, producerID string, subjectHash [32]byte, ledgerEpoch, producerEpoch uint64, now time.Time) error {
	if projectID != s.binding.ProjectID {
		return fmt.Errorf("producer project does not match canonical store binding")
	}
	probe := Request{ProjectID: projectID, ProducerID: producerID, SubjectHash: subjectHash, LedgerEpoch: ledgerEpoch, ProducerEpoch: producerEpoch, Sequence: 1, RequestHash: [32]byte{1}, KeyEpoch: 1}
	if err := probe.validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("register producer begin: %w", err)
	}
	defer tx.Rollback()
	var currentLedgerEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT ledger_epoch FROM ledger_binding WHERE singleton=1`).Scan(&currentLedgerEpoch); err != nil {
		return fmt.Errorf("register producer ledger epoch: %w", err)
	}
	if currentLedgerEpoch != ledgerEpoch {
		return fmt.Errorf("register producer fenced by ledger epoch: current=%d requested=%d", currentLedgerEpoch, ledgerEpoch)
	}
	var existing uint64
	var existingSubject []byte
	err = tx.QueryRowContext(ctx, `
SELECT producer_epoch, subject_hash FROM producer_heads WHERE project_id=? AND producer_id=?`, projectID, producerID).Scan(&existing, &existingSubject)
	if err == nil {
		if existing != producerEpoch || !bytes.Equal(existingSubject, subjectHash[:]) {
			return fmt.Errorf("producer already registered under another epoch or subject")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("register producer lookup: %w", err)
	}
	var count int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM producer_heads").Scan(&count); err != nil {
		return fmt.Errorf("register producer capacity: %w", err)
	}
	if count >= s.policy.MaxProducers {
		return ErrProducerLimit
	}
	stamp := now.UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO producer_heads(
  project_id, producer_id, subject_hash, producer_epoch, next_sequence, compacted_through,
  created_at_ns, updated_at_ns
) VALUES(?, ?, ?, ?, 1, 0, ?, ?)`, projectID, producerID, subjectHash[:], producerEpoch, stamp, stamp); err != nil {
		return fmt.Errorf("register producer insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("register producer commit: %w", err)
	}
	return nil
}

// AdvanceProducerEpoch is an explicit fence. It atomically retires all prior
// outcomes for this producer, deletes their details, and resets sequencing.
// A request from the retired epoch is thereafter Gone, never executable.
func (s *canonicalStore) AdvanceProducerEpoch(ctx context.Context, projectID, producerID string, ledgerEpoch, nextEpoch uint64, now time.Time) error {
	if projectID != s.binding.ProjectID {
		return fmt.Errorf("producer project does not match canonical store binding")
	}
	if projectID == "" || producerID == "" || ledgerEpoch == 0 || ledgerEpoch > math.MaxInt64 || nextEpoch == 0 || nextEpoch > math.MaxInt64 {
		return fmt.Errorf("invalid producer epoch advance")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("advance producer epoch begin: %w", err)
	}
	defer tx.Rollback()
	var currentLedgerEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT ledger_epoch FROM ledger_binding WHERE singleton=1`).Scan(&currentLedgerEpoch); err != nil {
		return fmt.Errorf("advance producer ledger epoch lookup: %w", err)
	}
	if currentLedgerEpoch != ledgerEpoch {
		return fmt.Errorf("advance producer fenced by ledger epoch: current=%d requested=%d", currentLedgerEpoch, ledgerEpoch)
	}
	var current uint64
	if err := tx.QueryRowContext(ctx, `
SELECT producer_epoch FROM producer_heads WHERE project_id=? AND producer_id=?`, projectID, producerID).Scan(&current); err != nil {
		return fmt.Errorf("advance producer epoch lookup: %w", err)
	}
	if current >= math.MaxInt64 || nextEpoch != current+1 {
		return fmt.Errorf("producer epoch must advance exactly once: current=%d requested=%d", current, nextEpoch)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM receipts WHERE project_id=? AND producer_id=?`, projectID, producerID); err != nil {
		return fmt.Errorf("advance producer epoch delete receipts: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE producer_heads SET producer_epoch=?, next_sequence=1, compacted_through=0, updated_at_ns=?
WHERE project_id=? AND producer_id=? AND producer_epoch=?`,
		nextEpoch, now.UTC().UnixNano(), projectID, producerID, current)
	if err != nil {
		return fmt.Errorf("advance producer epoch update: %w", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("advance producer epoch fence lost: rows=%d err=%v", rows, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("advance producer epoch commit: %w", err)
	}
	return nil
}

// RotateLedgerEpoch is the bounded tombstone-reclamation escape hatch. It is a
// repository-wide fence: every request carrying an older ledger epoch becomes
// permanently Gone, so all per-producer heads and receipts from the old
// namespace may be deleted atomically. Use it only as an explicit control-plane
// migration because it ends the retry horizon for every older request.
func (s *canonicalStore) RotateLedgerEpoch(ctx context.Context, nextEpoch uint64, now time.Time) error {
	if nextEpoch == 0 || nextEpoch > math.MaxInt64 {
		return fmt.Errorf("invalid ledger epoch rotation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("rotate ledger epoch begin: %w", err)
	}
	defer tx.Rollback()
	var current uint64
	if err := tx.QueryRowContext(ctx, `
SELECT ledger_epoch FROM ledger_binding WHERE singleton=1`).Scan(&current); err != nil {
		return fmt.Errorf("rotate ledger epoch lookup: %w", err)
	}
	if current >= math.MaxInt64 || nextEpoch != current+1 {
		return fmt.Errorf("ledger epoch must advance exactly once: current=%d requested=%d", current, nextEpoch)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM receipts"); err != nil {
		return fmt.Errorf("rotate ledger epoch delete receipts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM producer_heads"); err != nil {
		return fmt.Errorf("rotate ledger epoch delete producer heads: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE ledger_binding SET ledger_epoch=? WHERE singleton=1 AND ledger_epoch=?`, nextEpoch, current)
	if err != nil {
		return fmt.Errorf("rotate ledger epoch update: %w", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("rotate ledger epoch fence lost: rows=%d err=%v", rows, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("rotate ledger epoch commit: %w", err)
	}
	return nil
}

func (s *canonicalStore) LedgerEpoch(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := s.db.QueryRowContext(ctx, `
SELECT ledger_epoch FROM ledger_binding WHERE singleton=1`).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("read canonical ledger epoch: %w", err)
	}
	return epoch, nil
}

type ledger struct {
	store *canonicalStore
	keys  *VerificationRing
}

func newLedger(store *canonicalStore, keys *VerificationRing) (*ledger, error) {
	if store == nil || keys == nil {
		return nil, fmt.Errorf("canonical store and verification ring are required")
	}
	return &ledger{store: store, keys: keys}, nil
}

func (l *ledger) Execute(ctx context.Context, in Request, proposed Outcome, now time.Time, mutate Mutation) (Decision, error) {
	if err := in.validate(); err != nil {
		return Decision{}, err
	}
	if err := l.keys.Verify(in); err != nil {
		return Decision{Disposition: DispositionUnauthorized, Reason: "signature_not_in_verification_ring"}, nil
	}
	if in.ProjectID != l.store.binding.ProjectID {
		return Decision{Disposition: DispositionUnauthorized, Reason: "project_binding_mismatch"}, nil
	}
	if proposed.Code == "" || len(proposed.Code) > 64 {
		return Decision{}, fmt.Errorf("outcome code must contain 1..64 bytes")
	}
	if len(proposed.Payload) > l.store.policy.MaxOutcomeBytes {
		return Decision{}, fmt.Errorf("outcome payload exceeds %d bytes", l.store.policy.MaxOutcomeBytes)
	}
	tx, err := l.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Decision{}, fmt.Errorf("execute begin: %w", err)
	}
	defer tx.Rollback()
	decision, err := l.store.decideTx(ctx, tx, in)
	if err != nil {
		return Decision{}, err
	}
	if decision.Disposition != DispositionExecuted {
		if err := tx.Commit(); err != nil {
			return Decision{}, fmt.Errorf("decision commit: %w", err)
		}
		return decision, nil
	}
	if err := l.store.ensureReceiptCapacityTx(ctx, tx, in, now); err != nil {
		return Decision{}, err
	}
	if mutate != nil {
		if err := mutate(ctx, tx); err != nil {
			return Decision{}, fmt.Errorf("business mutation: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO receipts(
  project_id, producer_id, producer_epoch, sequence, request_hash,
  outcome_code, outcome_payload, committed_at_ns
) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		in.ProjectID, in.ProducerID, in.ProducerEpoch, in.Sequence, in.RequestHash[:],
		proposed.Code, normalizedPayload(proposed.Payload), now.UTC().UnixNano()); err != nil {
		return Decision{}, fmt.Errorf("insert canonical receipt: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE producer_heads SET next_sequence=?, updated_at_ns=?
WHERE project_id=? AND producer_id=? AND producer_epoch=? AND next_sequence=?`,
		in.Sequence+1, now.UTC().UnixNano(), in.ProjectID, in.ProducerID, in.ProducerEpoch, in.Sequence)
	if err != nil {
		return Decision{}, fmt.Errorf("advance producer sequence: %w", err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		return Decision{}, fmt.Errorf("%w: producer sequence compare-and-swap lost: rows=%d err=%v", ErrCorruptState, rows, err)
	}
	if err := tx.Commit(); err != nil {
		return Decision{}, fmt.Errorf("execute commit: %w", err)
	}
	decision.Outcome = cloneOutcome(proposed)
	decision.NextSequence = in.Sequence + 1
	return decision, nil
}

func (s *canonicalStore) decideTx(ctx context.Context, tx *sql.Tx, in Request) (Decision, error) {
	var ledgerEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT ledger_epoch FROM ledger_binding WHERE singleton=1`).Scan(&ledgerEpoch); err != nil {
		return Decision{}, fmt.Errorf("read ledger epoch: %w", err)
	}
	base := Decision{CurrentLedgerEpoch: ledgerEpoch}
	switch {
	case in.LedgerEpoch < ledgerEpoch:
		base.Disposition = DispositionGone
		base.Reason = "ledger_epoch_retired"
		return base, nil
	case in.LedgerEpoch > ledgerEpoch:
		base.Disposition = DispositionFenced
		base.Reason = "ledger_epoch_not_activated"
		return base, nil
	}
	var epoch, next, floor uint64
	var subjectHash []byte
	err := tx.QueryRowContext(ctx, `
SELECT subject_hash, producer_epoch, next_sequence, compacted_through
FROM producer_heads WHERE project_id=? AND producer_id=?`, in.ProjectID, in.ProducerID).Scan(&subjectHash, &epoch, &next, &floor)
	if errors.Is(err, sql.ErrNoRows) {
		base.Disposition = DispositionUnknown
		base.Reason = "producer_not_registered"
		return base, nil
	}
	if err != nil {
		return Decision{}, fmt.Errorf("read producer head: %w", err)
	}
	base.CurrentEpoch = epoch
	base.NextSequence = next
	base.CompactedThrough = floor
	switch {
	case !bytes.Equal(subjectHash, in.SubjectHash[:]):
		base.Disposition = DispositionUnauthorized
		base.Reason = "producer_subject_mismatch"
		return base, nil
	case in.ProducerEpoch < epoch:
		base.Disposition = DispositionGone
		base.Reason = "producer_epoch_retired"
		return base, nil
	case in.ProducerEpoch > epoch:
		base.Disposition = DispositionFenced
		base.Reason = "producer_epoch_not_activated"
		return base, nil
	case in.Sequence <= floor:
		base.Disposition = DispositionGone
		base.Reason = "sequence_compacted"
		return base, nil
	case in.Sequence > next:
		base.Disposition = DispositionGap
		base.Reason = "predecessor_not_terminal"
		return base, nil
	case in.Sequence == next:
		base.Disposition = DispositionExecuted
		base.Reason = "new_contiguous_sequence"
		return base, nil
	}
	var requestHash, payload []byte
	var code string
	err = tx.QueryRowContext(ctx, `
SELECT request_hash, outcome_code, outcome_payload
FROM receipts
WHERE project_id=? AND producer_id=? AND producer_epoch=? AND sequence=?`,
		in.ProjectID, in.ProducerID, in.ProducerEpoch, in.Sequence).Scan(&requestHash, &code, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return Decision{}, fmt.Errorf("%w: non-compacted prior sequence has no receipt", ErrCorruptState)
	}
	if err != nil {
		return Decision{}, fmt.Errorf("read canonical receipt: %w", err)
	}
	if !bytes.Equal(requestHash, in.RequestHash[:]) {
		base.Disposition = DispositionConflict
		base.Reason = "sequence_bound_to_different_request"
		return base, nil
	}
	base.Disposition = DispositionReplay
	base.Reason = "canonical_receipt_replay"
	base.Outcome = Outcome{Code: code, Payload: append([]byte(nil), payload...)}
	return base, nil
}

func (s *canonicalStore) ensureReceiptCapacityTx(ctx context.Context, tx *sql.Tx, in Request, now time.Time) error {
	global, producer, err := receiptCountsTx(ctx, tx, in.ProjectID, in.ProducerID, in.ProducerEpoch)
	if err != nil {
		return err
	}
	if global+1 <= s.policy.MaxReceipts && producer+1 <= s.policy.MaxReceiptsPerProducer {
		return nil
	}
	if _, err := s.compactTx(ctx, tx, now, nil); err != nil {
		return err
	}
	global, producer, err = receiptCountsTx(ctx, tx, in.ProjectID, in.ProducerID, in.ProducerEpoch)
	if err != nil {
		return err
	}
	if global+1 > s.policy.MaxReceipts || producer+1 > s.policy.MaxReceiptsPerProducer {
		return ErrCapacity
	}
	return nil
}

func receiptCountsTx(ctx context.Context, tx *sql.Tx, projectID, producerID string, epoch uint64) (int64, int64, error) {
	var global, producer int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM receipts").Scan(&global); err != nil {
		return 0, 0, fmt.Errorf("count global receipts: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM receipts
WHERE project_id=? AND producer_id=? AND producer_epoch=?`, projectID, producerID, epoch).Scan(&producer); err != nil {
		return 0, 0, fmt.Errorf("count producer receipts: %w", err)
	}
	return global, producer, nil
}

func cloneOutcome(in Outcome) Outcome {
	return Outcome{Code: in.Code, Payload: normalizedPayload(in.Payload)}
}

func normalizedPayload(in []byte) []byte {
	if len(in) == 0 {
		return []byte{}
	}
	return append([]byte(nil), in...)
}

type compactCandidate struct {
	projectID  string
	producerID string
	epoch      uint64
	through    uint64
}

func (s *canonicalStore) Compact(ctx context.Context, now time.Time, hook CompactHook) (CompactResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CompactResult{}, fmt.Errorf("compact begin: %w", err)
	}
	defer tx.Rollback()
	result, err := s.compactTx(ctx, tx, now, hook)
	if err != nil {
		return CompactResult{}, err
	}
	if hook != nil {
		if err := hook(CompactBeforeCommit); err != nil {
			return CompactResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return CompactResult{}, fmt.Errorf("compact commit: %w", err)
	}
	if hook != nil {
		if err := hook(CompactAfterCommit); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (s *canonicalStore) compactTx(ctx context.Context, tx *sql.Tx, now time.Time, hook CompactHook) (CompactResult, error) {
	cutoff := now.UTC().Add(-s.policy.MinimumRetryHorizon).UnixNano()
	rows, err := tx.QueryContext(ctx, `
WITH bounds AS (
  SELECT project_id, producer_id, producer_epoch, MAX(sequence) AS max_sequence,
         MIN(CASE WHEN committed_at_ns > ? THEN sequence END) AS first_too_new
  FROM receipts
  GROUP BY project_id, producer_id, producer_epoch
), candidates AS (
  SELECT project_id, producer_id, producer_epoch,
         CASE
           WHEN first_too_new IS NULL THEN max_sequence - ?
           WHEN first_too_new - 1 < max_sequence - ? THEN first_too_new - 1
           ELSE max_sequence - ?
         END AS through_sequence
  FROM bounds
)
SELECT c.project_id, c.producer_id, c.producer_epoch, c.through_sequence
FROM candidates c
JOIN producer_heads h
  ON h.project_id=c.project_id AND h.producer_id=c.producer_id
 AND h.producer_epoch=c.producer_epoch
WHERE c.through_sequence > h.compacted_through`,
		cutoff, s.policy.KeepRecentPerProducer, s.policy.KeepRecentPerProducer,
		s.policy.KeepRecentPerProducer)
	if err != nil {
		return CompactResult{}, fmt.Errorf("select compaction prefixes: %w", err)
	}
	var candidates []compactCandidate
	for rows.Next() {
		var candidate compactCandidate
		if err := rows.Scan(&candidate.projectID, &candidate.producerID, &candidate.epoch, &candidate.through); err != nil {
			_ = rows.Close()
			return CompactResult{}, fmt.Errorf("scan compaction prefix: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return CompactResult{}, fmt.Errorf("iterate compaction prefixes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return CompactResult{}, fmt.Errorf("close compaction prefixes: %w", err)
	}
	var result CompactResult
	for _, candidate := range candidates {
		res, err := tx.ExecContext(ctx, `
UPDATE producer_heads SET compacted_through=?, updated_at_ns=?
WHERE project_id=? AND producer_id=? AND producer_epoch=? AND compacted_through < ?`,
			candidate.through, now.UTC().UnixNano(), candidate.projectID, candidate.producerID,
			candidate.epoch, candidate.through)
		if err != nil {
			return CompactResult{}, fmt.Errorf("advance compacted-through watermark: %w", err)
		}
		advanced, err := res.RowsAffected()
		if err != nil {
			return CompactResult{}, fmt.Errorf("count advanced watermarks: %w", err)
		}
		result.ProducersAdvanced += advanced
	}
	if hook != nil {
		if err := hook(CompactAfterWatermarks); err != nil {
			return CompactResult{}, err
		}
	}
	for _, candidate := range candidates {
		res, err := tx.ExecContext(ctx, `
DELETE FROM receipts
WHERE project_id=? AND producer_id=? AND producer_epoch=? AND sequence <= ?`,
			candidate.projectID, candidate.producerID, candidate.epoch, candidate.through)
		if err != nil {
			return CompactResult{}, fmt.Errorf("delete compacted receipt prefix: %w", err)
		}
		deleted, err := res.RowsAffected()
		if err != nil {
			return CompactResult{}, fmt.Errorf("count compacted receipts: %w", err)
		}
		result.ReceiptsDeleted += deleted
	}
	if hook != nil {
		if err := hook(CompactAfterDeletes); err != nil {
			return CompactResult{}, err
		}
	}
	return result, nil
}

func (s *canonicalStore) Stats(ctx context.Context) (StoreStats, error) {
	var stats StoreStats
	err := s.db.QueryRowContext(ctx, `
SELECT
  (SELECT COUNT(*) FROM producer_heads),
  (SELECT COUNT(*) FROM receipts),
  (SELECT COALESCE(SUM(length(outcome_payload)), 0) FROM receipts)`).Scan(
		&stats.Producers, &stats.Receipts, &stats.OutcomePayloadBytes)
	if err != nil {
		return StoreStats{}, fmt.Errorf("read canonical store stats: %w", err)
	}
	return stats, nil
}

func (s *canonicalStore) head(ctx context.Context, projectID, producerID string) (epoch, next, floor uint64, err error) {
	err = s.db.QueryRowContext(ctx, `
SELECT producer_epoch, next_sequence, compacted_through
FROM producer_heads WHERE project_id=? AND producer_id=?`, projectID, producerID).Scan(&epoch, &next, &floor)
	return
}

func (s *canonicalStore) QuickCheck(ctx context.Context) error {
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("canonical quick check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("canonical quick check returned %q", result)
	}
	stats, err := s.Stats(ctx)
	if err != nil {
		return err
	}
	if stats.Producers > s.policy.MaxProducers || stats.Receipts > s.policy.MaxReceipts ||
		stats.OutcomePayloadBytes > stats.Receipts*int64(s.policy.MaxOutcomeBytes) {
		return fmt.Errorf("%w: global storage policy exceeded", ErrCorruptState)
	}
	var staleEpochReceipts int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM receipts r
JOIN producer_heads h
  ON h.project_id=r.project_id AND h.producer_id=r.producer_id
WHERE r.producer_epoch <> h.producer_epoch`).Scan(&staleEpochReceipts); err != nil {
		return fmt.Errorf("canonical stale-epoch invariant: %w", err)
	}
	if staleEpochReceipts != 0 {
		return fmt.Errorf("%w: receipts remain for a retired producer epoch", ErrCorruptState)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT h.project_id, h.producer_id, h.producer_epoch, h.next_sequence,
       h.compacted_through, COUNT(r.sequence), COALESCE(MIN(r.sequence), 0),
       COALESCE(MAX(r.sequence), 0)
FROM producer_heads h
LEFT JOIN receipts r
  ON r.project_id=h.project_id AND r.producer_id=h.producer_id
 AND r.producer_epoch=h.producer_epoch
GROUP BY h.project_id, h.producer_id, h.producer_epoch, h.next_sequence, h.compacted_through`)
	if err != nil {
		return fmt.Errorf("canonical invariant query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var projectID, producerID string
		var epoch, next, floor, count, minimum, maximum uint64
		if err := rows.Scan(&projectID, &producerID, &epoch, &next, &floor, &count, &minimum, &maximum); err != nil {
			return fmt.Errorf("canonical invariant scan: %w", err)
		}
		if count > uint64(s.policy.MaxReceiptsPerProducer) {
			return fmt.Errorf("%w: producer receipt cap exceeded for %s/%s", ErrCorruptState, projectID, producerID)
		}
		if floor >= next {
			return fmt.Errorf("%w: watermark is not below next sequence for %s/%s", ErrCorruptState, projectID, producerID)
		}
		expected := next - 1 - floor
		if count != expected || (count > 0 && (minimum != floor+1 || maximum != next-1)) {
			return fmt.Errorf("%w: receipt prefix has a hole for %s/%s epoch %d", ErrCorruptState, projectID, producerID, epoch)
		}
	}
	return rows.Err()
}

func (s *canonicalStore) Checkpoint(ctx context.Context) error {
	var busy, logFrames, checkpointed int
	if err := s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return fmt.Errorf("checkpoint canonical store: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("checkpoint canonical store remained busy: log=%d checkpointed=%d", logFrames, checkpointed)
	}
	return nil
}
