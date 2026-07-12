package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// appendBatch is a storage-soak accelerator. It verifies every envelope and
// commits the batch atomically, but it deliberately has no business-mutation
// callback. Production command execution must use Execute so the business
// mutation and receipt share one canonical transaction.
func (l *ledger) appendBatch(ctx context.Context, records []batchRecord, compactAt time.Time) error {
	if len(records) == 0 {
		return nil
	}
	for _, record := range records {
		if err := record.Request.validate(); err != nil {
			return err
		}
		if err := l.keys.Verify(record.Request); err != nil {
			return err
		}
		if record.Request.ProjectID != l.store.binding.ProjectID {
			return fmt.Errorf("batch project does not match canonical store binding")
		}
		if record.Outcome.Code == "" || len(record.Outcome.Code) > 64 {
			return fmt.Errorf("outcome code must contain 1..64 bytes")
		}
		if len(record.Outcome.Payload) > l.store.policy.MaxOutcomeBytes {
			return fmt.Errorf("outcome payload exceeds %d bytes", l.store.policy.MaxOutcomeBytes)
		}
	}
	tx, err := l.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("append batch begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := l.store.compactTx(ctx, tx, compactAt, nil); err != nil {
		return err
	}

	type producerState struct {
		projectID    string
		producerID   string
		epoch        uint64
		subjectHash  [32]byte
		originalNext uint64
		next         uint64
		incoming     int64
	}
	states := make(map[string]*producerState)
	stateKey := func(projectID, producerID string) string { return projectID + "\x00" + producerID }
	for _, record := range records {
		request := record.Request
		key := stateKey(request.ProjectID, request.ProducerID)
		state := states[key]
		if state == nil {
			state = &producerState{projectID: request.ProjectID, producerID: request.ProducerID}
			var storedSubject []byte
			if err := tx.QueryRowContext(ctx, `
SELECT subject_hash, producer_epoch, next_sequence
FROM producer_heads WHERE project_id=? AND producer_id=?`,
				request.ProjectID, request.ProducerID).Scan(&storedSubject, &state.epoch, &state.originalNext); err != nil {
				return fmt.Errorf("append batch producer head: %w", err)
			}
			copy(state.subjectHash[:], storedSubject)
			state.next = state.originalNext
			states[key] = state
		}
		if request.ProducerEpoch != state.epoch {
			return fmt.Errorf("append batch producer epoch mismatch: current=%d requested=%d", state.epoch, request.ProducerEpoch)
		}
		if request.SubjectHash != state.subjectHash {
			return fmt.Errorf("append batch producer subject mismatch")
		}
		if request.Sequence != state.next {
			return fmt.Errorf("append batch requires contiguous sequences: expected=%d requested=%d", state.next, request.Sequence)
		}
		state.next++
		state.incoming++
	}

	var global int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM receipts").Scan(&global); err != nil {
		return fmt.Errorf("append batch global capacity: %w", err)
	}
	if global+int64(len(records)) > l.store.policy.MaxReceipts {
		return ErrCapacity
	}
	for _, state := range states {
		var retained int64
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM receipts
WHERE project_id=? AND producer_id=? AND producer_epoch=?`,
			state.projectID, state.producerID, state.epoch).Scan(&retained); err != nil {
			return fmt.Errorf("append batch producer capacity: %w", err)
		}
		if retained+state.incoming > l.store.policy.MaxReceiptsPerProducer {
			return ErrCapacity
		}
	}

	insert, err := tx.PrepareContext(ctx, `
INSERT INTO receipts(
  project_id, producer_id, producer_epoch, sequence, request_hash,
  outcome_code, outcome_payload, committed_at_ns
) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare batch receipt insert: %w", err)
	}
	defer insert.Close()
	for _, record := range records {
		committedAt := record.CommittedAt
		if committedAt.IsZero() {
			committedAt = compactAt
		}
		if _, err := insert.ExecContext(ctx,
			record.Request.ProjectID, record.Request.ProducerID, record.Request.ProducerEpoch,
			record.Request.Sequence, record.Request.RequestHash[:], record.Outcome.Code,
			normalizedPayload(record.Outcome.Payload), committedAt.UTC().UnixNano()); err != nil {
			return fmt.Errorf("append batch receipt insert: %w", err)
		}
	}
	if err := insert.Close(); err != nil {
		return fmt.Errorf("close batch receipt insert: %w", err)
	}
	for _, state := range states {
		res, err := tx.ExecContext(ctx, `
UPDATE producer_heads SET next_sequence=?, updated_at_ns=?
WHERE project_id=? AND producer_id=? AND producer_epoch=? AND next_sequence=?`,
			state.next, compactAt.UTC().UnixNano(), state.projectID, state.producerID,
			state.epoch, state.originalNext)
		if err != nil {
			return fmt.Errorf("append batch producer advance: %w", err)
		}
		if rows, err := res.RowsAffected(); err != nil || rows != 1 {
			return fmt.Errorf("%w: append batch sequence compare-and-swap lost: rows=%d err=%v", ErrCorruptState, rows, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("append batch commit: %w", err)
	}
	return nil
}

func createBusinessProbeTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS business_probe (
  operation_key TEXT PRIMARY KEY,
  mutation_count INTEGER NOT NULL
)`)
	return err
}
