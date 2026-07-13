package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

var errInjectedRollback = errors.New("injected rollback before Dolt commit")

func subjectHash(producer string) string {
	sum := sha256.Sum256([]byte("beads-perf-lab-retention-subject:" + producer))
	return hex.EncodeToString(sum[:])
}

func newRequest(runID, producer string, repositoryEpoch, producerEpoch, sequence uint64, payload string, ring *verificationRing) (request, error) {
	requestHash := canonicalRequestHash(payload)
	in := request{
		RunID: runID, ProducerID: producer, SubjectHash: subjectHash(producer),
		ExpectedRepositoryEpoch: repositoryEpoch, ProducerEpoch: producerEpoch, Sequence: sequence,
		RequestHash: requestHash,
		Payload:     payload,
	}
	in.OperationID = canonicalOperationID(in, requestHash)
	return ring.sign(in)
}

func canonicalRequestHash(payload string) string {
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func canonicalOperationID(in request, requestHash string) string {
	identity := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", in.RunID, in.ProducerID, in.ProducerEpoch, in.Sequence, requestHash)
	digest := sha256.Sum256([]byte(identity))
	return "ret-" + hex.EncodeToString(digest[:16])
}

func validateRequest(in request) error {
	if _, err := uuid.Parse(in.RunID); err != nil {
		return fmt.Errorf("invalid run ID")
	}
	if !isProofProducer(in.ProducerID) || in.SubjectHash != subjectHash(in.ProducerID) {
		return fmt.Errorf("invalid producer identity")
	}
	if in.ExpectedRepositoryEpoch == 0 || in.ProducerEpoch == 0 || in.Sequence == 0 {
		return fmt.Errorf("epochs and sequence must be positive")
	}
	if len(in.RequestHash) != 64 || len(in.OperationID) != 36 || !strings.HasPrefix(in.OperationID, "ret-") {
		return fmt.Errorf("invalid operation identity")
	}
	if !strings.HasPrefix(in.Payload, operationPrefix) || len(in.Payload) > maxOutcomeBytes {
		return fmt.Errorf("payload is not bounded synthetic data")
	}
	wantHash := canonicalRequestHash(in.Payload)
	if in.RequestHash != wantHash {
		return fmt.Errorf("request hash does not bind canonical payload")
	}
	if in.OperationID != canonicalOperationID(in, wantHash) {
		return fmt.Errorf("operation ID does not bind canonical request identity")
	}
	return nil
}

func isProofProducer(candidate string) bool {
	for _, producer := range proofProducers {
		if candidate == producer {
			return true
		}
	}
	return false
}

func executeOperation(ctx context.Context, db *sql.DB, ring *verificationRing, in request, failpoint, readyFile string) (operationResult, error) {
	started := time.Now()
	base := operationResult{OperationID: in.OperationID, ProducerID: in.ProducerID, ProducerEpoch: in.ProducerEpoch, Sequence: in.Sequence}
	if err := validateRequest(in); err != nil {
		return base, err
	}
	if err := ring.verify(in); err != nil {
		base.Disposition = dispositionUnauthorized
		base.Reason = "key_epoch_or_signature_rejected"
		base.WallMS = elapsedMS(started)
		return base, nil
	}
	var serializationRetries, duplicateRetries, casRetries int
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return base, err
		}
		attemptFailpoint := failpoint
		if attempt > 1 && failpoint == "before-ledger-cas" {
			attemptFailpoint = ""
		}
		result, err := executeAttempt(ctx, conn, in, attemptFailpoint, readyFile)
		_ = conn.Close()
		if err == nil {
			result.Attempts = attempt
			result.SerializationRetries = serializationRetries
			result.DuplicateRetries = duplicateRetries
			result.CASRetries = casRetries
			result.WallMS = elapsedMS(started)
			return result, nil
		}
		serialization, duplicate := isRetryable(err)
		if errors.Is(err, errLedgerCAS) {
			casRetries++
		} else if serialization {
			serializationRetries++
		} else if duplicate {
			duplicateRetries++
		} else {
			return base, err
		}
		select {
		case <-ctx.Done():
			return base, ctx.Err()
		case <-time.After(time.Duration(attempt*10) * time.Millisecond):
		}
	}
	return base, fmt.Errorf("operation retry budget exhausted")
}

func executeAttempt(ctx context.Context, conn *sql.Conn, in request, failpoint, readyFile string) (operationResult, error) {
	result := operationResult{OperationID: in.OperationID, ProducerID: in.ProducerID, ProducerEpoch: in.ProducerEpoch, Sequence: in.Sequence}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return result, fmt.Errorf("begin canonical transaction: %w", err)
	}
	defer rollback(conn)
	if failpoint == "after-start" {
		waitForKill(readyFile)
	}
	ledger, err := readLedgerState(ctx, conn, in.RunID)
	if err != nil {
		return result, err
	}
	result.RepositoryEpoch = ledger.RepositoryEpoch
	switch {
	case in.ExpectedRepositoryEpoch < ledger.RepositoryEpoch:
		result.Disposition = dispositionGone
		result.Reason = "repository_epoch_retired"
		return rollbackDecision(ctx, conn, result)
	case in.ExpectedRepositoryEpoch > ledger.RepositoryEpoch:
		result.Disposition = dispositionFenced
		result.Reason = "repository_epoch_not_activated"
		return rollbackDecision(ctx, conn, result)
	}
	var storedSubject string
	var producerEpoch, nextSequence, compactedThrough uint64
	err = conn.QueryRowContext(ctx, `
SELECT subject_hash,producer_epoch,next_sequence,compacted_through
	FROM retention_producer_heads_v1
	WHERE run_id=? AND producer_id=?`, in.RunID, in.ProducerID).Scan(
		&storedSubject, &producerEpoch, &nextSequence, &compactedThrough)
	if errors.Is(err, sql.ErrNoRows) {
		result.Disposition = dispositionUnknown
		result.Reason = "producer_not_registered"
		return rollbackDecision(ctx, conn, result)
	}
	if err != nil {
		return result, fmt.Errorf("read producer head: %w", err)
	}
	result.NextSequence = nextSequence
	result.CompactedThrough = compactedThrough
	if storedSubject != in.SubjectHash {
		result.Disposition = dispositionUnauthorized
		result.Reason = "producer_subject_mismatch"
		return rollbackDecision(ctx, conn, result)
	}
	switch {
	case in.ProducerEpoch < producerEpoch:
		result.Disposition = dispositionGone
		result.Reason = "producer_epoch_retired"
		return rollbackDecision(ctx, conn, result)
	case in.ProducerEpoch > producerEpoch:
		result.Disposition = dispositionFenced
		result.Reason = "producer_epoch_not_activated"
		return rollbackDecision(ctx, conn, result)
	case in.Sequence <= compactedThrough:
		result.Disposition = dispositionGone
		result.Reason = "sequence_compacted"
		return rollbackDecision(ctx, conn, result)
	case in.Sequence > nextSequence:
		result.Disposition = dispositionGap
		result.Reason = "predecessor_not_terminal"
		return rollbackDecision(ctx, conn, result)
	case in.Sequence < nextSequence:
		var requestHash, operationID, outcomeCode, outcomePayload, subject string
		err := conn.QueryRowContext(ctx, `
SELECT request_hash,operation_id,outcome_code,outcome_payload,subject_hash
FROM retention_receipts_v1
WHERE run_id=? AND producer_id=? AND producer_epoch=? AND sequence=?`,
			in.RunID, in.ProducerID, in.ProducerEpoch, in.Sequence).Scan(
			&requestHash, &operationID, &outcomeCode, &outcomePayload, &subject)
		if err != nil {
			return result, fmt.Errorf("read retained receipt: %w", err)
		}
		if requestHash != in.RequestHash || operationID != in.OperationID || subject != in.SubjectHash {
			result.Disposition = dispositionConflict
			result.Reason = "sequence_bound_to_another_request"
			return rollbackDecision(ctx, conn, result)
		}
		if outcomeCode != "ok" || outcomePayload != in.Payload {
			return result, fmt.Errorf("retained receipt outcome mismatch")
		}
		result.Disposition = dispositionReplay
		result.Reason = "canonical_receipt_replay"
		return rollbackDecision(ctx, conn, result)
	}
	if int64(len(in.Payload)) > ledger.MaxOutcomeBytes {
		result.Disposition = dispositionCapacity
		result.Reason = "outcome_capacity_exhausted"
		return rollbackDecision(ctx, conn, result)
	}
	if ledger.ReceiptCount >= ledger.MaxReceipts {
		result.Disposition = dispositionCapacity
		result.Reason = "receipt_capacity_exhausted"
		return rollbackDecision(ctx, conn, result)
	}
	if failpoint == "before-ledger-cas" {
		if err := waitForLedgerBarrier(ctx, readyFile); err != nil {
			return result, err
		}
	}
	if err := reserveReceipt(ctx, conn, in.RunID, ledger); err != nil {
		return result, err
	}
	if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_business_v1(
  run_id,operation_id,producer_id,producer_epoch,sequence,request_hash,payload,created_at
) VALUES(?,?,?,?,?,?,?,UTC_TIMESTAMP(6))`,
		in.RunID, in.OperationID, in.ProducerID, in.ProducerEpoch, in.Sequence, in.RequestHash, in.Payload); err != nil {
		return result, err
	}
	if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_receipts_v1(
  run_id,producer_id,producer_epoch,sequence,subject_hash,request_hash,
  operation_id,outcome_code,outcome_payload,committed_at
) VALUES(?,?,?,?,?,?,?,'ok',?,UTC_TIMESTAMP(6))`,
		in.RunID, in.ProducerID, in.ProducerEpoch, in.Sequence, in.SubjectHash,
		in.RequestHash, in.OperationID, in.Payload); err != nil {
		return result, err
	}
	update, err := conn.ExecContext(ctx, `
UPDATE retention_producer_heads_v1
SET next_sequence=?,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND producer_id=? AND producer_epoch=? AND next_sequence=?`,
		in.Sequence+1, in.RunID, in.ProducerID, in.ProducerEpoch, in.Sequence)
	if err != nil {
		return result, err
	}
	if rows, err := update.RowsAffected(); err != nil || rows != 1 {
		return result, fmt.Errorf("producer head compare-and-swap lost: rows=%d err=%v", rows, err)
	}
	if failpoint == "after-mutations-before-commit" {
		waitForKill(readyFile)
	}
	if failpoint == "rollback-before-commit" {
		return result, errInjectedRollback
	}
	message := runCommit(in.RunID, "operation "+in.OperationID)
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", message); err != nil {
		return result, err
	}
	if failpoint == "after-dolt-commit-before-response" {
		waitForKill(readyFile)
	}
	result.Disposition = dispositionExecuted
	result.Reason = "business_receipt_and_head_committed"
	result.NextSequence = in.Sequence + 1
	result.CommitMessage = message
	return result, nil
}

func rollbackDecision(ctx context.Context, conn *sql.Conn, result operationResult) (operationResult, error) {
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return operationResult{}, fmt.Errorf("rollback read-only decision: %w", err)
	}
	return result, nil
}

func waitForKill(readyFile string) {
	if !strings.HasPrefix(readyFile, "/private/tmp/beads-fleet-scale-20260712/") {
		os.Exit(78)
	}
	file, err := os.OpenFile(readyFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(79)
	}
	_, _ = file.WriteString("ready\n")
	_ = file.Sync()
	_ = file.Close()
	for {
		time.Sleep(time.Hour)
	}
}

func waitForLedgerBarrier(ctx context.Context, readyFile string) error {
	if !strings.HasPrefix(readyFile, evidenceRoot+"/") {
		return fmt.Errorf("ledger barrier path escaped evidence root")
	}
	releaseFile := readyFile + ".release"
	file, err := os.OpenFile(readyFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create ledger barrier readiness file: %w", err)
	}
	if _, err := file.WriteString("ready\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	for {
		if _, err := os.Stat(releaseFile); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func elapsedMS(started time.Time) float64 {
	return float64(time.Since(started).Microseconds()) / 1000
}
