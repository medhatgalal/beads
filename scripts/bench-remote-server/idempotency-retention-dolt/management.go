package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func compactProducer(ctx context.Context, db *sql.DB, runID, producer string, repositoryEpoch, producerEpoch, through uint64, failpoint, readyFile string) error {
	return runManagement(ctx, db, func(conn *sql.Conn) error {
		return compactProducerAttempt(ctx, conn, runID, producer, repositoryEpoch, producerEpoch, through, failpoint, readyFile)
	})
}

func compactProducerAttempt(ctx context.Context, conn *sql.Conn, runID, producer string, repositoryEpoch, producerEpoch, through uint64, failpoint, readyFile string) error {
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return err
	}
	defer rollback(conn)
	ledger, err := readLedgerState(ctx, conn, runID)
	if err != nil {
		return err
	}
	if ledger.RepositoryEpoch != repositoryEpoch {
		return fmt.Errorf("repository epoch mismatch: expected=%d actual=%d", repositoryEpoch, ledger.RepositoryEpoch)
	}
	var currentProducerEpoch, next, floor uint64
	if err := conn.QueryRowContext(ctx, `
SELECT producer_epoch,next_sequence,compacted_through
FROM retention_producer_heads_v1
WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&currentProducerEpoch, &next, &floor); err != nil {
		return err
	}
	if currentProducerEpoch != producerEpoch || through <= floor || through >= next {
		return fmt.Errorf("invalid compaction prefix")
	}
	var deleteCount int64
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_receipts_v1
WHERE run_id=? AND producer_id=? AND producer_epoch=? AND sequence<=?`,
		runID, producer, producerEpoch, through).Scan(&deleteCount); err != nil {
		return err
	}
	if deleteCount <= 0 || deleteCount > ledger.ReceiptCount {
		return fmt.Errorf("compaction receipt count violates ledger")
	}
	updated, err := conn.ExecContext(ctx, `
UPDATE retention_producer_heads_v1
SET compacted_through=?,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND producer_id=? AND producer_epoch=? AND compacted_through=?`,
		through, runID, producer, producerEpoch, floor)
	if err != nil {
		return err
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("compaction head compare-and-swap lost: rows=%d err=%v", rows, err)
	}
	if failpoint == "after-watermark-before-delete" {
		waitForKill(readyFile)
	}
	deleted, err := conn.ExecContext(ctx, `
DELETE FROM retention_receipts_v1
WHERE run_id=? AND producer_id=? AND producer_epoch=? AND sequence<=?`,
		runID, producer, producerEpoch, through)
	if err != nil {
		return err
	}
	if rows, err := deleted.RowsAffected(); err != nil || rows != deleteCount {
		return fmt.Errorf("compaction deleted unexpected receipt count: rows=%d want=%d err=%v", rows, deleteCount, err)
	}
	if failpoint == "after-delete-before-commit" {
		waitForKill(readyFile)
	}
	if err := releaseReceipts(ctx, conn, runID, ledger, deleteCount); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", runCommit(runID, fmt.Sprintf("compact %s/%d through %d", producer, producerEpoch, through))); err != nil {
		return err
	}
	return nil
}

func advanceProducerEpoch(ctx context.Context, db *sql.DB, runID, producer string, repositoryEpoch, nextProducerEpoch uint64) error {
	return runManagement(ctx, db, func(conn *sql.Conn) error {
		return advanceProducerEpochAttempt(ctx, conn, runID, producer, repositoryEpoch, nextProducerEpoch)
	})
}

func advanceProducerEpochAttempt(ctx context.Context, conn *sql.Conn, runID, producer string, repositoryEpoch, nextProducerEpoch uint64) error {
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return err
	}
	defer rollback(conn)
	ledger, err := readLedgerState(ctx, conn, runID)
	if err != nil {
		return err
	}
	if ledger.RepositoryEpoch != repositoryEpoch {
		return fmt.Errorf("repository epoch mismatch: expected=%d actual=%d", repositoryEpoch, ledger.RepositoryEpoch)
	}
	var current uint64
	if err := conn.QueryRowContext(ctx, `
SELECT producer_epoch FROM retention_producer_heads_v1
WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&current); err != nil {
		return err
	}
	if nextProducerEpoch != current+1 {
		return fmt.Errorf("producer epoch must advance exactly once")
	}
	var deleteCount int64
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&deleteCount); err != nil {
		return err
	}
	deleted, err := conn.ExecContext(ctx, `
DELETE FROM retention_receipts_v1 WHERE run_id=? AND producer_id=?`, runID, producer)
	if err != nil {
		return err
	}
	if rows, err := deleted.RowsAffected(); err != nil || rows != deleteCount {
		return fmt.Errorf("producer epoch deleted unexpected receipt count: rows=%d want=%d err=%v", rows, deleteCount, err)
	}
	updated, err := conn.ExecContext(ctx, `
UPDATE retention_producer_heads_v1
SET producer_epoch=?,next_sequence=1,compacted_through=0,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND producer_id=? AND producer_epoch=?`, nextProducerEpoch, runID, producer, current)
	if err != nil {
		return err
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("producer epoch compare-and-swap lost: rows=%d err=%v", rows, err)
	}
	if err := releaseReceipts(ctx, conn, runID, ledger, deleteCount); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", runCommit(runID, fmt.Sprintf("producer-epoch %s %d", producer, nextProducerEpoch))); err != nil {
		return err
	}
	return nil
}

func rotateRepositoryEpoch(ctx context.Context, db *sql.DB, runID string, nextEpoch uint64) error {
	return runManagement(ctx, db, func(conn *sql.Conn) error {
		return rotateRepositoryEpochAttempt(ctx, conn, runID, nextEpoch)
	})
}

func rotateRepositoryEpochAttempt(ctx context.Context, conn *sql.Conn, runID string, nextEpoch uint64) error {
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return err
	}
	defer rollback(conn)
	ledger, err := readLedgerState(ctx, conn, runID)
	if err != nil {
		return err
	}
	if nextEpoch != ledger.RepositoryEpoch+1 {
		return fmt.Errorf("repository epoch must advance exactly once")
	}
	var physicalProducers, physicalReceipts int64
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_producer_heads_v1 WHERE run_id=?`, runID).Scan(&physicalProducers); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=?`, runID).Scan(&physicalReceipts); err != nil {
		return err
	}
	if physicalProducers != ledger.ProducerCount || physicalReceipts != ledger.ReceiptCount {
		return fmt.Errorf("repository epoch reset refused counter drift")
	}
	deletedReceipts, err := conn.ExecContext(ctx, `DELETE FROM retention_receipts_v1 WHERE run_id=?`, runID)
	if err != nil {
		return err
	}
	if rows, err := deletedReceipts.RowsAffected(); err != nil || rows != ledger.ReceiptCount {
		return fmt.Errorf("repository reset deleted unexpected receipts: rows=%d want=%d err=%v", rows, ledger.ReceiptCount, err)
	}
	deletedProducers, err := conn.ExecContext(ctx, `DELETE FROM retention_producer_heads_v1 WHERE run_id=?`, runID)
	if err != nil {
		return err
	}
	if rows, err := deletedProducers.RowsAffected(); err != nil || rows != ledger.ProducerCount {
		return fmt.Errorf("repository reset deleted unexpected producers: rows=%d want=%d err=%v", rows, ledger.ProducerCount, err)
	}
	if err := resetLedgerEpoch(ctx, conn, runID, ledger, nextEpoch); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", runCommit(runID, fmt.Sprintf("repository-epoch %d", nextEpoch))); err != nil {
		return err
	}
	return nil
}

func registerProducer(ctx context.Context, db *sql.DB, runID, producer string, repositoryEpoch, producerEpoch uint64, subject string) (registrationResult, error) {
	return registerProducerWithBarrier(ctx, db, runID, producer, repositoryEpoch, producerEpoch, subject, "")
}

func registerProducerWithBarrier(ctx context.Context, db *sql.DB, runID, producer string, repositoryEpoch, producerEpoch uint64, subject, readyFile string) (registrationResult, error) {
	started := time.Now()
	base := registrationResult{ProducerID: producer, RepositoryEpoch: repositoryEpoch, ProducerEpoch: producerEpoch}
	if !isProofProducer(producer) || subject != subjectHash(producer) || repositoryEpoch == 0 || producerEpoch == 0 {
		return base, fmt.Errorf("producer identity or epoch invalid")
	}
	var serializationRetries, duplicateRetries, casRetries int
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return base, err
		}
		attemptReadyFile := readyFile
		if attempt > 1 {
			attemptReadyFile = ""
		}
		result, err := registerProducerAttempt(ctx, conn, runID, producer, repositoryEpoch, producerEpoch, subject, attemptReadyFile)
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
		if err := retryDelay(ctx, attempt); err != nil {
			return base, err
		}
	}
	return base, fmt.Errorf("producer registration retry budget exhausted")
}

func registerProducerAttempt(ctx context.Context, conn *sql.Conn, runID, producer string, repositoryEpoch, producerEpoch uint64, subject, readyFile string) (registrationResult, error) {
	result := registrationResult{ProducerID: producer, RepositoryEpoch: repositoryEpoch, ProducerEpoch: producerEpoch}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return result, err
	}
	defer rollback(conn)
	ledger, err := readLedgerState(ctx, conn, runID)
	if err != nil {
		return result, err
	}
	result.RepositoryEpoch = ledger.RepositoryEpoch
	result.ProducerCount = ledger.ProducerCount
	if repositoryEpoch < ledger.RepositoryEpoch {
		result.Disposition = dispositionGone
		result.Reason = "repository_epoch_retired"
		return rollbackRegistrationDecision(ctx, conn, result)
	}
	if repositoryEpoch > ledger.RepositoryEpoch {
		result.Disposition = dispositionFenced
		result.Reason = "repository_epoch_not_activated"
		return rollbackRegistrationDecision(ctx, conn, result)
	}
	var storedSubject string
	var storedEpoch uint64
	err = conn.QueryRowContext(ctx, `
SELECT subject_hash,producer_epoch FROM retention_producer_heads_v1
WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&storedSubject, &storedEpoch)
	if err == nil {
		if storedSubject == subject && storedEpoch == producerEpoch {
			result.Disposition = dispositionReplay
			result.Reason = "producer_already_registered"
		} else {
			result.Disposition = dispositionConflict
			result.Reason = "producer_registration_conflict"
		}
		return rollbackRegistrationDecision(ctx, conn, result)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if ledger.ProducerCount >= ledger.MaxProducers {
		result.Disposition = dispositionCapacity
		result.Reason = "producer_capacity_exhausted"
		return rollbackRegistrationDecision(ctx, conn, result)
	}
	if readyFile != "" {
		if err := waitForLedgerBarrier(ctx, readyFile); err != nil {
			return result, err
		}
	}
	if err := reserveProducer(ctx, conn, runID, ledger); err != nil {
		return result, err
	}
	if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_producer_heads_v1(
  run_id,producer_id,subject_hash,producer_epoch,next_sequence,compacted_through,updated_at
) VALUES(?,?,?,?,1,0,UTC_TIMESTAMP(6))`, runID, producer, subject, producerEpoch); err != nil {
		return result, err
	}
	message := runCommit(runID, fmt.Sprintf("register %s/%d", producer, producerEpoch))
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", message); err != nil {
		return result, err
	}
	result.Disposition = dispositionExecuted
	result.Reason = "producer_and_counter_committed"
	result.ProducerCount = ledger.ProducerCount + 1
	result.CommitMessage = message
	return result, nil
}

func rollbackRegistrationDecision(ctx context.Context, conn *sql.Conn, result registrationResult) (registrationResult, error) {
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return registrationResult{}, fmt.Errorf("rollback registration decision: %w", err)
	}
	return result, nil
}

func runManagement(ctx context.Context, db *sql.DB, attemptFn func(*sql.Conn) error) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		err = attemptFn(conn)
		_ = conn.Close()
		if err == nil {
			return nil
		}
		serialization, duplicate := isRetryable(err)
		if !errors.Is(err, errLedgerCAS) && !serialization && !duplicate {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("management retry budget exhausted")
}

func retryDelay(ctx context.Context, attempt int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(attempt*10) * time.Millisecond):
		return nil
	}
}

func createStaleBranch(ctx context.Context, db *sql.DB, runID, epochOneHead string) (string, error) {
	compact := strings.ReplaceAll(runID[:8], "-", "")
	branch := staleBranchPrefix + compact
	if len(epochOneHead) != 32 {
		return "", fmt.Errorf("unexpected Dolt head shape")
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_BRANCH(?, ?)", branch, epochOneHead); err != nil {
		return "", fmt.Errorf("create stale restore branch: %w", err)
	}
	return branch, nil
}

func staleRestoreDecision(ctx context.Context, db *sql.DB, ring *verificationRing, branch string, in request) (operationResult, int64, int64, error) {
	if err := ring.verify(in); err != nil {
		return operationResult{}, 0, 0, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return operationResult{}, 0, 0, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "CALL DOLT_CHECKOUT(?)", branch); err != nil {
		return operationResult{}, 0, 0, fmt.Errorf("checkout stale restore branch: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "CALL DOLT_CHECKOUT('main')")
	}()
	var before int64
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_business_v1 WHERE run_id=? AND operation_id=?`, in.RunID, in.OperationID).Scan(&before); err != nil {
		return operationResult{}, 0, 0, err
	}
	result, err := executeAttempt(ctx, conn, in, "", "")
	if err != nil {
		return operationResult{}, 0, 0, err
	}
	var after int64
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_business_v1 WHERE run_id=? AND operation_id=?`, in.RunID, in.OperationID).Scan(&after); err != nil {
		return operationResult{}, 0, 0, err
	}
	return result, before, after, nil
}
