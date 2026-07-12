package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

func inspectRun(ctx context.Context, db *sql.DB, runID string, expectedEpoch uint64) (inspection, error) {
	result := inspection{
		InspectedAt: time.Now().UTC(), Database: labDatabase, LabID: labID, SchemaVersion: schemaVersion,
		RunID: runID, Producers: make(map[string]producerInspection),
		OperationBusinessRows: make(map[string]int64), OperationCommitRows: make(map[string]int64),
	}
	if err := verifyLabIdentity(ctx, db); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, "SELECT VERSION(),DOLT_VERSION(),DOLT_HASHOF('HEAD')").Scan(&result.MySQLCompatVersion, &result.DoltVersion, &result.HeadHash); err != nil {
		return result, err
	}
	ledger, err := readLedgerState(ctx, db, runID)
	if err != nil {
		return result, err
	}
	result.RepositoryEpoch = ledger.RepositoryEpoch
	result.MaxProducers = ledger.MaxProducers
	result.MaxReceipts = ledger.MaxReceipts
	result.MaxOutcomeBytes = ledger.MaxOutcomeBytes
	result.StoredProducerCount = ledger.ProducerCount
	result.StoredReceiptCount = ledger.ReceiptCount
	result.LedgerVersion = ledger.Version
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_producer_heads_v1 WHERE run_id=?`, runID).Scan(&result.ProducerRows); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=?`, runID).Scan(&result.ReceiptRows); err != nil {
		return result, err
	}
	result.CounterInvariant = result.StoredProducerCount == result.ProducerRows && result.StoredReceiptCount == result.ReceiptRows
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_business_v1 WHERE run_id=?`, runID).Scan(&result.BusinessRows); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&result.DirtyTableRows); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dolt_log WHERE message LIKE ?`, runCommit(runID, "%")).Scan(&result.RunCommitRows); err != nil {
		return result, err
	}
	rows, err := db.QueryContext(ctx, `SELECT name FROM dolt_branches WHERE name LIKE 'retention_stale_%' ORDER BY name`)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var branch string
		if err := rows.Scan(&branch); err != nil {
			_ = rows.Close()
			return result, err
		}
		result.Branches = append(result.Branches, branch)
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	for _, producer := range proofProducers {
		state, err := inspectProducer(ctx, db, runID, producer)
		if err != nil {
			return result, err
		}
		result.Producers[producer] = state
	}
	rows, err = db.QueryContext(ctx, `
SELECT operation_id,COUNT(*) FROM retention_business_v1
WHERE run_id=? GROUP BY operation_id ORDER BY operation_id`, runID)
	if err != nil {
		return result, err
	}
	var operations []string
	for rows.Next() {
		var operationID string
		var count int64
		if err := rows.Scan(&operationID, &count); err != nil {
			_ = rows.Close()
			return result, err
		}
		operations = append(operations, operationID)
		result.OperationBusinessRows[operationID] = count
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	sort.Strings(operations)
	for _, operationID := range operations {
		var count int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dolt_log WHERE message=?`, runCommit(runID, "operation "+operationID)).Scan(&count); err != nil {
			return result, err
		}
		result.OperationCommitRows[operationID] = count
	}
	result.Passed = result.Database == labDatabase && result.LabID == labID && result.SchemaVersion == schemaVersion &&
		result.RepositoryEpoch == expectedEpoch && result.DirtyTableRows == 0 && result.CounterInvariant &&
		result.MaxProducers <= maxProducerRows && result.MaxReceipts <= maxReceiptRows && result.MaxOutcomeBytes <= maxOutcomeBytes
	for _, count := range result.OperationBusinessRows {
		result.Passed = result.Passed && count == 1
	}
	for _, count := range result.OperationCommitRows {
		result.Passed = result.Passed && count == 1
	}
	return result, nil
}

func inspectCounters(ctx context.Context, db *sql.DB, runID string) (counterSnapshot, error) {
	var result counterSnapshot
	ledger, err := readLedgerState(ctx, db, runID)
	if err != nil {
		return result, err
	}
	result.StoredProducers = ledger.ProducerCount
	result.StoredReceipts = ledger.ReceiptCount
	result.Version = ledger.Version
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_producer_heads_v1 WHERE run_id=?`, runID).Scan(&result.PhysicalProducers); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=?`, runID).Scan(&result.PhysicalReceipts); err != nil {
		return result, err
	}
	return result, nil
}

func inspectProducer(ctx context.Context, db *sql.DB, runID, producer string) (producerInspection, error) {
	result := producerInspection{ProducerID: producer}
	err := db.QueryRowContext(ctx, `
SELECT producer_epoch,next_sequence,compacted_through
FROM retention_producer_heads_v1 WHERE run_id=? AND producer_id=?`, runID, producer).Scan(
		&result.ProducerEpoch, &result.NextSequence, &result.CompactedThrough)
	if err == nil {
		result.Exists = true
	} else if err != sql.ErrNoRows {
		return result, err
	}
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&result.ReceiptRows); err != nil {
		return result, err
	}
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_business_v1 WHERE run_id=? AND producer_id=?`, runID, producer).Scan(&result.BusinessRows); err != nil {
		return result, err
	}
	return result, nil
}

func operationCounts(ctx context.Context, db *sql.DB, runID, operationID string) (business, receipts, commits int64, err error) {
	if err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_business_v1 WHERE run_id=? AND operation_id=?`, runID, operationID).Scan(&business); err != nil {
		return
	}
	if err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=? AND operation_id=?`, runID, operationID).Scan(&receipts); err != nil {
		return
	}
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dolt_log WHERE message=?`, runCommit(runID, "operation "+operationID)).Scan(&commits)
	return
}

func currentHead(ctx context.Context, db *sql.DB) (string, error) {
	var head string
	err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&head)
	if err != nil {
		return "", fmt.Errorf("read Dolt head: %w", err)
	}
	return head, nil
}
