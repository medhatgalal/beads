package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var errLedgerCAS = errors.New("retention ledger compare-and-swap lost")

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readLedgerState(ctx context.Context, queryer queryRower, runID string) (ledgerState, error) {
	var state ledgerState
	err := queryer.QueryRowContext(ctx, `
SELECT repository_epoch,max_producers,max_receipts,max_outcome_bytes,
       producer_count,receipt_count,ledger_version
FROM retention_ledgers_v1 WHERE run_id=?`, runID).Scan(
		&state.RepositoryEpoch, &state.MaxProducers, &state.MaxReceipts, &state.MaxOutcomeBytes,
		&state.ProducerCount, &state.ReceiptCount, &state.Version)
	if err != nil {
		return state, fmt.Errorf("read retention ledger: %w", err)
	}
	if err := validateLedgerState(state); err != nil {
		return state, err
	}
	return state, nil
}

func validateLedgerState(state ledgerState) error {
	switch {
	case state.RepositoryEpoch == 0:
		return fmt.Errorf("invalid zero repository epoch")
	case state.Version == 0:
		return fmt.Errorf("invalid zero ledger version")
	case state.MaxProducers <= 0 || state.MaxProducers > maxProducerRows:
		return fmt.Errorf("stored producer limit exceeds binary ceiling")
	case state.MaxReceipts <= 0 || state.MaxReceipts > maxReceiptRows:
		return fmt.Errorf("stored receipt limit exceeds binary ceiling")
	case state.MaxOutcomeBytes <= 0 || state.MaxOutcomeBytes > maxOutcomeBytes:
		return fmt.Errorf("stored outcome limit exceeds binary ceiling")
	case state.ProducerCount < 0 || state.ProducerCount > state.MaxProducers:
		return fmt.Errorf("stored producer counter violates limit")
	case state.ReceiptCount < 0 || state.ReceiptCount > state.MaxReceipts:
		return fmt.Errorf("stored receipt counter violates limit")
	default:
		return nil
	}
}

func reserveProducer(ctx context.Context, conn *sql.Conn, runID string, state ledgerState) error {
	result, err := conn.ExecContext(ctx, `
UPDATE retention_ledgers_v1
SET producer_count=producer_count+1,ledger_version=ledger_version+1,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND repository_epoch=? AND ledger_version=? AND producer_count=?
  AND producer_count<max_producers
  AND max_producers BETWEEN 1 AND ?
  AND max_receipts BETWEEN 1 AND ?
  AND max_outcome_bytes BETWEEN 1 AND ?`,
		runID, state.RepositoryEpoch, state.Version, state.ProducerCount,
		maxProducerRows, maxReceiptRows, maxOutcomeBytes)
	return requireOneCAS(result, err)
}

func reserveReceipt(ctx context.Context, conn *sql.Conn, runID string, state ledgerState) error {
	result, err := conn.ExecContext(ctx, `
UPDATE retention_ledgers_v1
SET receipt_count=receipt_count+1,ledger_version=ledger_version+1,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND repository_epoch=? AND ledger_version=? AND receipt_count=?
  AND receipt_count<max_receipts
  AND max_producers BETWEEN 1 AND ?
  AND max_receipts BETWEEN 1 AND ?
  AND max_outcome_bytes BETWEEN 1 AND ?`,
		runID, state.RepositoryEpoch, state.Version, state.ReceiptCount,
		maxProducerRows, maxReceiptRows, maxOutcomeBytes)
	return requireOneCAS(result, err)
}

func releaseReceipts(ctx context.Context, conn *sql.Conn, runID string, state ledgerState, count int64) error {
	if count < 0 || count > state.ReceiptCount {
		return fmt.Errorf("invalid receipt counter release")
	}
	result, err := conn.ExecContext(ctx, `
UPDATE retention_ledgers_v1
SET receipt_count=receipt_count-?,ledger_version=ledger_version+1,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND repository_epoch=? AND ledger_version=? AND receipt_count=?
  AND receipt_count>=?
  AND max_producers BETWEEN 1 AND ?
  AND max_receipts BETWEEN 1 AND ?
  AND max_outcome_bytes BETWEEN 1 AND ?`,
		count, runID, state.RepositoryEpoch, state.Version, state.ReceiptCount, count,
		maxProducerRows, maxReceiptRows, maxOutcomeBytes)
	return requireOneCAS(result, err)
}

func resetLedgerEpoch(ctx context.Context, conn *sql.Conn, runID string, state ledgerState, nextEpoch uint64) error {
	result, err := conn.ExecContext(ctx, `
UPDATE retention_ledgers_v1
SET repository_epoch=?,producer_count=0,receipt_count=0,
    ledger_version=ledger_version+1,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=? AND repository_epoch=? AND ledger_version=?
  AND producer_count=? AND receipt_count=?
  AND max_producers BETWEEN 1 AND ?
  AND max_receipts BETWEEN 1 AND ?
  AND max_outcome_bytes BETWEEN 1 AND ?`,
		nextEpoch, runID, state.RepositoryEpoch, state.Version,
		state.ProducerCount, state.ReceiptCount, maxProducerRows, maxReceiptRows, maxOutcomeBytes)
	return requireOneCAS(result, err)
}

func requireOneCAS(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errLedgerCAS
	}
	return nil
}
