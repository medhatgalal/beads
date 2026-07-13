package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const retentionLabControlIdentityQuery = `
SELECT COUNT(*), COALESCE(MAX(environment), ''), COALESCE(MAX(lab_id), '')
FROM beads_perf_lab_control.lab_identity`

func openDB(database string, maxOpen int) (*sql.DB, error) {
	if database != "" && database != labDatabase {
		return nil, fmt.Errorf("isolation guard rejected database")
	}
	if net.ParseIP(labHost) == nil || labPort != 13360 {
		return nil, fmt.Errorf("isolation guard rejected endpoint")
	}
	connector, err := mysql.NewConnector(&mysql.Config{
		User: "root", Net: "tcp", Addr: net.JoinHostPort(labHost, fmt.Sprintf("%d", labPort)),
		DBName: database, ParseTime: true, AllowNativePasswords: true, TLSConfig: "false",
		Timeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(2 * time.Minute)
	return db, nil
}

func setupLab(ctx context.Context) (mysqlVersion, doltVersion, head string, err error) {
	admin, err := openDB("", 1)
	if err != nil {
		return "", "", "", err
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		return "", "", "", fmt.Errorf("connect loopback Dolt: %w", err)
	}
	var exists int
	if err := admin.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=?`, labDatabase).Scan(&exists); err != nil {
		return "", "", "", fmt.Errorf("inspect lab database existence: %w", err)
	}
	created := exists == 0
	legacySchema := false
	if created {
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE `beads_perf_lab_retention_ae`"); err != nil {
			return "", "", "", fmt.Errorf("create isolated lab database: %w", err)
		}
	}
	db, err := openDB(labDatabase, 1)
	if err != nil {
		return "", "", "", err
	}
	defer db.Close()
	if !created {
		var identityTable int
		if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema=? AND table_name='retention_lab_identity_v1'`, labDatabase).Scan(&identityTable); err != nil {
			return "", "", "", fmt.Errorf("inspect existing lab identity: %w", err)
		}
		if identityTable != 1 {
			return "", "", "", fmt.Errorf("refusing to adopt existing database without retention lab identity")
		}
		if err := verifyCompatibleLabIdentity(ctx, db); err != nil {
			return "", "", "", err
		}
		var storedSchema int
		if err := db.QueryRowContext(ctx, `SELECT schema_version FROM retention_lab_identity_v1 WHERE singleton=1`).Scan(&storedSchema); err != nil {
			return "", "", "", fmt.Errorf("read compatible lab schema version: %w", err)
		}
		legacySchema = storedSchema < schemaVersion
	}
	_, err = ensureSchema(ctx, db, legacySchema)
	if err != nil {
		return "", "", "", err
	}
	if created {
		if _, err := db.ExecContext(ctx, `
INSERT INTO retention_lab_identity_v1(
  singleton, schema_version, environment, database_name, lab_id, created_at
) VALUES(1, ?, 'synthetic', ?, ?, UTC_TIMESTAMP(6))`, schemaVersion, labDatabase, labID); err != nil {
			return "", "", "", fmt.Errorf("insert lab identity: %w", err)
		}
	} else {
		if _, err := db.ExecContext(ctx, `
UPDATE retention_lab_identity_v1 SET schema_version=? WHERE singleton=1`, schemaVersion); err != nil {
			return "", "", "", fmt.Errorf("advance lab schema identity: %w", err)
		}
	}
	if err := verifyLabIdentity(ctx, db); err != nil {
		return "", "", "", err
	}
	var dirty int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&dirty); err != nil {
		return "", "", "", fmt.Errorf("inspect setup working set: %w", err)
	}
	if dirty > 0 {
		if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", commitPrefix+"schema v2"); err != nil {
			return "", "", "", fmt.Errorf("commit lab schema: %w", err)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT VERSION(), DOLT_VERSION(), DOLT_HASHOF('HEAD')").Scan(&mysqlVersion, &doltVersion, &head); err != nil {
		return "", "", "", fmt.Errorf("read lab version/head: %w", err)
	}
	return mysqlVersion, doltVersion, head, nil
}

func verifyServerControlIdentity(ctx context.Context) error {
	admin, err := openDB("", 1)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("connect synthetic control plane: %w", err)
	}
	return verifyServerControlIdentityOnDB(ctx, admin)
}

func verifyServerControlIdentityOnDB(ctx context.Context, db *sql.DB) error {
	var count int
	var environment, actualLabID string
	if err := db.QueryRowContext(ctx, retentionLabControlIdentityQuery).Scan(&count, &environment, &actualLabID); err != nil {
		return fmt.Errorf("required beads_perf_lab_control identity marker is unavailable")
	}
	if count != 1 || environment != "synthetic" || actualLabID != labID {
		return fmt.Errorf("beads_perf_lab_control identity marker mismatch")
	}
	return nil
}

func ensureSchema(ctx context.Context, db *sql.DB, forceBackfill bool) (bool, error) {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS retention_lab_identity_v1 (
  singleton TINYINT PRIMARY KEY,
  schema_version INT NOT NULL,
  environment VARCHAR(32) NOT NULL,
  database_name VARCHAR(128) NOT NULL,
  lab_id VARCHAR(36) NOT NULL,
  created_at DATETIME(6) NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS retention_runs_v1 (
  run_id VARCHAR(36) PRIMARY KEY,
  lab_id VARCHAR(36) NOT NULL,
  created_at DATETIME(6) NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS retention_ledgers_v1 (
  run_id VARCHAR(36) PRIMARY KEY,
  repository_epoch BIGINT UNSIGNED NOT NULL,
  max_producers BIGINT NOT NULL,
  max_receipts BIGINT NOT NULL,
  max_outcome_bytes BIGINT NOT NULL,
	producer_count BIGINT NOT NULL,
	receipt_count BIGINT NOT NULL,
	ledger_version BIGINT UNSIGNED NOT NULL,
  updated_at DATETIME(6) NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS retention_producer_heads_v1 (
  run_id VARCHAR(36) NOT NULL,
  producer_id VARCHAR(64) NOT NULL,
  subject_hash CHAR(64) NOT NULL,
  producer_epoch BIGINT UNSIGNED NOT NULL,
  next_sequence BIGINT UNSIGNED NOT NULL,
  compacted_through BIGINT UNSIGNED NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY(run_id, producer_id)
)`,
		`CREATE TABLE IF NOT EXISTS retention_receipts_v1 (
  run_id VARCHAR(36) NOT NULL,
  producer_id VARCHAR(64) NOT NULL,
  producer_epoch BIGINT UNSIGNED NOT NULL,
  sequence BIGINT UNSIGNED NOT NULL,
  subject_hash CHAR(64) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  operation_id VARCHAR(64) NOT NULL,
  outcome_code VARCHAR(64) NOT NULL,
  outcome_payload VARCHAR(4096) NOT NULL,
  committed_at DATETIME(6) NOT NULL,
  PRIMARY KEY(run_id, producer_id, producer_epoch, sequence),
  UNIQUE KEY retention_receipt_operation_uq(run_id, operation_id)
)`,
		`CREATE TABLE IF NOT EXISTS retention_business_v1 (
  run_id VARCHAR(36) NOT NULL,
  operation_id VARCHAR(64) NOT NULL,
  producer_id VARCHAR(64) NOT NULL,
  producer_epoch BIGINT UNSIGNED NOT NULL,
  sequence BIGINT UNSIGNED NOT NULL,
  request_hash CHAR(64) NOT NULL,
  payload VARCHAR(4096) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY(run_id, operation_id),
  UNIQUE KEY retention_business_sequence_uq(run_id, producer_id, producer_epoch, sequence)
)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return false, fmt.Errorf("ensure isolated retention schema: %w", err)
		}
	}
	migrated := false
	columns := []struct {
		name, ddl string
	}{
		{"producer_count", "ALTER TABLE retention_ledgers_v1 ADD COLUMN producer_count BIGINT NOT NULL DEFAULT 0 AFTER max_outcome_bytes"},
		{"receipt_count", "ALTER TABLE retention_ledgers_v1 ADD COLUMN receipt_count BIGINT NOT NULL DEFAULT 0 AFTER producer_count"},
		{"ledger_version", "ALTER TABLE retention_ledgers_v1 ADD COLUMN ledger_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER receipt_count"},
	}
	for _, column := range columns {
		added, err := ensureLedgerColumn(ctx, db, column.name, column.ddl)
		if err != nil {
			return false, err
		}
		migrated = migrated || added
	}
	if migrated || forceBackfill {
		if err := backfillLedgerCounters(ctx, db); err != nil {
			return false, err
		}
	}
	return migrated, nil
}

func ensureLedgerColumn(ctx context.Context, db *sql.DB, column, ddl string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema=? AND table_name='retention_ledgers_v1' AND column_name=?`, labDatabase, column).Scan(&count); err != nil {
		return false, fmt.Errorf("inspect ledger column %s: %w", column, err)
	}
	if count == 1 {
		return false, nil
	}
	if count != 0 {
		return false, fmt.Errorf("unexpected ledger column multiplicity for %s", column)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return false, fmt.Errorf("add ledger column %s: %w", column, err)
	}
	return true, nil
}

func backfillLedgerCounters(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT run_id FROM retention_ledgers_v1 ORDER BY run_id`)
	if err != nil {
		return fmt.Errorf("list ledgers for counter migration: %w", err)
	}
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			_ = rows.Close()
			return err
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, runID := range runIDs {
		var producers, receipts int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_producer_heads_v1 WHERE run_id=?`, runID).Scan(&producers); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM retention_receipts_v1 WHERE run_id=?`, runID).Scan(&receipts); err != nil {
			return err
		}
		if producers > maxProducerRows || receipts > maxReceiptRows {
			return fmt.Errorf("legacy run %s exceeds binary retention ceiling", runID)
		}
		if _, err := db.ExecContext(ctx, `
UPDATE retention_ledgers_v1
SET producer_count=?,receipt_count=?,ledger_version=1,updated_at=UTC_TIMESTAMP(6)
WHERE run_id=?`, producers, receipts, runID); err != nil {
			return fmt.Errorf("backfill ledger counters for %s: %w", runID, err)
		}
	}
	return nil
}

func verifyLabIdentity(ctx context.Context, db *sql.DB) error {
	return verifyLabIdentityVersion(ctx, db, false)
}

func verifyCompatibleLabIdentity(ctx context.Context, db *sql.DB) error {
	return verifyLabIdentityVersion(ctx, db, true)
}

func verifyLabIdentityVersion(ctx context.Context, db *sql.DB, compatible bool) error {
	var database, environment, storedLabID string
	var storedSchema int
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		return fmt.Errorf("read database identity: %w", err)
	}
	if database != labDatabase {
		return fmt.Errorf("database identity mismatch")
	}
	if err := db.QueryRowContext(ctx, `
SELECT schema_version, environment, database_name, lab_id
FROM retention_lab_identity_v1 WHERE singleton=1`).Scan(&storedSchema, &environment, &database, &storedLabID); err != nil {
		return fmt.Errorf("read lab identity: %w", err)
	}
	versionOK := storedSchema == schemaVersion
	if compatible {
		versionOK = storedSchema == 1 || storedSchema == schemaVersion
	}
	if !versionOK || environment != "synthetic" || database != labDatabase || storedLabID != labID {
		return fmt.Errorf("lab identity mismatch")
	}
	return nil
}

var proofProducers = []string{
	"p-same", "p-order", "p-rollback", "p-compact", "p-kill-start",
	"p-kill-before", "p-kill-after", "p-restore", "p-main2",
	"p-cap-a", "p-cap-b", "p-cap-r1", "p-cap-r2",
}

var initialProofProducers = []string{
	"p-same", "p-order", "p-rollback", "p-compact", "p-kill-start",
	"p-kill-before", "p-kill-after", "p-restore", "p-cap-r1", "p-cap-r2",
}

func initializeRun(ctx context.Context, db *sql.DB, runID string) error {
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run ID must be a UUID")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return fmt.Errorf("initialize run begin: %w", err)
	}
	defer rollback(conn)
	if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_runs_v1(run_id,lab_id,created_at) VALUES(?,?,UTC_TIMESTAMP(6))`, runID, labID); err != nil {
		return fmt.Errorf("initialize run identity: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_ledgers_v1(
  run_id,repository_epoch,max_producers,max_receipts,max_outcome_bytes,
  producer_count,receipt_count,ledger_version,updated_at
) VALUES(?,1,?,?,?,?,0,1,UTC_TIMESTAMP(6))`, runID, proofProducerRows, proofReceiptRows,
		proofOutcomeBytes, len(initialProofProducers)); err != nil {
		return fmt.Errorf("initialize run ledger: %w", err)
	}
	for _, producer := range initialProofProducers {
		if _, err := conn.ExecContext(ctx, `
INSERT INTO retention_producer_heads_v1(
  run_id,producer_id,subject_hash,producer_epoch,next_sequence,compacted_through,updated_at
) VALUES(?,?,?,1,1,0,UTC_TIMESTAMP(6))`, runID, producer, subjectHash(producer)); err != nil {
			return fmt.Errorf("initialize producer %s: %w", producer, err)
		}
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", runCommit(runID, "initialize")); err != nil {
		return fmt.Errorf("commit run initialization: %w", err)
	}
	return nil
}

func runCommit(runID, suffix string) string {
	return commitPrefix + runID + " " + suffix
}

func rollback(conn *sql.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = conn.ExecContext(ctx, "ROLLBACK")
}

func isRetryable(err error) (serialization, duplicate bool) {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false, false
	}
	return mysqlErr.Number == 1213 || mysqlErr.Number == 1205, mysqlErr.Number == 1062
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	detail := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(detail) > 300 {
		return detail[:300]
	}
	return detail
}
