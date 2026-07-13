package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	storagepkg "github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
)

const (
	receiptPrefix             = "_beads_perf_gateway_receipt_v1/"
	outboxPrefix              = "_beads_perf_gateway_outbox_v1/"
	terminalOutcomePrefix     = "_beads_perf_gateway_terminal_outcome_v2/"
	terminalOutboxPrefix      = "_beads_perf_gateway_terminal_outbox_v2/"
	maxMetadataValueBytes     = 60 * 1024
	maxOutboxMarkerValueBytes = 16 * 1024
)

var errReceiptIdentityMismatch = errors.New("receipt identity mismatch")

type operationBackend interface {
	Attestation(context.Context) (labAttestation, error)
	Ping(context.Context) (any, error)
	Show(context.Context, string) (any, error)
	List(context.Context, int) (any, error)
	Ready(context.Context, int) (any, error)
	Execute(context.Context, operation) (*doltReceipt, error)
	LookupReceipt(context.Context, operation) (*doltReceipt, error)
	LegacyOutcomeExists(context.Context, string, string) (bool, error)
	PersistPermanentFailure(context.Context, operation, string, string) (*doltReceipt, error)
	VerifyTerminalDurability(context.Context, operation, *doltReceipt) error
	PoolStats() any
	Close(context.Context) error
}

type uowBackend struct {
	provider               *uow.DirectDoltServerProvider
	projectID              string
	databaseName           string
	labID                  string
	clusterGuard           clusterAckGuardConfig
	clusterBarrier         chan struct{}
	clusterAckCertificates map[string]clusterAckCertificate
	clusterAckOrder        []string
}

func newUOWBackend(
	ctx context.Context,
	provider *uow.DirectDoltServerProvider,
	expectedProjectID, expectedDatabaseName, expectedLabID string,
	clusterGuard clusterAckGuardConfig,
) (*uowBackend, error) {
	b := &uowBackend{
		provider: provider, projectID: expectedProjectID,
		databaseName: expectedDatabaseName, labID: expectedLabID, clusterGuard: clusterGuard,
	}
	if clusterGuard.Enabled {
		b.clusterBarrier = make(chan struct{}, 1)
		b.clusterAckCertificates = make(map[string]clusterAckCertificate)
	}
	if err := b.verifyIdentity(ctx); err != nil {
		return nil, err
	}
	if err := b.verifyClusterAckStartup(ctx); err != nil {
		return nil, err
	}
	if err := provider.Prewarm(ctx, 2); err != nil {
		return nil, fmt.Errorf("prewarm pool: %w", err)
	}
	return b, nil
}

func (b *uowBackend) Close(ctx context.Context) error {
	return b.provider.Close(ctx)
}

func (b *uowBackend) verifyIdentity(ctx context.Context) error {
	uw, err := b.provider.NewUOW(ctx)
	if err != nil {
		return fmt.Errorf("identity uow: %w", err)
	}
	defer uw.Close(ctx)
	verified, err := uw.ConfigUseCase().VerifyInit(ctx)
	if err != nil {
		return fmt.Errorf("identity verify: %w", err)
	}
	if len(verified.Missing) != 0 {
		return fmt.Errorf("identity verify: required fields missing: %s", strings.Join(verified.Missing, ","))
	}
	if verified.ProjectID != b.projectID {
		return fmt.Errorf("identity verify: database project mismatch")
	}
	_, err = readLabAttestation(ctx, uw, b.projectID, b.databaseName, b.labID)
	return err
}

func (b *uowBackend) Attestation(ctx context.Context) (labAttestation, error) {
	uw, err := b.readUOW(ctx)
	if err != nil {
		return labAttestation{}, err
	}
	defer uw.Close(ctx)
	return readLabAttestation(ctx, uw, b.projectID, b.databaseName, b.labID)
}

func readLabAttestation(
	ctx context.Context,
	uw uow.UnitOfWork,
	expectedProjectID, expectedDatabaseName, expectedLabID string,
) (labAttestation, error) {
	databaseRows, err := uw.RawSQLUseCase().Query(ctx, "SELECT DATABASE()")
	if err != nil {
		return labAttestation{}, fmt.Errorf("lab attestation database identity: %w", err)
	}
	actualDatabase, err := singleStringValue(databaseRows.Rows)
	if err != nil || actualDatabase != expectedDatabaseName {
		return labAttestation{}, fmt.Errorf("lab attestation database identity mismatch")
	}
	markerRows, err := uw.RawSQLUseCase().Query(ctx,
		"SELECT value FROM metadata WHERE `key` = ?", labAttestationMetadataKey)
	if err != nil {
		return labAttestation{}, fmt.Errorf("lab attestation marker: %w", err)
	}
	markerJSON, err := singleStringValue(markerRows.Rows)
	if err != nil {
		return labAttestation{}, fmt.Errorf("lab attestation marker is missing or malformed")
	}
	var marker labAttestation
	if err := json.Unmarshal([]byte(markerJSON), &marker); err != nil {
		return labAttestation{}, fmt.Errorf("lab attestation marker is invalid")
	}
	if err := validateLabAttestation(marker, expectedProjectID, expectedDatabaseName, expectedLabID); err != nil {
		return labAttestation{}, err
	}
	marker.Signature = ""
	return marker, nil
}

func singleStringValue(rows [][]any) (string, error) {
	if len(rows) != 1 || len(rows[0]) != 1 {
		return "", fmt.Errorf("expected one value")
	}
	value, ok := rows[0][0].(string)
	if !ok || value == "" {
		return "", fmt.Errorf("expected a non-empty string")
	}
	return value, nil
}

func validateLabAttestation(marker labAttestation, projectID, databaseName, labID string) error {
	if marker.Version != 1 || marker.Environment != "synthetic" ||
		marker.ProjectID != projectID || marker.DatabaseName != databaseName || marker.LabID != labID {
		return fmt.Errorf("lab attestation identity mismatch")
	}
	return nil
}

func (b *uowBackend) readUOW(ctx context.Context) (uow.UnitOfWork, error) {
	uw, err := b.provider.NewUOW(ctx)
	if err != nil {
		return nil, err
	}
	verified, err := uw.ConfigUseCase().VerifyInit(ctx)
	if err != nil {
		uw.Close(ctx)
		return nil, err
	}
	if len(verified.Missing) != 0 || verified.ProjectID != b.projectID {
		uw.Close(ctx)
		return nil, fmt.Errorf("project identity changed; gateway is fail-closed")
	}
	return uw, nil
}

func (b *uowBackend) Ping(ctx context.Context) (any, error) {
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	page, err := uw.IssueUseCase().SearchIssues(ctx, "", types.IssueFilter{Limit: 1, SkipWisps: true, SkipLabels: true})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "ok", "project_id": b.projectID, "sample_count": len(page.Items)}, nil
}

func (b *uowBackend) Show(ctx context.Context, id string) (any, error) {
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	return uw.IssueUseCase().GetIssue(ctx, id)
}

func (b *uowBackend) List(ctx context.Context, limit int) (any, error) {
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	page, err := uw.IssueUseCase().SearchIssuesWithCounts(ctx, "", types.IssueFilter{Limit: limit})
	if err != nil {
		return nil, err
	}
	return map[string]any{"items": page.Items, "has_more": page.HasMore}, nil
}

func (b *uowBackend) Ready(ctx context.Context, limit int) (any, error) {
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	page, err := uw.IssueUseCase().GetReadyWorkWithCounts(ctx, types.WorkFilter{Limit: limit})
	if err != nil {
		return nil, err
	}
	return map[string]any{"items": page.Items, "has_more": page.HasMore}, nil
}

func (b *uowBackend) PoolStats() any {
	s := b.provider.Stats()
	m := b.provider.SQLMetrics()
	return map[string]any{
		"max_open_connections":     s.MaxOpenConnections,
		"open_connections":         s.OpenConnections,
		"in_use":                   s.InUse,
		"idle":                     s.Idle,
		"wait_count":               s.WaitCount,
		"wait_duration_ns":         s.WaitDuration.Nanoseconds(),
		"sql_statement_count":      m.StatementCount,
		"sql_exec_count":           m.ExecCount,
		"sql_query_count":          m.QueryCount,
		"sql_query_row_count":      m.QueryRowCount,
		"transaction_count":        m.TransactionCount,
		"commit_attempt_count":     m.CommitAttemptCount,
		"commit_success_count":     m.CommitSuccessCount,
		"rollback_count":           m.RollbackCount,
		"sql_call_duration_ns":     m.CallDurationNS,
		"sql_max_call_duration_ns": m.MaxCallDurationNS,
	}
}

func (b *uowBackend) Execute(ctx context.Context, op operation) (*doltReceipt, error) {
	if err := b.validateOperationProject(op); err != nil {
		return nil, err
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)

	if existing, err := receiptFromUOW(ctx, uw, op); err != nil {
		return nil, err
	} else if existing != nil {
		existing.Reconciled = true
		return existing, nil
	}

	result, affected, err := executeOperation(ctx, uw, op)
	if err != nil {
		return nil, classifyDeterministicOperationError(err)
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal operation result: %w", err)
	}
	sort.Strings(affected)
	status := statusSucceeded
	eventPayload, err := json.Marshal(durableOutboxPayload{
		OperationID: op.ID,
		Kind:        op.Kind,
		Status:      status,
		AffectedIDs: affected,
		ResultHash:  digestHex(resultJSON),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal outbox event: %w", err)
	}
	now := time.Now().UTC()
	version := 1
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		version = 2
	}
	receipt := &doltReceipt{
		Version:     version,
		OperationID: op.ID,
		ProjectID:   op.ProjectID,
		KeyHash:     op.KeyHash,
		SubjectHash: op.SubjectHash,
		RequestHash: op.RequestHash,
		Kind:        op.Kind,
		Status:      status,
		Result:      resultJSON,
		Outbox: outboxEvent{
			OperationID: op.ID,
			Sequence:    0,
			EventType:   op.Kind + ".succeeded",
			Payload:     eventPayload,
			CreatedAt:   now,
		},
		CommittedAt: now,
	}
	stored, inserted, err := persistOutcomeInUOW(ctx, uw, op, receipt)
	if err != nil {
		return nil, err
	}
	if !inserted {
		// Another writer already established the immutable canonical outcome.
		// Closing this UOW rolls back this attempt's issue mutations.
		stored.Reconciled = true
		return stored, nil
	}
	if err := uw.Commit(ctx, "beads-perf-lab gateway "+op.ID); err != nil {
		return nil, fmt.Errorf("commit operation: %w", err)
	}
	return stored, nil
}

// PersistPermanentFailure records an acknowledged, immutable terminal failure
// in Dolt. SQLite is only a projection: a terminal HTTP 200 is never based on
// a queue-only failure.
func (b *uowBackend) PersistPermanentFailure(
	ctx context.Context, op operation, code, message string,
) (*doltReceipt, error) {
	if err := b.validateOperationProject(op); err != nil {
		return nil, err
	}
	if !isTerminalIdempotencyNamespace(op.KeyHash) {
		return nil, fmt.Errorf("permanent outcome persistence requires terminal-v2 idempotency")
	}
	if code == "" || message == "" {
		return nil, fmt.Errorf("permanent failure code and message are required")
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	if existing, err := receiptFromUOW(ctx, uw, op); err != nil {
		return nil, err
	} else if existing != nil {
		existing.Reconciled = true
		return existing, nil
	}

	resultJSON, err := json.Marshal(map[string]string{"status": statusFailed, "error_code": code})
	if err != nil {
		return nil, fmt.Errorf("marshal permanent failure result: %w", err)
	}
	eventPayload, err := json.Marshal(durableOutboxPayload{
		OperationID: op.ID,
		Kind:        op.Kind,
		Status:      statusFailed,
		ResultHash:  digestHex(resultJSON),
		ErrorCode:   code,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal permanent failure outbox: %w", err)
	}
	now := time.Now().UTC()
	outcome := &doltReceipt{
		Version: 2, OperationID: op.ID, ProjectID: op.ProjectID,
		KeyHash: op.KeyHash, SubjectHash: op.SubjectHash, RequestHash: op.RequestHash,
		Kind: op.Kind, Status: statusFailed, Result: resultJSON,
		ErrorCode: code, ErrorMessage: message, CommittedAt: now,
		Outbox: outboxEvent{OperationID: op.ID, Sequence: 0,
			EventType: op.Kind + ".failed", Payload: eventPayload, CreatedAt: now},
	}
	stored, inserted, err := persistOutcomeInUOW(ctx, uw, op, outcome)
	if err != nil {
		return nil, err
	}
	if !inserted {
		stored.Reconciled = true
		return stored, nil
	}
	if err := uw.Commit(ctx, "beads-perf-lab gateway terminal failure "+op.ID); err != nil {
		return nil, fmt.Errorf("commit permanent failure: %w", err)
	}
	return stored, nil
}

type durableOutboxPayload struct {
	OperationID string   `json:"operation_id"`
	Kind        string   `json:"kind"`
	Status      string   `json:"status"`
	AffectedIDs []string `json:"affected_ids,omitempty"`
	ResultHash  string   `json:"result_hash"`
	ErrorCode   string   `json:"error_code,omitempty"`
}

type canonicalMetadataUOW interface {
	RawSQLUseCase() domain.RawSQLUseCase
}

// persistOutcomeInUOW uses insert-once/compare-on-conflict semantics. It never
// overwrites an established outcome. inserted=false means an identical-identity
// outcome already exists and the caller must not commit its attempted mutation.
func persistOutcomeInUOW(
	ctx context.Context, uw canonicalMetadataUOW, op operation, outcome *doltReceipt,
) (*doltReceipt, bool, error) {
	if outcome == nil {
		return nil, false, fmt.Errorf("canonical outcome is required")
	}
	if err := validateReceipt(*outcome, op); err != nil {
		return nil, false, err
	}
	receiptJSON, err := json.Marshal(outcome)
	if err != nil {
		return nil, false, fmt.Errorf("marshal canonical outcome: %w", err)
	}
	if err := validateDurableMetadataSizes(receiptJSON, outcome.Outbox.Payload); err != nil {
		return nil, false, err
	}
	if _, err := uw.RawSQLUseCase().Exec(ctx,
		"INSERT INTO metadata (`key`, value) VALUES (?, ?)", receiptKey(op), string(receiptJSON)); err != nil {
		existing, lookupErr := receiptFromUOW(ctx, uw, op)
		if lookupErr != nil {
			return nil, false, errors.Join(fmt.Errorf("insert canonical outcome: %w", err), lookupErr)
		}
		if existing == nil {
			return nil, false, fmt.Errorf("insert canonical outcome: %w", err)
		}
		return existing, false, nil
	}
	if _, err := uw.RawSQLUseCase().Exec(ctx,
		"INSERT INTO metadata (`key`, value) VALUES (?, ?)", outboxKey(op), string(outcome.Outbox.Payload)); err != nil {
		marker, lookupErr := metadataValueFromUOW(ctx, uw, outboxKey(op))
		if lookupErr != nil || marker != string(outcome.Outbox.Payload) {
			return nil, false, errors.Join(fmt.Errorf("insert canonical outbox: %w", err), lookupErr)
		}
	}
	return outcome, true, nil
}

func validateDurableMetadataSizes(receiptJSON, eventPayload []byte) error {
	if len(receiptJSON) > maxMetadataValueBytes {
		return permanentf("operation result is too large for a durable receipt")
	}
	if len(eventPayload) > maxOutboxMarkerValueBytes {
		return permanentf("operation outbox marker is too large")
	}
	return nil
}

func (b *uowBackend) LookupReceipt(ctx context.Context, op operation) (*doltReceipt, error) {
	if err := b.validateOperationProject(op); err != nil {
		return nil, err
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return nil, err
	}
	defer uw.Close(ctx)
	return receiptFromUOW(ctx, uw, op)
}

// LegacyOutcomeExists is a migration fence, not a v1-to-v2 promotion path.
// A terminal-v2 caller cannot safely acknowledge or replay a v1 receipt because
// v1 did not persist a terminal failure contract. Detecting a verified v1 row
// lets the HTTP layer fail closed instead of executing the same client key in a
// second namespace.
func (b *uowBackend) LegacyOutcomeExists(ctx context.Context, projectID, keyHash string) (bool, error) {
	if projectID != b.projectID || keyHash == "" || isTerminalIdempotencyNamespace(keyHash) {
		return false, permanentf("legacy outcome identity mismatch")
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return false, err
	}
	defer uw.Close(ctx)
	value, err := metadataValueFromUOW(ctx, uw, receiptPrefix+keyHash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read legacy outcome: %w", err)
	}
	var receipt doltReceipt
	if err := json.Unmarshal([]byte(value), &receipt); err != nil {
		return false, fmt.Errorf("decode legacy outcome: %w", err)
	}
	if receipt.Version != 1 || receipt.ProjectID != projectID || receipt.KeyHash != keyHash ||
		receipt.OperationID == "" || receipt.SubjectHash == "" || receipt.RequestHash == "" || receipt.Kind == "" {
		return false, errReceiptIdentityMismatch
	}
	op := operation{
		ID: receipt.OperationID, ProjectID: receipt.ProjectID, KeyHash: receipt.KeyHash,
		SubjectHash: receipt.SubjectHash, RequestHash: receipt.RequestHash, Kind: receipt.Kind,
	}
	if err := validateReceipt(receipt, op); err != nil {
		return false, err
	}
	marker, err := metadataValueFromUOW(ctx, uw, outboxPrefix+receipt.OperationID+"/0")
	if err != nil {
		return false, fmt.Errorf("read legacy outbox marker: %w", err)
	}
	if marker != string(receipt.Outbox.Payload) {
		return false, fmt.Errorf("legacy receipt outbox marker mismatch")
	}
	return true, nil
}

func (b *uowBackend) validateOperationProject(op operation) error {
	if op.ProjectID != b.projectID {
		return permanentf("operation project identity mismatch")
	}
	return nil
}

func receiptFromUOW(ctx context.Context, uw canonicalMetadataUOW, op operation) (*doltReceipt, error) {
	value, err := metadataValueFromUOW(ctx, uw, receiptKey(op))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read receipt: %w", err)
	}
	var receipt doltReceipt
	if err := json.Unmarshal([]byte(value), &receipt); err != nil {
		return nil, fmt.Errorf("decode receipt: %w", err)
	}
	if err := validateReceipt(receipt, op); err != nil {
		return nil, err
	}
	marker, err := metadataValueFromUOW(ctx, uw, outboxKey(op))
	if err != nil {
		return nil, fmt.Errorf("read outbox marker: %w", err)
	}
	if marker != string(receipt.Outbox.Payload) {
		return nil, fmt.Errorf("receipt outbox marker mismatch")
	}
	return &receipt, nil
}

func metadataValueFromUOW(ctx context.Context, uw canonicalMetadataUOW, key string) (string, error) {
	rows, err := uw.RawSQLUseCase().Query(ctx, "SELECT value FROM metadata WHERE `key` = ?", key)
	if err != nil {
		return "", err
	}
	if len(rows.Rows) == 0 {
		return "", sql.ErrNoRows
	}
	if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 {
		return "", fmt.Errorf("metadata value cardinality mismatch")
	}
	value, ok := rows.Rows[0][0].(string)
	if !ok {
		return "", fmt.Errorf("metadata value type mismatch")
	}
	return value, nil
}

func validateReceipt(receipt doltReceipt, op operation) error {
	wantVersion := 1
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		wantVersion = 2
	}
	if receipt.Version != wantVersion || receipt.OperationID != op.ID || receipt.ProjectID != op.ProjectID || receipt.KeyHash != op.KeyHash ||
		receipt.SubjectHash != op.SubjectHash || receipt.RequestHash != op.RequestHash || receipt.Kind != op.Kind {
		return errReceiptIdentityMismatch
	}
	status := receipt.Status
	if wantVersion == 1 && status == "" {
		status = statusSucceeded
	}
	if status != statusSucceeded && status != statusFailed {
		return fmt.Errorf("receipt status is invalid")
	}
	if wantVersion == 1 && status != statusSucceeded {
		return fmt.Errorf("legacy receipt cannot represent failure")
	}
	if receipt.Outbox.OperationID != op.ID || receipt.Outbox.Sequence != 0 || receipt.Outbox.EventType != op.Kind+"."+status {
		return fmt.Errorf("receipt outbox identity mismatch")
	}
	if wantVersion == 2 {
		if err := validateTerminalOutcomeStructure(receipt, status); err != nil {
			return err
		}
	}
	return nil
}

func validateTerminalOutcomeStructure(receipt doltReceipt, status string) error {
	if len(receipt.Result) == 0 || !json.Valid(receipt.Result) || receipt.CommittedAt.IsZero() || receipt.Outbox.CreatedAt.IsZero() {
		return fmt.Errorf("terminal outcome result or timestamps are malformed")
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(receipt.Result, &result); err != nil || len(result) == 0 {
		return fmt.Errorf("terminal outcome result is malformed")
	}
	var payload durableOutboxPayload
	dec := json.NewDecoder(strings.NewReader(string(receipt.Outbox.Payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("terminal outcome outbox is malformed")
	}
	if err := requireJSONEOF(dec); err != nil {
		return fmt.Errorf("terminal outcome outbox is malformed")
	}
	if payload.OperationID != receipt.OperationID || payload.Kind != receipt.Kind || payload.Status != status ||
		payload.ResultHash != digestHex(receipt.Result) {
		return fmt.Errorf("terminal outcome outbox does not bind the result")
	}
	switch status {
	case statusSucceeded:
		if receipt.ErrorCode != "" || receipt.ErrorMessage != "" || payload.ErrorCode != "" || len(payload.AffectedIDs) == 0 {
			return fmt.Errorf("successful terminal outcome fields are malformed")
		}
		for _, id := range payload.AffectedIDs {
			if id == "" {
				return fmt.Errorf("successful terminal outcome affected IDs are malformed")
			}
		}
	case statusFailed:
		if receipt.ErrorCode == "" || receipt.ErrorMessage == "" || payload.ErrorCode != receipt.ErrorCode || len(payload.AffectedIDs) != 0 {
			return fmt.Errorf("failed terminal outcome is missing failure fields")
		}
		var failure struct {
			Status    string `json:"status"`
			ErrorCode string `json:"error_code"`
		}
		failureDecoder := json.NewDecoder(strings.NewReader(string(receipt.Result)))
		failureDecoder.DisallowUnknownFields()
		if err := failureDecoder.Decode(&failure); err != nil || requireJSONEOF(failureDecoder) != nil ||
			failure.Status != statusFailed || failure.ErrorCode != receipt.ErrorCode {
			return fmt.Errorf("failed terminal outcome result is malformed")
		}
	}
	return nil
}

func receiptKey(op operation) string {
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		return terminalOutcomePrefix + strings.TrimPrefix(op.KeyHash, terminalIdempotencyNamespace)
	}
	return receiptPrefix + op.KeyHash
}

func outboxKey(op operation) string {
	if isTerminalIdempotencyNamespace(op.KeyHash) {
		return terminalOutboxPrefix + op.ID + "/0"
	}
	return outboxPrefix + op.ID + "/0"
}

func digestHex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

type createPayload struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
	Priority    *int   `json:"priority,omitempty"`
}

type updatePayload struct {
	ID          string        `json:"id"`
	Title       *string       `json:"title,omitempty"`
	Description *string       `json:"description,omitempty"`
	Status      *types.Status `json:"status,omitempty"`
	Priority    *int          `json:"priority,omitempty"`
	Assignee    *string       `json:"assignee,omitempty"`
}

type closePayload struct {
	ID     string `json:"id"`
	Reason string `json:"reason,omitempty"`
}

type claimPayload struct {
	ID string `json:"id"`
}

type batchPayload struct {
	Actions []batchAction `json:"actions"`
}

type batchAction struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type graphPayload struct {
	CommitMessage string      `json:"commit_message,omitempty"`
	Nodes         []graphNode `json:"nodes"`
	Edges         []graphEdge `json:"edges,omitempty"`
}

type graphNode struct {
	Key         string          `json:"key"`
	Title       string          `json:"title"`
	Description string          `json:"description,omitempty"`
	Type        types.IssueType `json:"type,omitempty"`
	Priority    *int            `json:"priority,omitempty"`
}

type graphEdge struct {
	FromKey string               `json:"from_key"`
	ToKey   string               `json:"to_key"`
	Type    types.DependencyType `json:"type,omitempty"`
}

func executeOperation(ctx context.Context, uw uow.UnitOfWork, op operation) (any, []string, error) {
	switch op.Kind {
	case "issue.create", "issue.update", "issue.close", "issue.claim":
		result, ids, err := executeAction(ctx, uw.IssueUseCase(), batchAction{Kind: op.Kind, Payload: op.Payload}, actorFor(op))
		return result, ids, err
	case "batch.apply":
		var payload batchPayload
		if err := decodePayload(op.Payload, &payload); err != nil {
			return nil, nil, err
		}
		if len(payload.Actions) == 0 || len(payload.Actions) > 500 {
			return nil, nil, permanentf("batch must contain between 1 and 500 actions")
		}
		results := make([]any, 0, len(payload.Actions))
		var affected []string
		for i, action := range payload.Actions {
			result, ids, err := executeAction(ctx, uw.IssueUseCase(), action, actorFor(op))
			if err != nil {
				return nil, nil, fmt.Errorf("batch action %d: %w", i, err)
			}
			results = append(results, result)
			affected = append(affected, ids...)
		}
		return map[string]any{"results": results}, affected, nil
	case "graph.apply":
		return executeGraph(ctx, uw.IssueUseCase(), op.Payload, actorFor(op))
	default:
		return nil, nil, permanentf("unsupported operation kind %q", op.Kind)
	}
}

func executeAction(ctx context.Context, uc domain.IssueUseCase, action batchAction, actor string) (any, []string, error) {
	switch action.Kind {
	case "issue.create":
		var payload createPayload
		if err := decodePayload(action.Payload, &payload); err != nil {
			return nil, nil, err
		}
		if err := validateSyntheticTitle(payload.Title); err != nil {
			return nil, nil, err
		}
		issueType := types.IssueType(payload.Type)
		if issueType == "" {
			issueType = types.TypeTask
		}
		if !allowedIssueType(issueType) {
			return nil, nil, permanentf("unsupported issue type %q", issueType)
		}
		priority := 2
		if payload.Priority != nil {
			priority = *payload.Priority
		}
		if priority < 0 || priority > 4 {
			return nil, nil, permanentf("priority must be between 0 and 4")
		}
		result, err := uc.CreateIssue(ctx, domain.CreateIssueParams{Issue: &types.Issue{
			Title:       payload.Title,
			Description: payload.Description,
			IssueType:   issueType,
			Status:      types.StatusOpen,
			Priority:    priority,
		}}, actor)
		if err != nil {
			return nil, nil, err
		}
		return result.Issue, []string{result.Issue.ID}, nil
	case "issue.update":
		var payload updatePayload
		if err := decodePayload(action.Payload, &payload); err != nil {
			return nil, nil, err
		}
		if err := validateUpdatePayload(payload); err != nil {
			return nil, nil, err
		}
		if err := requireSyntheticIssue(ctx, uc, payload.ID); err != nil {
			return nil, nil, err
		}
		updates := map[string]any{}
		if payload.Title != nil {
			if err := validateSyntheticTitle(*payload.Title); err != nil {
				return nil, nil, err
			}
			updates["title"] = *payload.Title
		}
		if payload.Description != nil {
			updates["description"] = *payload.Description
		}
		if payload.Status != nil {
			updates["status"] = *payload.Status
		}
		if payload.Priority != nil {
			updates["priority"] = *payload.Priority
		}
		if payload.Assignee != nil {
			updates["assignee"] = *payload.Assignee
		}
		if len(updates) == 0 {
			return nil, nil, permanentf("update has no fields")
		}
		if err := uc.UpdateIssue(ctx, payload.ID, updates, actor); err != nil {
			return nil, nil, err
		}
		issue, err := uc.GetIssue(ctx, payload.ID)
		return issue, []string{payload.ID}, err
	case "issue.close":
		var payload closePayload
		if err := decodePayload(action.Payload, &payload); err != nil {
			return nil, nil, err
		}
		if err := requireSyntheticIssue(ctx, uc, payload.ID); err != nil {
			return nil, nil, err
		}
		result, err := uc.CloseIssue(ctx, payload.ID, domain.CloseIssueParams{Reason: payload.Reason}, actor)
		return result, []string{payload.ID}, err
	case "issue.claim":
		var payload claimPayload
		if err := decodePayload(action.Payload, &payload); err != nil {
			return nil, nil, err
		}
		if err := requireSyntheticIssue(ctx, uc, payload.ID); err != nil {
			return nil, nil, err
		}
		result, err := uc.ClaimIssueIfOpen(ctx, payload.ID, actor)
		if err != nil {
			return nil, nil, err
		}
		issue, err := uc.GetIssue(ctx, payload.ID)
		return map[string]any{"claim": result, "issue": issue}, []string{payload.ID}, err
	default:
		return nil, nil, permanentf("unsupported batch action %q", action.Kind)
	}
}

func validateUpdatePayload(payload updatePayload) error {
	if payload.Status != nil && !payload.Status.IsValid() {
		return permanentf("unsupported issue status %q", *payload.Status)
	}
	if payload.Priority != nil && (*payload.Priority < 0 || *payload.Priority > 4) {
		return permanentf("priority must be between 0 and 4")
	}
	return nil
}

func executeGraph(ctx context.Context, uc domain.IssueUseCase, raw json.RawMessage, actor string) (any, []string, error) {
	var payload graphPayload
	if err := decodePayload(raw, &payload); err != nil {
		return nil, nil, err
	}
	if err := validateGraph(payload); err != nil {
		return nil, nil, err
	}
	plan := domain.GraphPlan{Nodes: make([]domain.GraphNode, 0, len(payload.Nodes)), Edges: make([]domain.GraphEdge, 0, len(payload.Edges))}
	for _, node := range payload.Nodes {
		priority := 2
		if node.Priority != nil {
			priority = *node.Priority
		}
		issueType := node.Type
		if issueType == "" {
			issueType = types.TypeTask
		}
		plan.Nodes = append(plan.Nodes, domain.GraphNode{Key: node.Key, Issue: &types.Issue{
			Title: node.Title, Description: node.Description, IssueType: issueType,
			Status: types.StatusOpen, Priority: priority,
		}})
	}
	for _, edge := range payload.Edges {
		depType := edge.Type
		if depType == "" {
			depType = types.DepBlocks
		}
		plan.Edges = append(plan.Edges, domain.GraphEdge{FromKey: edge.FromKey, ToKey: edge.ToKey, Type: depType})
	}
	result, err := uc.ApplyIssueGraph(ctx, plan, actor)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(result.IDs))
	for _, id := range result.IDs {
		ids = append(ids, id)
	}
	return map[string]any{
		"contract": "synthetic-blocking-dag-v1", "ids": result.IDs,
		"node_count": len(payload.Nodes), "edge_count": len(payload.Edges),
	}, ids, nil
}

func validateGraph(payload graphPayload) error {
	if payload.CommitMessage != "" {
		return permanentf("commit_message is not part of synthetic-blocking-dag-v1")
	}
	if len(payload.Nodes) == 0 || len(payload.Nodes) > 200 {
		return permanentf("graph must contain between 1 and 200 nodes")
	}
	if len(payload.Edges) > 500 {
		return permanentf("graph must contain at most 500 edges")
	}
	keys := make(map[string]struct{}, len(payload.Nodes))
	for _, node := range payload.Nodes {
		if node.Key == "" {
			return permanentf("graph node key must not be empty")
		}
		if _, exists := keys[node.Key]; exists {
			return permanentf("duplicate graph node key %q", node.Key)
		}
		keys[node.Key] = struct{}{}
		if err := validateSyntheticTitle(node.Title); err != nil {
			return err
		}
		issueType := node.Type
		if issueType == "" {
			issueType = types.TypeTask
		}
		if !allowedIssueType(issueType) {
			return permanentf("unsupported issue type %q", issueType)
		}
		if node.Priority != nil && (*node.Priority < 0 || *node.Priority > 4) {
			return permanentf("priority must be between 0 and 4")
		}
	}
	adj := make(map[string][]string, len(keys))
	seenEdges := make(map[string]struct{}, len(payload.Edges))
	for _, edge := range payload.Edges {
		if _, ok := keys[edge.FromKey]; !ok {
			return permanentf("edge from_key %q is not a graph node", edge.FromKey)
		}
		if _, ok := keys[edge.ToKey]; !ok {
			return permanentf("edge to_key %q is not a graph node", edge.ToKey)
		}
		depType := edge.Type
		if depType == "" {
			depType = types.DepBlocks
		}
		if depType != types.DepBlocks && depType != types.DepConditionalBlocks {
			return permanentf("gateway graph supports only blocking edges")
		}
		key := edge.FromKey + "\x00" + edge.ToKey + "\x00" + string(depType)
		if _, exists := seenEdges[key]; exists {
			return permanentf("duplicate graph edge")
		}
		seenEdges[key] = struct{}{}
		adj[edge.FromKey] = append(adj[edge.FromKey], edge.ToKey)
	}
	state := make(map[string]uint8, len(keys))
	var visit func(string) bool
	visit = func(node string) bool {
		if state[node] == 1 {
			return false
		}
		if state[node] == 2 {
			return true
		}
		state[node] = 1
		for _, next := range adj[node] {
			if !visit(next) {
				return false
			}
		}
		state[node] = 2
		return true
	}
	for key := range keys {
		if !visit(key) {
			return permanentf("graph contains a blocking cycle")
		}
	}
	return nil
}

func requireSyntheticIssue(ctx context.Context, uc domain.IssueUseCase, id string) error {
	if id == "" {
		return permanentf("issue id must not be empty")
	}
	issue, err := uc.GetIssue(ctx, id)
	if err != nil {
		return err
	}
	return validateSyntheticTitle(issue.Title)
}

func validateSyntheticTitle(title string) error {
	if !strings.HasPrefix(title, "[beads-perf-lab]") {
		return permanentf("gateway prototype accepts synthetic lab titles only")
	}
	return nil
}

func allowedIssueType(t types.IssueType) bool {
	switch t {
	case types.TypeTask, types.TypeBug, types.TypeFeature, types.TypeEpic, types.TypeChore:
		return true
	default:
		return false
	}
}

func decodePayload(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return permanentf("payload is required")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return permanentf("invalid payload: %v", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return permanentf("invalid payload: %v", err)
	}
	return nil
}

func actorFor(op operation) string {
	prefix := op.SubjectHash
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	return "gateway-" + prefix
}

type permanentError struct{ error }

func permanentf(format string, args ...any) error {
	return permanentError{error: fmt.Errorf(format, args...)}
}

func classifyDeterministicOperationError(err error) error {
	switch {
	case err == nil, isPermanent(err):
		return err
	case errors.Is(err, sql.ErrNoRows), errors.Is(err, storagepkg.ErrNotFound):
		return permanentf("issue not found")
	case errors.Is(err, storagepkg.ErrAlreadyClaimed):
		return permanentf("issue is already claimed")
	case errors.Is(err, storagepkg.ErrNotClaimable):
		return permanentf("issue is not claimable")
	default:
		return err
	}
}

func isPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}
