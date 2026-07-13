package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain"
)

// clusterAckGuardConfig is intentionally opt-in. The existing single-node lab
// has no dolt_cluster database and therefore retains its current behavior when
// Enabled is false.
type clusterAckGuardConfig struct {
	Enabled          bool
	ExpectedEpoch    int64
	RequiredStandbys []string
}

type clusterAckObservation struct {
	Database       string
	Standby        string
	Role           string
	Epoch          int64
	LagMillis      *int64
	LastUpdate     *time.Time
	CurrentError   *string
	GlobalRole     string
	GlobalEpoch    int64
	AckTimeoutSecs int64
	Head           string
}

const maxClusterAckCertificates = 4096

type clusterAckCertificate struct {
	ReceiptDigest    string
	RoleEpoch        int64
	Standbys         string
	AcknowledgedHead string
}

func clusterAckGuardConfigFromGateway(cfg gatewayConfig) (clusterAckGuardConfig, error) {
	if !cfg.ClusterAckGuard {
		if cfg.ClusterRoleEpoch != 0 || strings.TrimSpace(cfg.ClusterStandbys) != "" {
			return clusterAckGuardConfig{}, fmt.Errorf("cluster role epoch and standbys require --cluster-ack-guard")
		}
		return clusterAckGuardConfig{}, nil
	}
	if cfg.ClusterRoleEpoch < 1 {
		return clusterAckGuardConfig{}, fmt.Errorf("cluster acknowledgement guard requires a positive expected role epoch")
	}
	parts := strings.Split(cfg.ClusterStandbys, ",")
	seen := make(map[string]struct{}, len(parts))
	standbys := make([]string, 0, len(parts))
	for _, raw := range parts {
		name := strings.TrimSpace(raw)
		if !validClusterRemoteName(name) {
			return clusterAckGuardConfig{}, fmt.Errorf("cluster acknowledgement guard has an invalid standby remote name")
		}
		if _, exists := seen[name]; exists {
			return clusterAckGuardConfig{}, fmt.Errorf("cluster acknowledgement guard standby remotes must be unique")
		}
		seen[name] = struct{}{}
		standbys = append(standbys, name)
	}
	if len(standbys) == 0 {
		return clusterAckGuardConfig{}, fmt.Errorf("cluster acknowledgement guard requires at least one standby")
	}
	sort.Strings(standbys)
	return clusterAckGuardConfig{Enabled: true, ExpectedEpoch: int64(cfg.ClusterRoleEpoch), RequiredStandbys: standbys}, nil
}

func validClusterRemoteName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

const clusterAckStartupQuery = `SELECT s.` + "`database`" + `, s.standby_remote, s.role, s.epoch,
s.replication_lag_millis, s.last_update, s.current_error,
@@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch,
@@GLOBAL.dolt_cluster_ack_writes_timeout_secs, DOLT_HASHOF('HEAD')
FROM dolt_cluster.dolt_cluster_status AS s
WHERE s.` + "`database`" + ` = ? ORDER BY s.standby_remote`

const clusterAckTerminalQuery = `SELECT receipt.value, outbox.value,
s.` + "`database`" + `, s.standby_remote, s.role, s.epoch,
s.replication_lag_millis, s.last_update, s.current_error,
@@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch,
@@GLOBAL.dolt_cluster_ack_writes_timeout_secs, DOLT_HASHOF('HEAD')
FROM metadata AS receipt
JOIN metadata AS outbox ON outbox.` + "`key`" + ` = ?
JOIN dolt_cluster.dolt_cluster_status AS s ON s.` + "`database`" + ` = ?
WHERE receipt.` + "`key`" + ` = ? ORDER BY s.standby_remote`

func (b *uowBackend) verifyClusterAckStartup(ctx context.Context) error {
	if !b.clusterGuard.Enabled {
		return nil
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement startup check: %w", err)
	}
	defer uw.Close(ctx)
	rows, err := uw.RawSQLUseCase().Query(ctx, clusterAckStartupQuery, b.databaseName)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement startup check unavailable: %w", err)
	}
	observations, err := parseClusterAckRows(rows, 0)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement startup check malformed: %w", err)
	}
	if err := validateClusterAckObservations(observations, b.databaseName, b.clusterGuard); err != nil {
		return fmt.Errorf("cluster acknowledgement startup check failed: %w", err)
	}
	return nil
}

// VerifyTerminalDurability binds the exact immutable receipt and outbox marker
// to a warning-free Dolt commit barrier. Dolt intentionally reports replication
// wait failure as warning 3024 while returning SQL success, so the warning must
// be read immediately from the same pinned session. The allow-empty commit
// advances HEAD after the canonical outcome; acknowledgement of that later HEAD
// proves the outcome is present on every configured standby. A post-barrier
// cluster-status snapshot fences role/epoch/topology changes and requires every
// hook to report nextHead == lastPushedHead (replication_lag_millis=0).
func (b *uowBackend) VerifyTerminalDurability(ctx context.Context, op operation, receipt *doltReceipt) error {
	if !b.clusterGuard.Enabled {
		return nil
	}
	if receipt == nil {
		return fmt.Errorf("cluster acknowledgement requires a canonical outcome")
	}
	if err := validateReceipt(*receipt, op); err != nil {
		return fmt.Errorf("cluster acknowledgement outcome identity: %w", err)
	}
	select {
	case b.clusterBarrier <- struct{}{}:
		defer func() { <-b.clusterBarrier }()
	case <-ctx.Done():
		return fmt.Errorf("cluster acknowledgement barrier wait: %w", ctx.Err())
	}
	uw, err := b.readUOW(ctx)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement uow: %w", err)
	}
	defer uw.Close(ctx)
	raw := uw.RawSQLUseCase()
	rows, err := raw.Query(ctx, clusterAckTerminalQuery,
		outboxKey(op), b.databaseName, receiptKey(op))
	if err != nil {
		return fmt.Errorf("cluster acknowledgement barrier unavailable: %w", err)
	}
	if err := validateCanonicalBarrierRows(rows, op, receipt); err != nil {
		return fmt.Errorf("cluster acknowledgement canonical binding: %w", err)
	}
	observations, err := parseClusterAckRows(rows, 2)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement barrier malformed: %w", err)
	}
	if err := validateClusterAckObservations(observations, b.databaseName, b.clusterGuard); err != nil {
		return fmt.Errorf("cluster acknowledgement pre-barrier not satisfied: %w", err)
	}
	if b.hasClusterAckCertificate(op, receipt) {
		// The canonical outcome was already acknowledged by this process. The
		// exact receipt/outbox, role epoch, standby set, and current zero-lag
		// topology were revalidated above, so replay needs no history-producing
		// allow-empty commit.
		return nil
	}
	if err := executeClusterAckBarrier(ctx, raw, op); err != nil {
		return err
	}
	rows, err = raw.Query(ctx, clusterAckTerminalQuery,
		outboxKey(op), b.databaseName, receiptKey(op))
	if err != nil {
		return fmt.Errorf("cluster acknowledgement post-barrier unavailable: %w", err)
	}
	if err := validateCanonicalBarrierRows(rows, op, receipt); err != nil {
		return fmt.Errorf("cluster acknowledgement post-barrier canonical binding: %w", err)
	}
	observations, err = parseClusterAckRows(rows, 2)
	if err != nil {
		return fmt.Errorf("cluster acknowledgement post-barrier malformed: %w", err)
	}
	if err := validateClusterAckObservations(observations, b.databaseName, b.clusterGuard); err != nil {
		return fmt.Errorf("cluster acknowledgement post-barrier not satisfied: %w", err)
	}
	b.recordClusterAckCertificate(op, receipt, observations[0].Head)
	return nil
}

func canonicalReceiptDigest(receipt *doltReceipt) string {
	if receipt == nil {
		return ""
	}
	copy := *receipt
	copy.Reconciled = false
	body, err := json.Marshal(copy)
	if err != nil {
		return ""
	}
	return digestHex(body)
}

func (b *uowBackend) hasClusterAckCertificate(op operation, receipt *doltReceipt) bool {
	if b == nil || b.clusterAckCertificates == nil {
		return false
	}
	certificate, ok := b.clusterAckCertificates[op.ID]
	return ok && certificate.ReceiptDigest == canonicalReceiptDigest(receipt) &&
		certificate.RoleEpoch == b.clusterGuard.ExpectedEpoch &&
		certificate.Standbys == strings.Join(b.clusterGuard.RequiredStandbys, ",")
}

func (b *uowBackend) recordClusterAckCertificate(op operation, receipt *doltReceipt, head string) {
	if b.clusterAckCertificates == nil {
		b.clusterAckCertificates = make(map[string]clusterAckCertificate)
	}
	if _, exists := b.clusterAckCertificates[op.ID]; !exists {
		if len(b.clusterAckOrder) >= maxClusterAckCertificates {
			oldest := b.clusterAckOrder[0]
			b.clusterAckOrder = b.clusterAckOrder[1:]
			delete(b.clusterAckCertificates, oldest)
		}
		b.clusterAckOrder = append(b.clusterAckOrder, op.ID)
	}
	b.clusterAckCertificates[op.ID] = clusterAckCertificate{
		ReceiptDigest:    canonicalReceiptDigest(receipt),
		RoleEpoch:        b.clusterGuard.ExpectedEpoch,
		Standbys:         strings.Join(b.clusterGuard.RequiredStandbys, ","),
		AcknowledgedHead: head,
	}
}

func executeClusterAckBarrier(ctx context.Context, raw domain.RawSQLUseCase, op operation) error {
	message := "beads-perf-lab gateway cluster-ack barrier " + op.ID
	// Do not use -A here: this proof transaction intentionally has no data
	// changes and must not stage unrelated working-set changes.
	if _, err := raw.Exec(ctx, "CALL DOLT_COMMIT('--allow-empty', '-m', ?)", message); err != nil {
		return fmt.Errorf("cluster acknowledgement barrier commit failed: %w", err)
	}
	warnings, err := raw.Query(ctx, "SHOW WARNINGS")
	if err != nil {
		return fmt.Errorf("cluster acknowledgement barrier warnings unavailable: %w", err)
	}
	if err := validateClusterAckWarnings(warnings); err != nil {
		return fmt.Errorf("cluster acknowledgement barrier was not acknowledged: %w", err)
	}
	return nil
}

func validateClusterAckWarnings(result *domain.RawSQLResult) error {
	if result == nil {
		return fmt.Errorf("warning result is missing")
	}
	for _, row := range result.Rows {
		if len(row) != 3 {
			return fmt.Errorf("warning result is malformed")
		}
		_, okLevel := stringValue(row[0])
		code, okCode := int64Value(row[1])
		_, okMessage := stringValue(row[2])
		if !okLevel || !okCode || !okMessage {
			return fmt.Errorf("warning result is malformed")
		}
		// ERQueryTimeout (3024) is Dolt's documented replication-wait
		// failure signal. Any other warning is also fail-closed because this
		// barrier has no legitimate warning contract.
		if code == 3024 {
			return fmt.Errorf("barrier commit returned a replication timeout warning")
		}
		return fmt.Errorf("barrier commit returned an unexpected warning")
	}
	return nil
}

func validateCanonicalBarrierRows(rows *domain.RawSQLResult, op operation, want *doltReceipt) error {
	if rows == nil || len(rows.Rows) == 0 || len(rows.Rows[0]) < 2 {
		return fmt.Errorf("canonical outcome, outbox, or cluster status is missing")
	}
	var canonical doltReceipt
	receiptJSON, ok := stringValue(rows.Rows[0][0])
	if !ok || json.Unmarshal([]byte(receiptJSON), &canonical) != nil {
		return fmt.Errorf("canonical outcome is malformed")
	}
	if err := validateReceipt(canonical, op); err != nil {
		return err
	}
	wantCopy := *want
	wantCopy.Reconciled = false
	canonical.Reconciled = false
	if !reflect.DeepEqual(canonical, wantCopy) {
		return fmt.Errorf("canonical outcome changed during acknowledgement")
	}
	outbox, ok := stringValue(rows.Rows[0][1])
	if !ok || outbox != string(canonical.Outbox.Payload) {
		return fmt.Errorf("canonical outbox marker mismatch")
	}
	for _, row := range rows.Rows[1:] {
		if len(row) < 2 || row[0] != rows.Rows[0][0] || row[1] != rows.Rows[0][1] {
			return fmt.Errorf("canonical outcome snapshot is inconsistent")
		}
	}
	return nil
}

func parseClusterAckRows(result *domain.RawSQLResult, prefix int) ([]clusterAckObservation, error) {
	if result == nil || len(result.Rows) == 0 {
		return nil, fmt.Errorf("no cluster status rows")
	}
	observations := make([]clusterAckObservation, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) != prefix+11 {
			return nil, fmt.Errorf("unexpected cluster status column count")
		}
		database, okDatabase := stringValue(row[prefix])
		standby, okStandby := stringValue(row[prefix+1])
		role, okRole := stringValue(row[prefix+2])
		epoch, okEpoch := int64Value(row[prefix+3])
		lag, okLag := nullableInt64Value(row[prefix+4])
		lastUpdate, okLastUpdate := nullableTimeValue(row[prefix+5])
		currentError, okCurrentError := nullableStringValue(row[prefix+6])
		globalRole, okGlobalRole := stringValue(row[prefix+7])
		globalEpoch, okGlobalEpoch := int64Value(row[prefix+8])
		ackTimeout, okAckTimeout := int64Value(row[prefix+9])
		head, okHead := stringValue(row[prefix+10])
		if !okDatabase || !okStandby || !okRole || !okEpoch || !okLag || !okLastUpdate ||
			!okCurrentError || !okGlobalRole || !okGlobalEpoch || !okAckTimeout || !okHead {
			return nil, fmt.Errorf("cluster status value has an unexpected type")
		}
		observations = append(observations, clusterAckObservation{
			Database: database, Standby: standby, Role: role, Epoch: epoch,
			LagMillis: lag, LastUpdate: lastUpdate, CurrentError: currentError,
			GlobalRole: globalRole, GlobalEpoch: globalEpoch, AckTimeoutSecs: ackTimeout, Head: head,
		})
	}
	return observations, nil
}

func validateClusterAckObservations(observations []clusterAckObservation, database string, cfg clusterAckGuardConfig) error {
	if !cfg.Enabled || len(cfg.RequiredStandbys) == 0 || cfg.ExpectedEpoch < 1 {
		return fmt.Errorf("cluster acknowledgement configuration is incomplete")
	}
	want := make(map[string]struct{}, len(cfg.RequiredStandbys))
	for _, name := range cfg.RequiredStandbys {
		want[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(observations))
	var head string
	for _, observation := range observations {
		if observation.Database != database || observation.Role != "primary" || observation.GlobalRole != "primary" {
			return fmt.Errorf("database is not served by the expected primary")
		}
		if observation.Epoch != cfg.ExpectedEpoch || observation.GlobalEpoch != cfg.ExpectedEpoch {
			return fmt.Errorf("cluster role epoch mismatch")
		}
		if observation.AckTimeoutSecs <= 0 {
			return fmt.Errorf("cluster write acknowledgement timeout is disabled")
		}
		if observation.LagMillis == nil || *observation.LagMillis != 0 || observation.LastUpdate == nil ||
			observation.LastUpdate.IsZero() || observation.CurrentError != nil {
			return fmt.Errorf("standby is not caught up")
		}
		if observation.Head == "" || (head != "" && observation.Head != head) {
			return fmt.Errorf("cluster barrier HEAD is missing or inconsistent")
		}
		head = observation.Head
		if _, required := want[observation.Standby]; !required {
			return fmt.Errorf("cluster standby set differs from the required set")
		}
		if _, duplicate := seen[observation.Standby]; duplicate {
			return fmt.Errorf("cluster status contains a duplicate standby")
		}
		seen[observation.Standby] = struct{}{}
	}
	if len(seen) != len(want) {
		return fmt.Errorf("cluster standby set differs from the required set")
	}
	return nil
}

func stringValue(value any) (string, bool) {
	text, ok := value.(string)
	return text, ok && text != ""
}

func int64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case uint64:
		if typed > (^uint64(0) >> 1) {
			return 0, false
		}
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func nullableInt64Value(value any) (*int64, bool) {
	if value == nil {
		return nil, true
	}
	parsed, ok := int64Value(value)
	return &parsed, ok
}

func nullableStringValue(value any) (*string, bool) {
	if value == nil {
		return nil, true
	}
	text, ok := value.(string)
	return &text, ok
}

func nullableTimeValue(value any) (*time.Time, bool) {
	if value == nil {
		return nil, true
	}
	parsed, ok := value.(time.Time)
	return &parsed, ok
}
