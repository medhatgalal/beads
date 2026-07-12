package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain"
)

type clusterAckRawFake struct {
	execQuery  string
	execArgs   []any
	execErr    error
	queryQuery string
	warnings   *domain.RawSQLResult
	queryErr   error
}

func (f *clusterAckRawFake) Exec(_ context.Context, query string, args ...any) (int64, error) {
	f.execQuery = query
	f.execArgs = append([]any(nil), args...)
	return 0, f.execErr
}

func (f *clusterAckRawFake) Query(_ context.Context, query string, _ ...any) (*domain.RawSQLResult, error) {
	f.queryQuery = query
	return f.warnings, f.queryErr
}

func TestClusterAckGuardConfigIsExplicitAndFenced(t *testing.T) {
	disabled, err := clusterAckGuardConfigFromGateway(gatewayConfig{})
	if err != nil || disabled.Enabled {
		t.Fatalf("disabled config=%#v err=%v", disabled, err)
	}

	tests := []gatewayConfig{
		{ClusterRoleEpoch: 3},
		{ClusterAckGuard: true, ClusterStandbys: "standby-a"},
		{ClusterAckGuard: true, ClusterRoleEpoch: 3},
		{ClusterAckGuard: true, ClusterRoleEpoch: 3, ClusterStandbys: "standby-a,standby-a"},
		{ClusterAckGuard: true, ClusterRoleEpoch: 3, ClusterStandbys: "standby-a, bad remote"},
		{ClusterAckGuard: true, ClusterRoleEpoch: 3, ClusterStandbys: "standby-a,standby-b;DROP"},
	}
	for i, cfg := range tests {
		if _, err := clusterAckGuardConfigFromGateway(cfg); err == nil {
			t.Fatalf("invalid cluster config %d accepted: %#v", i, cfg)
		}
	}

	guard, err := clusterAckGuardConfigFromGateway(gatewayConfig{
		ClusterAckGuard: true, ClusterRoleEpoch: 7, ClusterStandbys: "standby-b, standby-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !guard.Enabled || guard.ExpectedEpoch != 7 || strings.Join(guard.RequiredStandbys, ",") != "standby-a,standby-b" {
		t.Fatalf("guard=%#v", guard)
	}
}

func validClusterAckResult(t *testing.T) *domain.RawSQLResult {
	t.Helper()
	now := time.Now().UTC()
	return &domain.RawSQLResult{Rows: [][]any{
		{testDatabase, "standby-a", "primary", int64(7), int64(0), now, nil, "primary", int64(7), int64(10), "head-1"},
		{testDatabase, "standby-b", "primary", int64(7), int64(0), now, nil, "primary", int64(7), int64(10), "head-1"},
	}}
}

func clusterAckGuardForTest() clusterAckGuardConfig {
	return clusterAckGuardConfig{Enabled: true, ExpectedEpoch: 7, RequiredStandbys: []string{"standby-a", "standby-b"}}
}

func TestClusterAckObservationRequiresExactHealthySet(t *testing.T) {
	valid := validClusterAckResult(t)
	observations, err := parseClusterAckRows(valid, 0)
	if err != nil || validateClusterAckObservations(observations, testDatabase, clusterAckGuardForTest()) != nil {
		t.Fatalf("valid observations=%#v parse_err=%v", observations, err)
	}

	tests := []struct {
		name string
		edit func(*domain.RawSQLResult)
	}{
		{name: "missing standby", edit: func(r *domain.RawSQLResult) { r.Rows = r.Rows[:1] }},
		{name: "unexpected standby", edit: func(r *domain.RawSQLResult) { r.Rows[1][1] = "standby-c" }},
		{name: "duplicate standby", edit: func(r *domain.RawSQLResult) { r.Rows[1][1] = "standby-a" }},
		{name: "standby lag", edit: func(r *domain.RawSQLResult) { r.Rows[0][4] = int64(1) }},
		{name: "unknown lag", edit: func(r *domain.RawSQLResult) { r.Rows[0][4] = nil }},
		{name: "replication error", edit: func(r *domain.RawSQLResult) { r.Rows[0][6] = "standby unavailable" }},
		{name: "ack disabled", edit: func(r *domain.RawSQLResult) { r.Rows[0][9] = int64(0) }},
		{name: "stale epoch", edit: func(r *domain.RawSQLResult) { r.Rows[0][8] = int64(6) }},
		{name: "not primary", edit: func(r *domain.RawSQLResult) { r.Rows[0][7] = "standby" }},
		{name: "inconsistent head", edit: func(r *domain.RawSQLResult) { r.Rows[1][10] = "head-2" }},
		{name: "no successful update", edit: func(r *domain.RawSQLResult) { r.Rows[0][5] = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validClusterAckResult(t)
			tt.edit(result)
			observations, parseErr := parseClusterAckRows(result, 0)
			if parseErr == nil && validateClusterAckObservations(observations, testDatabase, clusterAckGuardForTest()) == nil {
				t.Fatal("unsafe cluster state accepted")
			}
		})
	}
}

func TestClusterAckCanonicalRowsBindReceiptAndOutbox(t *testing.T) {
	op := operation{
		ID: "11111111-2222-4333-8444-555555555555", ProjectID: testProjectID,
		KeyHash:     terminalIdempotencyKeyHash(testStableKey, "cluster-ack-key"),
		SubjectHash: "subject-hash", RequestHash: "request-hash", Kind: "issue.create",
	}
	receipt := terminalSuccessOutcomeForTest(op, json.RawMessage(`{"ok":true}`))
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	cluster := validClusterAckResult(t)
	rows := &domain.RawSQLResult{}
	for _, status := range cluster.Rows {
		rows.Rows = append(rows.Rows, append([]any{string(receiptJSON), string(receipt.Outbox.Payload)}, status...))
	}
	if err := validateCanonicalBarrierRows(rows, op, receipt); err != nil {
		t.Fatalf("valid canonical barrier: %v", err)
	}
	rows.Rows[1][1] = "changed outbox"
	if err := validateCanonicalBarrierRows(rows, op, receipt); err == nil {
		t.Fatal("inconsistent canonical outbox accepted")
	}
	if err := validateCanonicalBarrierRows(&domain.RawSQLResult{Rows: [][]any{{"short"}}}, op, receipt); err == nil {
		t.Fatal("short canonical row accepted")
	}
}

func TestClusterAckWarningsFailClosed(t *testing.T) {
	if err := validateClusterAckWarnings(&domain.RawSQLResult{}); err != nil {
		t.Fatalf("empty warning set: %v", err)
	}
	tests := []*domain.RawSQLResult{
		nil,
		{Rows: [][]any{{"Warning", int64(3024), "Timed out replication of commit to 1 out of 1 replicas."}}},
		{Rows: [][]any{{"Warning", int64(9999), "unexpected warning"}}},
		{Rows: [][]any{{"short", int64(3024)}}},
		{Rows: [][]any{{"Warning", "not-a-number", "bad code"}}},
	}
	for i, result := range tests {
		if err := validateClusterAckWarnings(result); err == nil {
			t.Fatalf("unsafe warning result %d accepted: %#v", i, result)
		}
	}
}

func TestClusterAckCertificateAvoidsReplayCommitsAndStaysBounded(t *testing.T) {
	backend := &uowBackend{
		clusterGuard:           clusterAckGuardForTest(),
		clusterAckCertificates: make(map[string]clusterAckCertificate),
	}
	op := operation{
		ID: "11111111-2222-4333-8444-555555555555", ProjectID: testProjectID,
		KeyHash:     terminalIdempotencyKeyHash(testStableKey, "certificate-key"),
		SubjectHash: "subject", RequestHash: "request", Kind: "issue.create",
	}
	receipt := terminalSuccessOutcomeForTest(op, json.RawMessage(`{"ok":true}`))
	if backend.hasClusterAckCertificate(op, receipt) {
		t.Fatal("outcome was acknowledged before a barrier")
	}
	backend.recordClusterAckCertificate(op, receipt, "head-1")
	for i := 0; i < 10_000; i++ {
		if !backend.hasClusterAckCertificate(op, receipt) {
			t.Fatalf("certificate missed on replay %d", i)
		}
	}
	changed := *receipt
	changed.Result = json.RawMessage(`{"ok":false}`)
	if backend.hasClusterAckCertificate(op, &changed) {
		t.Fatal("changed canonical outcome reused an acknowledgement certificate")
	}
	backend.clusterGuard.ExpectedEpoch++
	if backend.hasClusterAckCertificate(op, receipt) {
		t.Fatal("role epoch change reused an acknowledgement certificate")
	}
	backend.clusterGuard = clusterAckGuardForTest()

	firstID := ""
	for i := 0; i < maxClusterAckCertificates+10; i++ {
		item := op
		item.ID = fmt.Sprintf("certificate-operation-%05d", i)
		itemReceipt := *receipt
		itemReceipt.OperationID = item.ID
		itemReceipt.Outbox.OperationID = item.ID
		backend.recordClusterAckCertificate(item, &itemReceipt, "head-2")
		if i == 0 {
			firstID = item.ID
		}
	}
	if len(backend.clusterAckCertificates) != maxClusterAckCertificates || len(backend.clusterAckOrder) != maxClusterAckCertificates {
		t.Fatalf("certificate cache maps/order=%d/%d", len(backend.clusterAckCertificates), len(backend.clusterAckOrder))
	}
	if _, exists := backend.clusterAckCertificates[firstID]; exists {
		t.Fatal("oldest acknowledgement certificate was not evicted")
	}
}

func TestClusterAckBarrierReadsWarningsFromPinnedRunner(t *testing.T) {
	op := operation{ID: "11111111-2222-4333-8444-555555555555"}
	fake := &clusterAckRawFake{warnings: &domain.RawSQLResult{}}
	if err := executeClusterAckBarrier(context.Background(), fake, op); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fake.execQuery, "DOLT_COMMIT") || strings.Contains(fake.execQuery, "-A") || len(fake.execArgs) != 1 ||
		!strings.Contains(fake.execArgs[0].(string), op.ID) || fake.queryQuery != "SHOW WARNINGS" {
		t.Fatalf("barrier exec=%q args=%#v query=%q", fake.execQuery, fake.execArgs, fake.queryQuery)
	}

	fake = &clusterAckRawFake{warnings: &domain.RawSQLResult{Rows: [][]any{
		{"Warning", int64(3024), "Timed out replication of commit to 1 out of 1 replicas."},
	}}}
	if err := executeClusterAckBarrier(context.Background(), fake, op); err == nil {
		t.Fatal("replication timeout warning was acknowledged")
	}
	fake = &clusterAckRawFake{execErr: errors.New("commit unavailable")}
	if err := executeClusterAckBarrier(context.Background(), fake, op); err == nil || fake.queryQuery != "" {
		t.Fatalf("commit error did not stop before warning lookup: err=%v query=%q", err, fake.queryQuery)
	}
}

func TestTerminalClusterAckFailsClosedForInitialAndRetryLookup(t *testing.T) {
	ctx := context.Background()
	queue, err := openTestOperationQueue(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	backend.durabilityErr = errors.New("standby has not acknowledged canonical HEAD")
	service, err := newTestGatewayService(ctx, testToken, queue, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Close()
		_ = queue.Close()
	}()

	body := `{"kind":"issue.create","payload":{"title":"[beads-perf-lab] cluster ack fence"}}`
	for attempt := 0; attempt < 2; attempt++ {
		response := submitTerminalRequest(ctx, service, testToken, "cluster-ack-terminal-key", body)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" ||
			!strings.Contains(response.Body.String(), "cluster_ack_unavailable") ||
			strings.Contains(response.Body.String(), "standby has not acknowledged") {
			t.Fatalf("attempt=%d status=%d headers=%v body=%s", attempt, response.Code, response.Header(), response.Body.String())
		}
	}
	if backend.executes.Load() != 1 || backend.durabilityChecks.Load() < 2 {
		t.Fatalf("executes=%d durability_checks=%d", backend.executes.Load(), backend.durabilityChecks.Load())
	}

	backend.mu.Lock()
	backend.durabilityErr = nil
	backend.mu.Unlock()
	recovered := submitTerminalRequest(ctx, service, testToken, "cluster-ack-terminal-key", body)
	if recovered.Code != http.StatusOK {
		t.Fatalf("recovered status=%d body=%s", recovered.Code, recovered.Body.String())
	}
	if backend.executes.Load() != 1 {
		t.Fatalf("retry re-executed canonical outcome: %d", backend.executes.Load())
	}
}
