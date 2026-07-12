package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testLabID      = "dc6020ef-b433-41b6-9426-cedd9bf40502"
	testProjectA   = "093828a8-233f-4fc9-a8b7-5b5f77a48c0b"
	testProjectB   = "80d22c54-066c-4f89-b6e8-d94e19b4e902"
	testOperationA = "11111111-1111-4111-8111-111111111111"
	testOperationB = "22222222-2222-8222-8222-222222222222"
)

func TestValidateInventoryFailClosedBoundaries(t *testing.T) {
	base := validInventory()
	if err := validateInventory(base); err != nil {
		t.Fatalf("valid inventory: %v", err)
	}
	tests := []struct {
		name string
		edit func(*inventory)
		want string
	}{
		{name: "production SQL port", edit: func(v *inventory) { v.Targets[0].Port = 3306 }, want: "3306 and 3307"},
		{name: "production Dolt port", edit: func(v *inventory) { v.Targets[0].Port = 3307 }, want: "3306 and 3307"},
		{name: "hostname", edit: func(v *inventory) { v.Targets[0].Host = "localhost" }, want: "numeric loopback"},
		{name: "non-loopback", edit: func(v *inventory) { v.Targets[0].Host = "192.0.2.10" }, want: "numeric loopback"},
		{name: "database", edit: func(v *inventory) { v.Targets[0].DatabaseID = "production" }, want: "beads_perf_lab"},
		{name: "relative secret", edit: func(v *inventory) { v.Targets[0].PasswordFile = "password" }, want: "must be absolute"},
		{name: "duplicate project", edit: func(v *inventory) { v.Targets[1].ProjectID = v.Targets[0].ProjectID }, want: "duplicate project"},
		{name: "duplicate queue", edit: func(v *inventory) { v.Targets[1].QueuePath = v.Targets[0].QueuePath }, want: "duplicate queue"},
		{name: "noncanonical project", edit: func(v *inventory) { v.Targets[0].ProjectID = strings.ToUpper(v.Targets[0].ProjectID) }, want: "canonical UUID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Targets = append([]inventoryTarget(nil), base.Targets...)
			test.edit(&value)
			err := validateInventory(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateInventory() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestDoltSQLConfigBindsExactPerProjectPrincipal(t *testing.T) {
	firstTarget := validInventory().Targets[0]
	first, err := doltSQLConfig(firstTarget, "secret")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.ReplaceAll(firstTarget.ProjectID, "-", "")
	if first.User != want || len(first.User) != 32 {
		t.Fatalf("fleet inspector SQL user = %q, want %q", first.User, want)
	}

	secondTarget := validInventory().Targets[1]
	second, err := doltSQLConfig(secondTarget, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if second.User == first.User {
		t.Fatal("cross-project fleet targets shared a SQL principal")
	}
}

func TestFleetProjectBindingRejectsCrossProjectDatabaseAndUser(t *testing.T) {
	target := validInventory().Targets[0]
	user := strings.ReplaceAll(target.ProjectID, "-", "")
	if err := verifyFleetProjectBinding(target.ProjectID, target.ProjectID, user); err != nil {
		t.Fatalf("exact binding rejected: %v", err)
	}
	other := validInventory().Targets[1].ProjectID
	if err := verifyFleetProjectBinding(target.ProjectID, other, user); err == nil {
		t.Fatal("cross-project database marker was accepted")
	}
	if err := verifyFleetProjectBinding(target.ProjectID, target.ProjectID, strings.ReplaceAll(other, "-", "")); err == nil {
		t.Fatal("cross-project SQL principal was accepted")
	}
}

func TestPrivateFileAndSymlinkGuards(t *testing.T) {
	directory, err := os.MkdirTemp("/private/tmp", "fleet-inspect-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	private := filepath.Join(directory, "private.json")
	if err := os.WriteFile(private, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readGuardedFile(private, 100, true); err != nil {
		t.Fatalf("read private file: %v", err)
	}
	if err := os.Chmod(private, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readGuardedFile(private, 100, true); err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Fatalf("permissive file error = %v", err)
	}
	if err := os.Chmod(private, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readGuardedFile(link, 100, true); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestDeriveExpectedCompletePointGraphAndRead(t *testing.T) {
	inv := validInventory()
	report := validRunnerReport()
	got, err := deriveExpected(inv, report)
	if err != nil {
		t.Fatalf("deriveExpected: %v", err)
	}
	if len(got.Operations) != 2 || got.IssueCount != 101 || got.EdgeCount != 200 || got.EventCount != 101 {
		t.Fatalf("derived totals = operations:%d issues:%d edges:%d events:%d", len(got.Operations), got.IssueCount, got.EdgeCount, got.EventCount)
	}
	if len(got.ByDatabase[inv.Targets[0].DatabaseID].Titles) != 1 || len(got.ByDatabase[inv.Targets[1].DatabaseID].Titles) != 100 {
		t.Fatalf("per-database title counts were not exact")
	}
	graph := got.Operations[testOperationB]
	if graph.Kind != "graph.apply" || len(graph.Edges) != 200 || graph.Edges[0].Type != "blocks" {
		t.Fatalf("derived graph = %#v", graph)
	}
}

func TestDeriveExpectedRejectsIncompleteEvidence(t *testing.T) {
	tests := []struct {
		name string
		edit func(*runnerReport)
		want string
	}{
		{name: "sample truncation", edit: func(v *runnerReport) { v.BoundedEvidence.SampleRecords.Truncated = true }, want: "sample evidence"},
		{name: "result truncation", edit: func(v *runnerReport) { v.BoundedEvidence.ResultLatency.Truncated = true }, want: "result evidence"},
		{name: "operation truncation", edit: func(v *runnerReport) { v.BoundedEvidence.OperationIDProof.Truncated = true }, want: "operation-ID evidence"},
		{name: "missing operation id", edit: func(v *runnerReport) { v.Samples[1].OperationID = "" }, want: "operation_id"},
		{name: "missing sample", edit: func(v *runnerReport) { v.Samples = v.Samples[:2] }, want: "samples are incomplete"},
		{name: "counter failure", edit: func(v *runnerReport) { v.Correctness.MissingOperationIDs = 1 }, want: "correctness counters"},
		{name: "dropped", edit: func(v *runnerReport) { v.Metrics.Dropped = 1 }, want: "outcome accounting"},
		{name: "wrong mode", edit: func(v *runnerReport) { v.Mode = "open-loop" }, want: "mode must be closed-loop"},
		{name: "unknown profile", edit: func(v *runnerReport) { v.Profile = "smoke" }, want: "profile must be"},
		{name: "zero workload", edit: func(v *runnerReport) {
			v.Metrics.Generated, v.Metrics.Completed, v.Metrics.Passed = 0, 0, 0
			v.Metrics.ReadCommands, v.Metrics.PointWrites, v.Metrics.Graphs = 0, 0, 0
			v.Metrics.ByDatabase = map[string]runnerMetricGroup{}
			v.BoundedEvidence.ResultLatency = boundedStoreEvidence{}
			v.BoundedEvidence.SampleRecords = boundedStoreEvidence{}
			v.BoundedEvidence.OperationIDProof = boundedStoreEvidence{}
			v.Samples = nil
		}, want: "required mixed workload"},
		{name: "no graph", edit: func(v *runnerReport) {
			v.Metrics.PointWrites++
			v.Metrics.Graphs--
			v.Samples[1].Kind = "point_write"
			v.Samples[1].Command = "create"
		}, want: "required mixed workload"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validRunnerReport()
			value.Samples = append([]runnerSample(nil), value.Samples...)
			test.edit(&value)
			_, err := deriveExpected(validInventory(), value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("deriveExpected() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateQueueOperationChecksCanonicalPayloadAndHashes(t *testing.T) {
	sample := validRunnerReport().Samples[0]
	expected := buildExpectedOperation("realistic-test", sample)
	payload, err := json.Marshal(map[string]any{
		"title": expected.Titles[0], "description": "isolated realistic fleet synthetic point mutation",
		"type": "task", "priority": 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := []byte(`{"id":"bd-test"}`)
	op := queueOperation{
		ID: expected.ID, ProjectID: testProjectA, KeyHash: strings.Repeat("a", 64),
		SubjectHash: strings.Repeat("b", 64), Kind: expected.Kind, Payload: payload,
		Status: "succeeded", Attempts: 1, Result: result,
	}
	op.RequestHash = digestHex(bytes.Join([][]byte{[]byte(testProjectA), []byte(op.Kind), payload}, []byte{0}))
	marker, err := json.Marshal(map[string]any{
		"operation_id": op.ID, "kind": op.Kind, "affected_ids": []string{"bd-test"}, "result_hash": digestHex(result),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := queueOutbox{OperationID: op.ID, Sequence: 0, EventType: op.Kind + ".succeeded", Payload: marker}
	if err := validateQueueOperation(testProjectA, expected, op, event); err != nil {
		t.Fatalf("valid queue operation: %v", err)
	}
	op.RequestHash = strings.Repeat("c", 64)
	if err := validateQueueOperation(testProjectA, expected, op, event); err == nil || !strings.Contains(err.Error(), "request_hash") {
		t.Fatalf("request hash mismatch error = %v", err)
	}
}

func TestFixedInQueryAndBatchBounds(t *testing.T) {
	query, args, err := fixedInQuery("SELECT x WHERE id IN (", ")", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if query != "SELECT x WHERE id IN (?,?,?)" || len(args) != 3 {
		t.Fatalf("fixed query = %q args=%d", query, len(args))
	}
	if _, _, err := fixedInQuery("", "", nil); err == nil {
		t.Fatal("empty fixed query batch was accepted")
	}
	tooMany := make([]string, maxBatchSize+1)
	if _, _, err := fixedInQuery("", "", tooMany); err == nil {
		t.Fatal("oversized fixed query batch was accepted")
	}
	values := make([]string, maxBatchSize*2+1)
	var sizes []int
	if err := forStringBatches(values, func(batch []string) error {
		sizes = append(sizes, len(batch))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := []int{maxBatchSize, maxBatchSize, 1}; len(sizes) != len(got) || sizes[0] != got[0] || sizes[1] != got[1] || sizes[2] != got[2] {
		t.Fatalf("batch sizes = %v", sizes)
	}
}

func TestValidateDurableReceiptMatchesQueueHashesAndPayloads(t *testing.T) {
	expected := expectedOperation{ID: testOperationA, Kind: "issue.create", Titles: []string{"[beads-perf-lab] realistic realistic-test point"}}
	result := []byte(`{"id":"bd-test"}`)
	markerBody := []byte(`{"affected_ids":["bd-test"],"kind":"issue.create","operation_id":"` + testOperationA + `","result_hash":"` + digestHex(result) + `"}`)
	op := queueOperation{
		ID: testOperationA, ProjectID: testProjectA, KeyHash: strings.Repeat("a", 64),
		SubjectHash: strings.Repeat("b", 64), RequestHash: strings.Repeat("c", 64),
		Kind: "issue.create", Result: result,
	}
	event := queueOutbox{OperationID: op.ID, Sequence: 0, EventType: op.Kind + ".succeeded", Payload: markerBody}
	var value receipt
	value.Version = 1
	value.OperationID = op.ID
	value.ProjectID = op.ProjectID
	value.KeyHash = op.KeyHash
	value.SubjectHash = op.SubjectHash
	value.RequestHash = op.RequestHash
	value.Kind = op.Kind
	value.Result = result
	value.CommittedAt = time.Unix(1, 0).UTC()
	value.Outbox.OperationID = op.ID
	value.Outbox.Sequence = 0
	value.Outbox.EventType = op.Kind + ".succeeded"
	value.Outbox.Payload = markerBody
	value.Outbox.CreatedAt = time.Unix(1, 0).UTC()
	receiptBody, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDurableReceipt(receiptBody, markerBody, expected, op, event, testProjectA); err != nil {
		t.Fatalf("valid durable receipt: %v", err)
	}
	value.KeyHash = strings.Repeat("d", 64)
	receiptBody, err = json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDurableReceipt(receiptBody, markerBody, expected, op, event, testProjectA); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("identity mismatch error = %v", err)
	}
}

func validInventory() inventory {
	return inventory{
		SchemaVersion: 1, LabID: testLabID,
		Targets: []inventoryTarget{
			{Host: "127.0.0.1", Port: 13360, DatabaseID: "beads_perf_lab_a", ProjectID: testProjectA, PasswordFile: "/private/tmp/password-a", QueuePath: "/private/tmp/queue-a.db"},
			{Host: "127.0.0.1", Port: 13360, DatabaseID: "beads_perf_lab_b", ProjectID: testProjectB, PasswordFile: "/private/tmp/password-b", QueuePath: "/private/tmp/queue-b.db"},
		},
	}
}

func validRunnerReport() runnerReport {
	var value runnerReport
	value.SchemaVersion = reportSchemaVersion
	value.Mode = "closed-loop"
	value.Profile = "busy"
	value.Config.RunID = "realistic-test"
	value.Config.LabID = testLabID
	value.Config.RegisteredDatabases = 2
	value.Config.ActiveDatabases = 2
	value.Config.ActiveDatabaseIDs = []string{"beads_perf_lab_a", "beads_perf_lab_b"}
	value.Attestation.Required = 2
	value.Attestation.Verified = 2
	value.Metrics.Generated = 3
	value.Metrics.Completed = 3
	value.Metrics.AccountingBalanced = true
	value.Metrics.Passed = 3
	value.Metrics.ReadCommands = 1
	value.Metrics.PointWrites = 1
	value.Metrics.Graphs = 1
	value.Metrics.ByDatabase = map[string]runnerMetricGroup{
		"beads_perf_lab_a": {Commands: 2, Passed: 2},
		"beads_perf_lab_b": {Commands: 1, Passed: 1},
	}
	value.BoundedEvidence.ResultLatency = boundedStoreEvidence{Seen: 3, Retained: 3, Limit: 10}
	value.BoundedEvidence.SampleRecords = boundedStoreEvidence{Seen: 3, Retained: 3, Limit: 10}
	value.BoundedEvidence.OperationIDProof = boundedStoreEvidence{Seen: 2, Retained: 2, Limit: 10}
	value.Samples = []runnerSample{
		{Identity: "actor-000001-cycle-000000001-command-000", Ordinal: 0, Kind: "point_write", Command: "create", DatabaseID: "beads_perf_lab_a", OperationID: testOperationA, HTTPStatus: 200, Passed: true},
		{Identity: "graph-000000001", Ordinal: 1, Kind: "graph", Command: "graph", DatabaseID: "beads_perf_lab_b", OperationID: testOperationB, HTTPStatus: 200, Passed: true},
		// Ordinal gaps are valid when a boundary-cancelled job was assigned an
		// ordinal but removed from generated accounting before admission.
		{Identity: "actor-000002-cycle-000000001-command-000", Ordinal: 9, Kind: "read", Command: "show", DatabaseID: "beads_perf_lab_a", HTTPStatus: 200, Passed: true},
	}
	return value
}
