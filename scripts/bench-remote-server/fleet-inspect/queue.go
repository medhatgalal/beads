package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

const queueOperationsQueryPrefix = `
SELECT id, project_id, key_hash, subject_hash, request_hash, kind,
       payload_json, status, attempts, result_json
FROM operations WHERE id IN (`

const queueOutboxQueryPrefix = `
SELECT operation_id, sequence, event_type, payload_json
FROM outbox WHERE operation_id IN (`

func inspectQueue(ctx context.Context, target inventoryTarget, expected *expectedDatabase, runID string) (queueEvidence, error) {
	if err := validateQueueFiles(target.QueuePath); err != nil {
		return queueEvidence{}, err
	}
	u := &url.URL{Scheme: "file", Path: target.QueuePath}
	query := u.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "query_only(ON)")
	query.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return queueEvidence{}, fmt.Errorf("open queue read-only: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	var quickCheck string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&quickCheck); err != nil || quickCheck != "ok" {
		return queueEvidence{}, fmt.Errorf("queue quick_check failed")
	}
	var schemaVersion int
	var projectID, databaseName, keyID, subjectID string
	if err := db.QueryRowContext(ctx, `
SELECT schema_version, project_id, database_name, idempotency_key_id, subject_id
FROM gateway_binding WHERE singleton=1`).Scan(&schemaVersion, &projectID, &databaseName, &keyID, &subjectID); err != nil {
		return queueEvidence{}, fmt.Errorf("queue binding is missing or unreadable")
	}
	if schemaVersion != 1 || projectID != target.ProjectID || databaseName != target.DatabaseID ||
		!hexDigestPattern.MatchString(keyID) || !hexDigestPattern.MatchString(subjectID) {
		return queueEvidence{}, fmt.Errorf("queue binding identity mismatch")
	}
	var bindingRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM gateway_binding").Scan(&bindingRows); err != nil || bindingRows != 1 {
		return queueEvidence{}, fmt.Errorf("queue binding cardinality mismatch")
	}
	var foreignProjects int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE project_id<>?", target.ProjectID).Scan(&foreignProjects); err != nil || foreignProjects != 0 {
		return queueEvidence{}, fmt.Errorf("queue contains a foreign project")
	}
	var runRows int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM operations
WHERE kind IN ('issue.create','graph.apply')
  AND instr(CAST(payload_json AS TEXT), ?) > 0`, runTitlePrefix(runID)).Scan(&runRows); err != nil {
		return queueEvidence{}, fmt.Errorf("count run queue rows: %w", err)
	}
	if runRows != len(expected.Operations) {
		return queueEvidence{}, fmt.Errorf("run queue row count=%d want=%d", runRows, len(expected.Operations))
	}

	result := queueEvidence{Operations: map[string]queueOperation{}, Outbox: map[string]queueOutbox{}}
	ids := sortedOperationIDs(expected.Operations)
	if err := forStringBatches(ids, func(batch []string) error {
		query, args, err := fixedInQuery(queueOperationsQueryPrefix, ") ORDER BY id", batch)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("read expected queue operations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var op queueOperation
			if err := rows.Scan(&op.ID, &op.ProjectID, &op.KeyHash, &op.SubjectHash, &op.RequestHash,
				&op.Kind, &op.Payload, &op.Status, &op.Attempts, &op.Result); err != nil {
				return fmt.Errorf("scan queue operation: %w", err)
			}
			if _, duplicate := result.Operations[op.ID]; duplicate {
				return fmt.Errorf("queue repeats operation %q", op.ID)
			}
			result.Bytes += int64(len(op.Payload) + len(op.Result))
			if result.Bytes > maxQueueEvidenceBytes {
				return fmt.Errorf("queue evidence exceeds its byte bound")
			}
			result.Operations[op.ID] = op
		}
		return rows.Err()
	}); err != nil {
		return queueEvidence{}, err
	}
	if err := forStringBatches(ids, func(batch []string) error {
		query, args, err := fixedInQuery(queueOutboxQueryPrefix, ") ORDER BY operation_id, sequence", batch)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("read expected queue outbox: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var event queueOutbox
			if err := rows.Scan(&event.OperationID, &event.Sequence, &event.EventType, &event.Payload); err != nil {
				return fmt.Errorf("scan queue outbox: %w", err)
			}
			if _, duplicate := result.Outbox[event.OperationID]; duplicate {
				return fmt.Errorf("queue repeats outbox event for %q", event.OperationID)
			}
			result.Bytes += int64(len(event.Payload))
			if result.Bytes > maxQueueEvidenceBytes {
				return fmt.Errorf("queue evidence exceeds its byte bound")
			}
			result.Outbox[event.OperationID] = event
		}
		return rows.Err()
	}); err != nil {
		return queueEvidence{}, err
	}

	if len(result.Operations) != len(expected.Operations) || len(result.Outbox) != len(expected.Operations) {
		return queueEvidence{}, fmt.Errorf("queue expected operation or outbox rows are missing")
	}
	for id, expectedOp := range expected.Operations {
		op := result.Operations[id]
		event := result.Outbox[id]
		if err := validateQueueOperation(target.ProjectID, expectedOp, op, event); err != nil {
			return queueEvidence{}, fmt.Errorf("queue operation %s: %w", id, err)
		}
	}
	return result, nil
}

func validateQueueOperation(projectID string, expected expectedOperation, op queueOperation, event queueOutbox) error {
	if op.ID != expected.ID || op.ProjectID != projectID || op.Kind != expected.Kind || op.Status != "succeeded" || op.Attempts < 1 || op.Attempts > 3 {
		return fmt.Errorf("terminal identity, kind, status, or attempts mismatch")
	}
	if !hexDigestPattern.MatchString(op.KeyHash) || !hexDigestPattern.MatchString(op.SubjectHash) || !hexDigestPattern.MatchString(op.RequestHash) {
		return fmt.Errorf("queue hashes are malformed")
	}
	canonical, err := canonicalJSON(op.Payload)
	if err != nil || !bytes.Equal(canonical, op.Payload) {
		return fmt.Errorf("queue payload is not canonical JSON")
	}
	wantRequestHash := digestHex(bytes.Join([][]byte{[]byte(projectID), []byte(op.Kind), canonical}, []byte{0}))
	if op.RequestHash != wantRequestHash {
		return fmt.Errorf("request_hash mismatch")
	}
	if err := validateOperationPayload(expected, canonical); err != nil {
		return err
	}
	if !json.Valid(op.Result) || len(op.Result) == 0 {
		return fmt.Errorf("queue result_json is missing or invalid")
	}
	if event.OperationID != op.ID || event.Sequence != 0 || event.EventType != op.Kind+".succeeded" || !json.Valid(event.Payload) {
		return fmt.Errorf("queue outbox identity mismatch")
	}
	var marker outboxMarker
	if err := decodeStrictJSON(event.Payload, &marker); err != nil {
		return fmt.Errorf("queue outbox payload: %w", err)
	}
	if marker.OperationID != op.ID || marker.Kind != op.Kind || marker.ResultHash != digestHex(op.Result) || len(marker.AffectedIDs) != len(expected.Titles) {
		return fmt.Errorf("queue outbox payload mismatch")
	}
	seen := map[string]struct{}{}
	for _, id := range marker.AffectedIDs {
		if id == "" {
			return fmt.Errorf("queue outbox has an empty affected ID")
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("queue outbox repeats an affected ID")
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateOperationPayload(expected expectedOperation, body []byte) error {
	switch expected.Kind {
	case "issue.create":
		var payload struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			Type        string `json:"type"`
			Priority    int    `json:"priority"`
		}
		if err := decodeStrictJSON(body, &payload); err != nil {
			return fmt.Errorf("point payload: %w", err)
		}
		if len(expected.Titles) != 1 || payload.Title != expected.Titles[0] || payload.Description != "isolated realistic fleet synthetic point mutation" || payload.Type != "task" || payload.Priority != 2 {
			return fmt.Errorf("point payload does not match the frozen workload")
		}
	case "graph.apply":
		var payload struct {
			Nodes []struct {
				Key      string `json:"key"`
				Title    string `json:"title"`
				Type     string `json:"type"`
				Priority int    `json:"priority"`
			} `json:"nodes"`
			Edges []struct {
				FromKey string `json:"from_key"`
				ToKey   string `json:"to_key"`
				Type    string `json:"type"`
			} `json:"edges"`
		}
		if err := decodeStrictJSON(body, &payload); err != nil {
			return fmt.Errorf("graph payload: %w", err)
		}
		if len(payload.Nodes) != 100 || len(payload.Edges) != 200 || len(expected.Titles) != 100 || len(expected.Edges) != 200 {
			return fmt.Errorf("graph payload cardinality mismatch")
		}
		for index, node := range payload.Nodes {
			if node.Key != fmt.Sprintf("n%03d", index) || node.Title != expected.Titles[index] || node.Type != "task" || node.Priority != 2 {
				return fmt.Errorf("graph node %d mismatch", index)
			}
		}
		for index, edge := range payload.Edges {
			fromIndex := titleIndex(expected.Titles, expected.Edges[index].FromTitle)
			toIndex := titleIndex(expected.Titles, expected.Edges[index].ToTitle)
			if edge.FromKey != fmt.Sprintf("n%03d", fromIndex) || edge.ToKey != fmt.Sprintf("n%03d", toIndex) || edge.Type != "blocks" {
				return fmt.Errorf("graph edge %d mismatch", index)
			}
		}
	default:
		return fmt.Errorf("unsupported expected operation kind")
	}
	return nil
}

func titleIndex(titles []string, title string) int {
	for index, candidate := range titles {
		if candidate == title {
			return index
		}
	}
	return -1
}

func canonicalJSON(body []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func decodeStrictJSON(body []byte, value any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return err
	}
	return requireJSONEOF(dec)
}

func digestHex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func fixedInQuery(prefix, suffix string, values []string) (string, []any, error) {
	if len(values) == 0 || len(values) > maxBatchSize {
		return "", nil, fmt.Errorf("fixed query batch must contain 1-%d values", maxBatchSize)
	}
	args := make([]any, len(values))
	for index, value := range values {
		args[index] = value
	}
	return prefix + strings.TrimSuffix(strings.Repeat("?,", len(values)), ",") + suffix, args, nil
}

func forStringBatches(values []string, fn func([]string) error) error {
	for start := 0; start < len(values); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(values) {
			end = len(values)
		}
		if err := fn(values[start:end]); err != nil {
			return err
		}
	}
	return nil
}
