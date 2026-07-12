package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const maxDurableMarkers = 100_001

type durableMarkers struct {
	Receipts              map[string][]byte
	Outboxes              map[string][]byte
	ReceiptOperationCount map[string]int
	OutboxOperationCount  map[string]int
	Commits               map[string]int
}

func inspectDolt(
	ctx context.Context,
	target inventoryTarget,
	labID, runID string,
	expected *expectedDatabase,
	allExpected map[string]expectedOperation,
	allQueues map[string]queueOperation,
	queue queueEvidence,
) (databaseInspection, error) {
	result := databaseInspection{
		DatabaseID: target.DatabaseID, ProjectID: target.ProjectID,
		ExpectedOperations: len(expected.Operations), ExpectedIssues: len(expected.Titles),
		ExpectedDependencies: len(expected.Edges), ExpectedEvents: len(expected.Events),
		QueueSucceededOperations: len(queue.Operations), QueueOutboxEvents: len(queue.Outbox),
	}
	password, err := readSecret(target.PasswordFile)
	if err != nil {
		return result, fmt.Errorf("password_file validation failed")
	}
	defer zero(password)
	driverConfig, err := doltSQLConfig(target, string(password))
	if err != nil {
		return result, fmt.Errorf("derive guarded project connector")
	}
	result.SQLUser = driverConfig.User
	connector, err := mysql.NewConnector(driverConfig)
	if err != nil {
		return result, fmt.Errorf("create guarded connector")
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxIdleTime(0)
	if err := db.PingContext(ctx); err != nil {
		return result, fmt.Errorf("connect to isolated database")
	}

	if err := verifyDoltIdentity(ctx, db, target, labID, &result); err != nil {
		return result, err
	}
	issues, err := readRunIssues(ctx, db, runTitlePrefix(runID), len(expected.Titles)+1)
	if err != nil {
		return result, err
	}
	result.ActualIssues = len(issues)
	if err := compareStringSet(mapKeys(issues), expected.Titles, "issue titles"); err != nil {
		return result, err
	}
	for operationID, operation := range expected.Operations {
		var marker outboxMarker
		if err := decodeStrictJSON(queue.Outbox[operationID].Payload, &marker); err != nil {
			return result, fmt.Errorf("queue outbox payload is invalid")
		}
		actualIDs := make(map[string]struct{}, len(marker.AffectedIDs))
		for _, id := range marker.AffectedIDs {
			actualIDs[id] = struct{}{}
		}
		expectedIDs := make(map[string]struct{}, len(operation.Titles))
		for _, title := range operation.Titles {
			expectedIDs[issues[title]] = struct{}{}
		}
		if err := compareStringSet(actualIDs, expectedIDs, "operation affected issue IDs"); err != nil {
			return result, err
		}
	}
	edges, err := readRunEdges(ctx, db, runTitlePrefix(runID), len(expected.Edges)+1)
	if err != nil {
		return result, err
	}
	result.ActualDependencies = len(edges)
	if err := compareStringSet(edges, expected.Edges, "dependency edges"); err != nil {
		return result, err
	}
	events, err := readRunEvents(ctx, db, runTitlePrefix(runID), len(expected.Events)+1)
	if err != nil {
		return result, err
	}
	result.ActualEvents = len(events)
	if err := compareStringSet(events, expected.Events, "issue events"); err != nil {
		return result, err
	}

	receiptKeys := make(map[string]struct{}, len(expected.Operations))
	outboxKeys := make(map[string]struct{}, len(expected.Operations))
	for operationID := range expected.Operations {
		receiptKeys[receiptPrefix+allQueues[operationID].KeyHash] = struct{}{}
		outboxKeys[outboxPrefix+operationID+"/0"] = struct{}{}
	}
	markers, err := readDurableMarkers(ctx, db, receiptKeys, outboxKeys, allExpected)
	if err != nil {
		return result, err
	}
	for operationID, operation := range allExpected {
		queueOperation, ok := allQueues[operationID]
		if !ok {
			return result, fmt.Errorf("global queue evidence is incomplete")
		}
		receiptKey := receiptPrefix + queueOperation.KeyHash
		outboxKey := outboxPrefix + operationID + "/0"
		message := commitPrefix + operationID
		if operation.Database != target.DatabaseID {
			result.ForeignReceiptCount += markers.ReceiptOperationCount[operationID]
			result.ForeignOutboxMarkerCount += markers.OutboxOperationCount[operationID]
			result.ForeignOperationCommitCount += markers.Commits[message]
			continue
		}
		receiptBody, receiptExists := markers.Receipts[receiptKey]
		outboxBody, outboxExists := markers.Outboxes[outboxKey]
		if receiptExists {
			result.ReceiptCount++
		}
		if outboxExists {
			result.OutboxMarkerCount++
		}
		result.OperationCommitCount += markers.Commits[message]
		if !receiptExists || !outboxExists || markers.ReceiptOperationCount[operationID] != 1 ||
			markers.OutboxOperationCount[operationID] != 1 || markers.Commits[message] != 1 {
			return result, fmt.Errorf("operation durable marker cardinality mismatch")
		}
		if err := validateDurableReceipt(receiptBody, outboxBody, operation, queueOperation, queue.Outbox[operationID], target.ProjectID); err != nil {
			return result, err
		}
	}
	if result.ReceiptCount != len(expected.Operations) || result.OutboxMarkerCount != len(expected.Operations) ||
		result.OperationCommitCount != len(expected.Operations) || result.ForeignReceiptCount != 0 ||
		result.ForeignOutboxMarkerCount != 0 || result.ForeignOperationCommitCount != 0 {
		return result, fmt.Errorf("durable own or cross-database operation proof mismatch")
	}
	result.Passed = result.IdentityVerified && result.AttestationVerified && result.DirtyTableCount == 0 &&
		result.ActualIssues == result.ExpectedIssues && result.ActualDependencies == result.ExpectedDependencies &&
		result.ActualEvents == result.ExpectedEvents
	return result, nil
}

func doltSQLConfig(target inventoryTarget, password string) (*mysql.Config, error) {
	user, err := labidentity.SQLUser(target.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project identity is not canonical")
	}
	return &mysql.Config{
		User: user, Passwd: password, Net: "tcp",
		Addr: net.JoinHostPort(target.Host, fmt.Sprintf("%d", target.Port)), DBName: target.DatabaseID,
		Timeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 5 * time.Second,
		ParseTime: true, AllowNativePasswords: true, TLSConfig: "false",
	}, nil
}

func verifyDoltIdentity(ctx context.Context, db *sql.DB, target inventoryTarget, labID string, result *databaseInspection) error {
	var actualDatabase, actualProject, markerJSON string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&actualDatabase); err != nil || actualDatabase != target.DatabaseID {
		return fmt.Errorf("database identity mismatch")
	}
	if err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key`='_project_id'").Scan(&actualProject); err != nil {
		return fmt.Errorf("project identity mismatch")
	}
	if err := verifyFleetProjectBinding(target.ProjectID, actualProject, result.SQLUser); err != nil {
		return fmt.Errorf("project identity mismatch")
	}
	result.IdentityVerified = true
	if err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key`=?", labAttestationKey).Scan(&markerJSON); err != nil {
		return fmt.Errorf("lab attestation is missing")
	}
	var marker struct {
		Version      int    `json:"version"`
		Environment  string `json:"environment"`
		LabID        string `json:"lab_id"`
		DatabaseName string `json:"database_name"`
		ProjectID    string `json:"project_id"`
	}
	if err := decodeStrictJSON([]byte(markerJSON), &marker); err != nil || marker.Version != 1 || marker.Environment != "synthetic" ||
		marker.LabID != labID || marker.DatabaseName != target.DatabaseID || marker.ProjectID != target.ProjectID {
		return fmt.Errorf("lab attestation identity mismatch")
	}
	result.AttestationVerified = true
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&result.DirtyTableCount); err != nil {
		return fmt.Errorf("read dirty-table status")
	}
	if result.DirtyTableCount != 0 {
		return fmt.Errorf("database has dirty tables")
	}
	return nil
}

func verifyFleetProjectBinding(expectedProjectID, actualProjectID, sqlUser string) error {
	expectedUser, err := labidentity.SQLUser(expectedProjectID)
	if err != nil || sqlUser != expectedUser || actualProjectID != expectedProjectID {
		return fmt.Errorf("project identity mismatch")
	}
	return nil
}

func readRunIssues(ctx context.Context, db *sql.DB, prefix string, limit int) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT id, title FROM issues
WHERE LEFT(title, CHAR_LENGTH(?))=?
ORDER BY title, id LIMIT ?`, prefix, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("read run issues")
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var id, title string
		if err := rows.Scan(&id, &title); err != nil {
			return nil, fmt.Errorf("scan run issue")
		}
		if _, duplicate := result[title]; duplicate {
			return nil, fmt.Errorf("duplicate run issue title")
		}
		result[title] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run issues")
	}
	if len(result) >= limit {
		return nil, fmt.Errorf("run issue query exceeded its evidence bound")
	}
	return result, nil
}

func readRunEdges(ctx context.Context, db *sql.DB, prefix string, limit int) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `
SELECT COALESCE(src.title,''), COALESCE(dst.title,''), d.type
FROM dependencies d
LEFT JOIN issues src ON src.id=d.issue_id
LEFT JOIN issues dst ON dst.id=d.depends_on_issue_id
WHERE LEFT(COALESCE(src.title,''), CHAR_LENGTH(?))=?
   OR LEFT(COALESCE(dst.title,''), CHAR_LENGTH(?))=?
ORDER BY d.issue_id, COALESCE(d.depends_on_issue_id,d.depends_on_wisp_id,d.depends_on_external), d.type LIMIT ?`, prefix, prefix, prefix, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("read run dependency edges")
	}
	defer rows.Close()
	result := map[string]struct{}{}
	for rows.Next() {
		var fromTitle, toTitle, edgeType string
		if err := rows.Scan(&fromTitle, &toTitle, &edgeType); err != nil {
			return nil, fmt.Errorf("scan run dependency edge")
		}
		key := edgeKey(fromTitle, toTitle, edgeType)
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate run dependency edge")
		}
		result[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run dependency edges")
	}
	if len(result) >= limit {
		return nil, fmt.Errorf("run dependency query exceeded its evidence bound")
	}
	return result, nil
}

func readRunEvents(ctx context.Context, db *sql.DB, prefix string, limit int) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `
SELECT i.title, e.event_type
FROM events e JOIN issues i ON i.id=e.issue_id
WHERE LEFT(i.title, CHAR_LENGTH(?))=?
ORDER BY i.title, e.event_type, e.id LIMIT ?`, prefix, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("read run events")
	}
	defer rows.Close()
	result := map[string]struct{}{}
	rowCount := 0
	for rows.Next() {
		var title, eventType string
		if err := rows.Scan(&title, &eventType); err != nil {
			return nil, fmt.Errorf("scan run event")
		}
		rowCount++
		key := eventKey(title, eventType)
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate run event")
		}
		result[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run events")
	}
	if rowCount >= limit {
		return nil, fmt.Errorf("run event query exceeded its evidence bound")
	}
	return result, nil
}

func readDurableMarkers(
	ctx context.Context,
	db *sql.DB,
	receiptKeys, outboxKeys map[string]struct{},
	allExpected map[string]expectedOperation,
) (durableMarkers, error) {
	result := durableMarkers{
		Receipts: map[string][]byte{}, Outboxes: map[string][]byte{},
		ReceiptOperationCount: map[string]int{}, OutboxOperationCount: map[string]int{},
		Commits: map[string]int{},
	}
	var retainedBytes int64
	for _, spec := range []struct {
		prefix string
		dest   map[string][]byte
		keep   map[string]struct{}
	}{
		{prefix: receiptPrefix, dest: result.Receipts, keep: receiptKeys},
		{prefix: outboxPrefix, dest: result.Outboxes, keep: outboxKeys},
	} {
		rows, err := db.QueryContext(ctx, `
SELECT `+"`key`"+`, value FROM metadata
WHERE LEFT(`+"`key`"+`, CHAR_LENGTH(?))=?
ORDER BY `+"`key`"+` LIMIT ?`, spec.prefix, spec.prefix, maxDurableMarkers)
		if err != nil {
			return durableMarkers{}, fmt.Errorf("read durable metadata markers")
		}
		count := 0
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				rows.Close()
				return durableMarkers{}, fmt.Errorf("scan durable metadata marker")
			}
			count++
			if _, keep := spec.keep[key]; keep {
				spec.dest[key] = []byte(value)
				retainedBytes += int64(len(key) + len(value))
				if retainedBytes > maxDurableRetainedBytes {
					rows.Close()
					return durableMarkers{}, fmt.Errorf("durable marker evidence exceeds its byte bound")
				}
			}
			var identity struct {
				OperationID string `json:"operation_id"`
			}
			if err := json.Unmarshal([]byte(value), &identity); err != nil || !operationIDPattern.MatchString(identity.OperationID) {
				rows.Close()
				return durableMarkers{}, fmt.Errorf("durable metadata marker is malformed")
			}
			if _, expected := allExpected[identity.OperationID]; expected {
				if spec.prefix == receiptPrefix {
					result.ReceiptOperationCount[identity.OperationID]++
				} else {
					result.OutboxOperationCount[identity.OperationID]++
				}
			}
		}
		iterationErr := rows.Err()
		rows.Close()
		if iterationErr != nil {
			return durableMarkers{}, fmt.Errorf("iterate durable metadata markers")
		}
		if count >= maxDurableMarkers {
			return durableMarkers{}, fmt.Errorf("durable metadata marker query exceeded its evidence bound")
		}
	}
	rows, err := db.QueryContext(ctx, `
SELECT message FROM dolt_log
WHERE message LIKE ? ORDER BY date, commit_hash LIMIT ?`, commitPrefix+"%", maxDurableMarkers)
	if err != nil {
		return durableMarkers{}, fmt.Errorf("read gateway operation commits")
	}
	count := 0
	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			rows.Close()
			return durableMarkers{}, fmt.Errorf("scan gateway operation commit")
		}
		count++
		operationID := strings.TrimPrefix(message, commitPrefix)
		if _, expected := allExpected[operationID]; expected {
			result.Commits[message]++
		}
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return durableMarkers{}, fmt.Errorf("iterate gateway operation commits")
	}
	if count >= maxDurableMarkers {
		return durableMarkers{}, fmt.Errorf("gateway commit query exceeded its evidence bound")
	}
	return result, nil
}

func validateDurableReceipt(
	receiptBody, markerBody []byte,
	expected expectedOperation,
	op queueOperation,
	event queueOutbox,
	projectID string,
) error {
	var value receipt
	if err := decodeStrictJSON(receiptBody, &value); err != nil {
		return fmt.Errorf("decode durable receipt")
	}
	if value.Version != 1 || value.OperationID != op.ID || value.ProjectID != projectID || value.KeyHash != op.KeyHash ||
		value.SubjectHash != op.SubjectHash || value.RequestHash != op.RequestHash || value.Kind != expected.Kind || value.CommittedAt.IsZero() {
		return fmt.Errorf("durable receipt identity mismatch")
	}
	if value.Outbox.OperationID != op.ID || value.Outbox.Sequence != 0 || value.Outbox.EventType != op.Kind+".succeeded" || value.Outbox.CreatedAt.IsZero() {
		return fmt.Errorf("durable receipt outbox identity mismatch")
	}
	canonicalResult, err := canonicalJSON(value.Result)
	if err != nil {
		return fmt.Errorf("durable receipt result is invalid")
	}
	queueResult, err := canonicalJSON(op.Result)
	if err != nil || !bytes.Equal(canonicalResult, queueResult) {
		return fmt.Errorf("Dolt and queue results differ")
	}
	canonicalOutbox, err := canonicalJSON(value.Outbox.Payload)
	if err != nil {
		return fmt.Errorf("durable receipt outbox is invalid")
	}
	queueOutbox, err := canonicalJSON(event.Payload)
	if err != nil || !bytes.Equal(canonicalOutbox, queueOutbox) {
		return fmt.Errorf("Dolt and queue outbox payloads differ")
	}
	marker, err := canonicalJSON(markerBody)
	if err != nil || !bytes.Equal(marker, queueOutbox) {
		return fmt.Errorf("Dolt outbox marker and queue outbox differ")
	}
	return nil
}

func compareStringSet(actual, expected map[string]struct{}, label string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s count=%d want=%d", label, len(actual), len(expected))
	}
	for key := range expected {
		if _, exists := actual[key]; !exists {
			return fmt.Errorf("%s are missing expected values", label)
		}
	}
	return nil
}

func mapKeys(input map[string]string) map[string]struct{} {
	result := make(map[string]struct{}, len(input))
	for key := range input {
		result[key] = struct{}{}
	}
	return result
}
