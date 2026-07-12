package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const maxConcurrentDatabaseInspections = 8

func inspectFleet(
	ctx context.Context,
	inv inventory,
	runner runnerReport,
	inventoryBody, runnerBody []byte,
) (inspectionReport, error) {
	expected, err := deriveExpected(inv, runner)
	if err != nil {
		return inspectionReport{}, err
	}
	result := inspectionReport{
		SchemaVersion: inspectionSchemaVersion, InspectedAt: time.Now().UTC(),
		RunID: expected.RunID, LabID: expected.LabID,
		RunnerReportSHA256: digestHex(runnerBody), InventorySHA256: digestHex(inventoryBody),
		ReportEvidenceExact: true, ExpectedOperations: len(expected.Operations),
		ExpectedIssues: expected.IssueCount, ExpectedDependencies: expected.EdgeCount,
		ExpectedEvents: expected.EventCount,
		Notes: []string{
			"Inspection used only fixed read-only SQL with bounded result sets; no SQL text is accepted from input.",
			"Every runner result sample and write operation ID was required to be complete and untruncated.",
			"Each synthetic database was checked independently, including absence of every other database's operation markers.",
			"SQLite queues must be quiesced and checkpointed before this point-in-time inspection.",
		},
	}

	for _, target := range inv.Targets {
		if err := validateRegularPath(target.PasswordFile, true); err != nil {
			return inspectionReport{}, fmt.Errorf("target %s password_file rejected: %w", target.DatabaseID, err)
		}
		if err := validateQueueFiles(target.QueuePath); err != nil {
			return inspectionReport{}, fmt.Errorf("target %s queue_path rejected: %w", target.DatabaseID, err)
		}
	}

	queueByDatabase := make(map[string]queueEvidence, len(inv.Targets))
	allQueueOperations := make(map[string]queueOperation, len(expected.Operations))
	var queueEvidenceBytes int64
	targetByID := inventoryByDatabase(inv)
	for _, databaseID := range sortedTargetIDs(inv) {
		target := targetByID[databaseID]
		queue, err := inspectQueue(ctx, target, expected.ByDatabase[databaseID], expected.RunID)
		if err != nil {
			return inspectionReport{}, fmt.Errorf("queue evidence for %s: %w", databaseID, err)
		}
		queueByDatabase[databaseID] = queue
		queueEvidenceBytes += queue.Bytes
		if queueEvidenceBytes > maxQueueEvidenceBytes {
			return inspectionReport{}, fmt.Errorf("fleet queue evidence exceeds its byte bound")
		}
		for operationID, operation := range queue.Operations {
			if _, duplicate := allQueueOperations[operationID]; duplicate {
				return inspectionReport{}, fmt.Errorf("operation %s appears in multiple queues", operationID)
			}
			allQueueOperations[operationID] = operation
		}
	}
	if len(allQueueOperations) != len(expected.Operations) {
		return inspectionReport{}, fmt.Errorf("fleet queue evidence is incomplete")
	}

	ids := sortedTargetIDs(inv)
	inspections := make(map[string]databaseInspection, len(ids))
	var mu sync.Mutex
	jobs := make(chan string)
	var workers sync.WaitGroup
	workerCount := maxConcurrentDatabaseInspections
	if workerCount > len(ids) {
		workerCount = len(ids)
	}
	for index := 0; index < workerCount; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for databaseID := range jobs {
				target := targetByID[databaseID]
				dbCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				inspection, inspectErr := inspectDolt(
					dbCtx, target, expected.LabID, expected.RunID, expected.ByDatabase[databaseID],
					expected.Operations, allQueueOperations, queueByDatabase[databaseID],
				)
				cancel()
				if inspectErr != nil {
					inspection.Passed = false
					inspection.ErrorClass = safeInspectionClass(inspectErr)
				}
				mu.Lock()
				inspections[databaseID] = inspection
				mu.Unlock()
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, databaseID := range ids {
			select {
			case jobs <- databaseID:
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	if len(inspections) != len(ids) {
		return inspectionReport{}, fmt.Errorf("inspection context ended before every database was inspected")
	}
	result.Passed = true
	for _, databaseID := range ids {
		inspection := inspections[databaseID]
		result.Databases = append(result.Databases, inspection)
		if !inspection.Passed {
			result.Passed = false
		}
	}
	return result, nil
}

func safeInspectionClass(err error) string {
	message := strings.ToLower(err.Error())
	classes := []struct {
		needle string
		class  string
	}{
		{"password", "secret_validation_failed"},
		{"connect", "database_connection_failed"},
		{"attestation", "attestation_mismatch"},
		{"identity", "database_identity_mismatch"},
		{"dirty", "dirty_database"},
		{"issue", "issue_effect_mismatch"},
		{"dependency", "dependency_effect_mismatch"},
		{"event", "event_effect_mismatch"},
		{"receipt", "receipt_mismatch"},
		{"outbox", "outbox_mismatch"},
		{"commit", "commit_mismatch"},
		{"queue", "queue_mismatch"},
		{"durable", "durable_marker_mismatch"},
	}
	for _, candidate := range classes {
		if strings.Contains(message, candidate.needle) {
			return candidate.class
		}
	}
	return "inspection_failed"
}
