package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func ringAt(active uint64) (*verificationRing, error) {
	const maxKeys = 3
	ring := newVerificationRing(maxKeys)
	for epoch := uint64(1); epoch <= active; epoch++ {
		if err := ring.rotate(epoch); err != nil {
			return nil, err
		}
	}
	return ring, nil
}

func runProof(ctx context.Context, runID string) (proofReport, error) {
	started := time.Now().UTC()
	report := proofReport{
		Schema: "beads.idempotency_retention_dolt_proof.v2", StartedAt: started,
		Database: labDatabase, Host: labHost, Port: labPort, RunID: runID,
		Scenarios:         make(map[string]scenarioEvidence),
		PromotionDecision: "HOLD_PENDING_INDEPENDENT_INSPECTION",
	}
	mysqlVersion, doltVersion, setupHead, err := setupLab(ctx)
	if err != nil {
		return finishProof(report, started), err
	}
	report.MySQLCompatVersion = mysqlVersion
	report.DoltVersion = doltVersion
	report.SetupHead = setupHead
	db, err := openDB(labDatabase, 12)
	if err != nil {
		return finishProof(report, started), err
	}
	defer db.Close()
	if err := initializeRun(ctx, db, runID); err != nil {
		return finishProof(report, started), err
	}
	runtimeDir, err := createRuntimeDir(runID)
	if err != nil {
		return finishProof(report, started), err
	}
	primaryRing, err := ringAt(1)
	if err != nil {
		return finishProof(report, started), err
	}
	if err := proveConcurrentProducerCapacity(ctx, db, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}

	if err := proveConcurrentSameKey(ctx, db, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}
	if err := proveOrderingAndProducerFence(ctx, db, primaryRing, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}
	if err := proveRollback(ctx, db, primaryRing, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}
	if err := proveCompactionAndKeys(ctx, db, primaryRing, report.Scenarios, runID, runtimeDir); err != nil {
		return finishProof(report, started), err
	}
	if err := proveKillBoundaries(ctx, db, report.Scenarios, runID, runtimeDir); err != nil {
		return finishProof(report, started), err
	}
	if err := proveStoredOutcomeLimit(ctx, db, primaryRing, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}
	if err := proveConcurrentReceiptCapacity(ctx, db, report.Scenarios, runID); err != nil {
		return finishProof(report, started), err
	}
	report.EpochOneHead, err = currentHead(ctx, db)
	if err != nil {
		return finishProof(report, started), err
	}
	if err := proveRepositoryRestoreFence(ctx, db, primaryRing, report.Scenarios, runID, report.EpochOneHead, &report); err != nil {
		return finishProof(report, started), err
	}
	report.FinalInspection, err = inspectRun(ctx, db, runID, 2)
	if err != nil {
		return finishProof(report, started), err
	}
	report.Passed = report.FinalInspection.Passed && report.FinalInspection.ProducerRows == 1 &&
		report.FinalInspection.ReceiptRows == 1 && report.FinalInspection.BusinessRows == 12 &&
		len(report.FinalInspection.OperationBusinessRows) == 12 && report.FinalInspection.StoredProducerCount == 1 &&
		report.FinalInspection.StoredReceiptCount == 1
	for _, scenario := range report.Scenarios {
		report.Passed = report.Passed && scenario.Passed
	}
	if report.Passed {
		report.PromotionDecision = "PROMOTE_TO_DISPOSABLE_GCP_MULTI_NODE_PROOF_NOT_PRODUCTION"
	}
	report.RemainingGaps = []string{
		"The server is single-node loopback Dolt; leader loss, standby promotion, and network partitions remain unproven.",
		"Worker processes were killed at real transaction boundaries, but the Dolt server process and disk were not killed.",
		"The stale writable branch is a controlled restore analog; actual snapshot restore plus external catalog fencing remains unproven.",
		"The proof uses fixed rows, not the 24-hour retry-window, segment-size, Dolt-history, and GC plateau workloads.",
		"The lab uses passwordless root only on loopback; production least-privilege and KMS availability remain separate gates.",
	}
	return finishProof(report, started), nil
}

func proveConcurrentProducerCapacity(ctx context.Context, db *sql.DB, scenarios map[string]scenarioEvidence, runID string) error {
	before, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	results, err := concurrentRegistrationWorkers(ctx, []registrationEnvelope{
		{RunID: runID, ProducerID: "p-cap-a", ProducerEpoch: 1, RepositoryEpoch: 1},
		{RunID: runID, ProducerID: "p-cap-b", ProducerEpoch: 1, RepositoryEpoch: 1},
	})
	if err != nil {
		return err
	}
	after, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	executed, capacity := 0, 0
	for _, result := range results {
		if result.Disposition == dispositionExecuted {
			executed++
		}
		if result.Disposition == dispositionCapacity && result.Reason == "producer_capacity_exhausted" {
			capacity++
		}
	}
	stateA, err := inspectProducer(ctx, db, runID, "p-cap-a")
	if err != nil {
		return err
	}
	stateB, err := inspectProducer(ctx, db, runID, "p-cap-b")
	if err != nil {
		return err
	}
	var commits int64
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM dolt_log WHERE message LIKE ?`, runCommit(runID, "register p-cap-%")).Scan(&commits); err != nil {
		return err
	}
	passed := before.consistent() && after.consistent() && before.StoredProducers == int64(len(initialProofProducers)) &&
		after.StoredProducers == proofProducerRows && after.StoredProducers == before.StoredProducers+1 &&
		executed == 1 && capacity == 1 && stateA.Exists != stateB.Exists && commits == 1
	scenarios["two_process_producer_hard_cap"] = scenarioEvidence{Passed: passed, Registrations: results,
		Notes: []string{fmt.Sprintf("before=%d after=%d executed=%d capacity=%d registration_commits=%d", before.StoredProducers, after.StoredProducers, executed, capacity, commits)}}
	if !passed {
		return fmt.Errorf("two-process producer hard-cap proof failed")
	}
	return nil
}

func proveConcurrentSameKey(ctx context.Context, db *sql.DB, scenarios map[string]scenarioEvidence, runID string) error {
	envelope := workerEnvelope{RunID: runID, ProducerID: "p-same", ProducerEpoch: 1, Sequence: 1, RepositoryEpoch: 1, Payload: operationPrefix + " concurrent same key"}
	results, err := concurrentWorkers(ctx, envelope)
	if err != nil {
		return err
	}
	ring, _ := ringAt(1)
	in, _ := newRequest(runID, envelope.ProducerID, 1, 1, 1, envelope.Payload, ring)
	business, receipts, commits, err := operationCounts(ctx, db, runID, in.OperationID)
	if err != nil {
		return err
	}
	state, err := inspectProducer(ctx, db, runID, envelope.ProducerID)
	if err != nil {
		return err
	}
	executed, replayed := 0, 0
	for _, result := range results {
		if result.Disposition == dispositionExecuted {
			executed++
		}
		if result.Disposition == dispositionReplay {
			replayed++
		}
	}
	passed := executed == 1 && replayed == 1 && business == 1 && receipts == 1 && commits == 1 &&
		state.NextSequence == 2 && state.ReceiptRows == 1 && state.BusinessRows == 1
	scenarios["two_process_same_key"] = scenarioEvidence{Passed: passed, Results: results,
		Notes: []string{fmt.Sprintf("executed=%d replayed=%d business=%d receipts=%d commits=%d", executed, replayed, business, receipts, commits)}}
	if !passed {
		return fmt.Errorf("two-process same-key proof failed")
	}
	return nil
}

func proveStoredOutcomeLimit(ctx context.Context, db *sql.DB, ring *verificationRing, scenarios map[string]scenarioEvidence, runID string) error {
	before, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	payload := operationPrefix + strings.Repeat("x", proofOutcomeBytes+1-len(operationPrefix))
	in, err := newRequest(runID, "p-cap-r1", 1, 1, 1, payload, ring)
	if err != nil {
		return err
	}
	result, err := executeOperation(ctx, db, ring, in, "", "")
	if err != nil {
		return err
	}
	after, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	business, receipts, commits, err := operationCounts(ctx, db, runID, in.OperationID)
	if err != nil {
		return err
	}
	state, err := inspectProducer(ctx, db, runID, "p-cap-r1")
	if err != nil {
		return err
	}
	passed := len(payload) == proofOutcomeBytes+1 && before.consistent() && after.consistent() &&
		before.StoredReceipts == after.StoredReceipts && before.Version == after.Version &&
		result.Disposition == dispositionCapacity && result.Reason == "outcome_capacity_exhausted" &&
		business == 0 && receipts == 0 && commits == 0 && state.NextSequence == 1
	scenarios["stored_outcome_hard_cap"] = scenarioEvidence{Passed: passed, Results: []operationResult{result},
		Notes: []string{fmt.Sprintf("stored_limit=%d attempted_bytes=%d receipt_counter=%d", proofOutcomeBytes, len(payload), after.StoredReceipts)}}
	if !passed {
		return fmt.Errorf("stored outcome hard-cap proof failed")
	}
	return nil
}

func proveConcurrentReceiptCapacity(ctx context.Context, db *sql.DB, scenarios map[string]scenarioEvidence, runID string) error {
	before, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	envelopes := []workerEnvelope{
		{RunID: runID, ProducerID: "p-cap-r1", ProducerEpoch: 1, Sequence: 1, RepositoryEpoch: 1, Payload: operationPrefix + " receipt cap one"},
		{RunID: runID, ProducerID: "p-cap-r2", ProducerEpoch: 1, Sequence: 1, RepositoryEpoch: 1, Payload: operationPrefix + " receipt cap two"},
	}
	results, err := concurrentWorkerEnvelopes(ctx, envelopes)
	if err != nil {
		return err
	}
	after, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	executed, capacity := 0, 0
	var business, receipts, commits int64
	for _, result := range results {
		if result.Disposition == dispositionExecuted {
			executed++
		}
		if result.Disposition == dispositionCapacity && result.Reason == "receipt_capacity_exhausted" {
			capacity++
		}
	}
	for _, envelope := range envelopes {
		ring, _ := ringAt(1)
		in, _ := newRequest(runID, envelope.ProducerID, 1, 1, 1, envelope.Payload, ring)
		b, r, c, err := operationCounts(ctx, db, runID, in.OperationID)
		if err != nil {
			return err
		}
		business += b
		receipts += r
		commits += c
	}
	stateOne, err := inspectProducer(ctx, db, runID, "p-cap-r1")
	if err != nil {
		return err
	}
	stateTwo, err := inspectProducer(ctx, db, runID, "p-cap-r2")
	if err != nil {
		return err
	}
	passed := before.consistent() && after.consistent() && before.StoredReceipts == proofReceiptRows-1 &&
		after.StoredReceipts == proofReceiptRows && after.StoredReceipts == before.StoredReceipts+1 &&
		executed == 1 && capacity == 1 && business == 1 && receipts == 1 && commits == 1 &&
		stateOne.NextSequence+stateTwo.NextSequence == 3
	scenarios["two_process_receipt_hard_cap"] = scenarioEvidence{Passed: passed, Results: results,
		Notes: []string{fmt.Sprintf("before=%d after=%d executed=%d capacity=%d business=%d receipts=%d commits=%d", before.StoredReceipts, after.StoredReceipts, executed, capacity, business, receipts, commits)}}
	if !passed {
		return fmt.Errorf("two-process receipt hard-cap proof failed")
	}
	return nil
}

func proveOrderingAndProducerFence(ctx context.Context, db *sql.DB, ring *verificationRing, scenarios map[string]scenarioEvidence, runID string) error {
	sequenceTwo, _ := newRequest(runID, "p-order", 1, 1, 2, operationPrefix+" ordered two", ring)
	gap, err := executeOperation(ctx, db, ring, sequenceTwo, "", "")
	if err != nil {
		return err
	}
	sequenceOne, _ := newRequest(runID, "p-order", 1, 1, 1, operationPrefix+" ordered one", ring)
	first, err := executeOperation(ctx, db, ring, sequenceOne, "", "")
	if err != nil {
		return err
	}
	second, err := executeOperation(ctx, db, ring, sequenceTwo, "", "")
	if err != nil {
		return err
	}
	beforeAdvance, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	if err := advanceProducerEpoch(ctx, db, runID, "p-order", 1, 2); err != nil {
		return err
	}
	afterAdvance, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	oldEpoch, err := executeOperation(ctx, db, ring, sequenceOne, "", "")
	if err != nil {
		return err
	}
	newEpochRequest, _ := newRequest(runID, "p-order", 1, 2, 1, operationPrefix+" producer epoch two", ring)
	newEpoch, err := executeOperation(ctx, db, ring, newEpochRequest, "", "")
	if err != nil {
		return err
	}
	state, err := inspectProducer(ctx, db, runID, "p-order")
	if err != nil {
		return err
	}
	afterNewEpoch, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	passed := gap.Disposition == dispositionGap && first.Disposition == dispositionExecuted && second.Disposition == dispositionExecuted &&
		oldEpoch.Disposition == dispositionGone && oldEpoch.Reason == "producer_epoch_retired" && newEpoch.Disposition == dispositionExecuted &&
		state.ProducerEpoch == 2 && state.NextSequence == 2 && state.ReceiptRows == 1 && state.BusinessRows == 3 &&
		beforeAdvance.consistent() && afterAdvance.consistent() && afterNewEpoch.consistent() &&
		afterAdvance.StoredReceipts == beforeAdvance.StoredReceipts-2 &&
		afterNewEpoch.StoredReceipts == afterAdvance.StoredReceipts+1
	scenarios["adjacent_order_and_producer_epoch"] = scenarioEvidence{Passed: passed,
		Results: []operationResult{gap, first, second, oldEpoch, newEpoch},
		Notes:   []string{fmt.Sprintf("producer epoch release decremented shared receipt counter %d -> %d; new epoch admission raised it to %d", beforeAdvance.StoredReceipts, afterAdvance.StoredReceipts, afterNewEpoch.StoredReceipts)}}
	if !passed {
		return fmt.Errorf("adjacent ordering or producer epoch proof failed")
	}
	return nil
}

func proveRollback(ctx context.Context, db *sql.DB, ring *verificationRing, scenarios map[string]scenarioEvidence, runID string) error {
	in, _ := newRequest(runID, "p-rollback", 1, 1, 1, operationPrefix+" rollback", ring)
	countersBefore, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	_, injectedErr := executeOperation(ctx, db, ring, in, "rollback-before-commit", "")
	countersAfterRollback, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	businessBefore, receiptsBefore, commitsBefore, err := operationCounts(ctx, db, runID, in.OperationID)
	if err != nil {
		return err
	}
	beforeState, err := inspectProducer(ctx, db, runID, "p-rollback")
	if err != nil {
		return err
	}
	result, err := executeOperation(ctx, db, ring, in, "", "")
	if err != nil {
		return err
	}
	businessAfter, receiptsAfter, commitsAfter, err := operationCounts(ctx, db, runID, in.OperationID)
	if err != nil {
		return err
	}
	countersAfterCommit, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	passed := errors.Is(injectedErr, errInjectedRollback) && businessBefore == 0 && receiptsBefore == 0 && commitsBefore == 0 &&
		beforeState.NextSequence == 1 && result.Disposition == dispositionExecuted && businessAfter == 1 && receiptsAfter == 1 && commitsAfter == 1 &&
		countersBefore.consistent() && countersAfterRollback.consistent() && countersAfterCommit.consistent() &&
		countersAfterRollback.StoredReceipts == countersBefore.StoredReceipts && countersAfterRollback.Version == countersBefore.Version &&
		countersAfterCommit.StoredReceipts == countersBefore.StoredReceipts+1
	scenarios["transaction_rollback"] = scenarioEvidence{Passed: passed, Results: []operationResult{result},
		Notes: []string{fmt.Sprintf("rollback kept shared receipt counter/version at %d/%d; committed retry raised counter to %d", countersBefore.StoredReceipts, countersBefore.Version, countersAfterCommit.StoredReceipts), fmt.Sprintf("before business=%d receipts=%d commits=%d; after business=%d receipts=%d commits=%d", businessBefore, receiptsBefore, commitsBefore, businessAfter, receiptsAfter, commitsAfter)}}
	if !passed {
		return fmt.Errorf("transaction rollback proof failed")
	}
	return nil
}

func proveCompactionAndKeys(ctx context.Context, db *sql.DB, primaryRing *verificationRing, scenarios map[string]scenarioEvidence, runID, runtimeDir string) error {
	firstRequest, _ := newRequest(runID, "p-compact", 1, 1, 1, operationPrefix+" compact one", primaryRing)
	secondRequest, _ := newRequest(runID, "p-compact", 1, 1, 2, operationPrefix+" compact two", primaryRing)
	first, err := executeOperation(ctx, db, primaryRing, firstRequest, "", "")
	if err != nil {
		return err
	}
	second, err := executeOperation(ctx, db, primaryRing, secondRequest, "", "")
	if err != nil {
		return err
	}
	countersBeforeKills, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	if err := killedCompaction(ctx, runtimeDir, runID, "p-compact", "after-watermark-before-delete", 1, 1, 1); err != nil {
		return err
	}
	afterWatermarkKill, err := inspectProducer(ctx, db, runID, "p-compact")
	if err != nil {
		return err
	}
	countersAfterWatermarkKill, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	if err := killedCompaction(ctx, runtimeDir, runID, "p-compact", "after-delete-before-commit", 1, 1, 1); err != nil {
		return err
	}
	afterDeleteKill, err := inspectProducer(ctx, db, runID, "p-compact")
	if err != nil {
		return err
	}
	countersAfterDeleteKill, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	if err := compactProducer(ctx, db, runID, "p-compact", 1, 1, 1, "", ""); err != nil {
		return err
	}
	countersAfterCompaction, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	rotatedRing, _ := ringAt(1)
	oldKeyRejectedBefore := rotatedRing.verify(firstRequest)
	for epoch := uint64(2); epoch <= 4; epoch++ {
		if err := rotatedRing.rotate(epoch); err != nil {
			return err
		}
	}
	oldKeyRejectedAfter := rotatedRing.verify(firstRequest)
	compactedRequest, _ := rotatedRing.sign(firstRequest)
	compacted, err := executeOperation(ctx, db, rotatedRing, compactedRequest, "", "")
	if err != nil {
		return err
	}
	recentRequest, _ := rotatedRing.sign(secondRequest)
	recent, err := executeOperation(ctx, db, rotatedRing, recentRequest, "", "")
	if err != nil {
		return err
	}
	business, receipts, commits, err := operationCounts(ctx, db, runID, firstRequest.OperationID)
	if err != nil {
		return err
	}
	state, err := inspectProducer(ctx, db, runID, "p-compact")
	if err != nil {
		return err
	}
	countersAfterDecisions, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	passed := first.Disposition == dispositionExecuted && second.Disposition == dispositionExecuted &&
		afterWatermarkKill.CompactedThrough == 0 && afterWatermarkKill.ReceiptRows == 2 &&
		afterDeleteKill.CompactedThrough == 0 && afterDeleteKill.ReceiptRows == 2 && oldKeyRejectedBefore == nil &&
		errors.Is(oldKeyRejectedAfter, errVerification) && compacted.Disposition == dispositionGone && compacted.Reason == "sequence_compacted" &&
		recent.Disposition == dispositionReplay && business == 1 && receipts == 0 && commits == 1 &&
		state.CompactedThrough == 1 && state.NextSequence == 3 && state.ReceiptRows == 1 && state.BusinessRows == 2 &&
		countersBeforeKills.consistent() && countersAfterWatermarkKill.consistent() && countersAfterDeleteKill.consistent() &&
		countersAfterCompaction.consistent() && countersAfterDecisions.consistent() &&
		countersAfterWatermarkKill.StoredReceipts == countersBeforeKills.StoredReceipts &&
		countersAfterWatermarkKill.Version == countersBeforeKills.Version &&
		countersAfterDeleteKill.StoredReceipts == countersBeforeKills.StoredReceipts &&
		countersAfterDeleteKill.Version == countersBeforeKills.Version &&
		countersAfterCompaction.StoredReceipts == countersBeforeKills.StoredReceipts-1 &&
		countersAfterDecisions.StoredReceipts == countersAfterCompaction.StoredReceipts &&
		countersAfterDecisions.Version == countersAfterCompaction.Version
	scenarios["compaction_and_key_ring"] = scenarioEvidence{Passed: passed,
		Results: []operationResult{first, second, compacted, recent},
		Notes: []string{
			fmt.Sprintf("Process death after watermark update and after receipt deletion kept ledger receipt counter/version at %d/%d; committed compaction decremented it to %d.", countersBeforeKills.StoredReceipts, countersBeforeKills.Version, countersAfterCompaction.StoredReceipts),
			"Key epoch 1 was accepted before rotation and rejected after the bounded ring advanced to epochs 2..4; the same request re-signed at epoch 4 remained Gone.",
		}}
	if !passed {
		return fmt.Errorf("compaction or key-ring proof failed")
	}
	return nil
}

func proveKillBoundaries(ctx context.Context, db *sql.DB, scenarios map[string]scenarioEvidence, runID, runtimeDir string) error {
	tests := []struct {
		name, producer, failpoint string
		wantBefore                int64
		wantRetry                 disposition
	}{
		{name: "kill_after_start", producer: "p-kill-start", failpoint: "after-start", wantBefore: 0, wantRetry: dispositionExecuted},
		{name: "kill_after_mutations", producer: "p-kill-before", failpoint: "after-mutations-before-commit", wantBefore: 0, wantRetry: dispositionExecuted},
		{name: "kill_after_commit_before_response", producer: "p-kill-after", failpoint: "after-dolt-commit-before-response", wantBefore: 1, wantRetry: dispositionReplay},
	}
	for _, test := range tests {
		countersBefore, err := inspectCounters(ctx, db, runID)
		if err != nil {
			return err
		}
		envelope := workerEnvelope{RunID: runID, ProducerID: test.producer, ProducerEpoch: 1, Sequence: 1,
			RepositoryEpoch: 1, Payload: operationPrefix + " " + test.name, Failpoint: test.failpoint}
		if err := killedWorker(ctx, runtimeDir, envelope); err != nil {
			return err
		}
		countersAfterKill, err := inspectCounters(ctx, db, runID)
		if err != nil {
			return err
		}
		ring, _ := ringAt(1)
		in, _ := newRequest(runID, test.producer, 1, 1, 1, envelope.Payload, ring)
		businessBefore, receiptsBefore, commitsBefore, err := operationCounts(ctx, db, runID, in.OperationID)
		if err != nil {
			return err
		}
		restarted, err := singleWorker(ctx, workerEnvelope{RunID: runID, ProducerID: test.producer, ProducerEpoch: 1, Sequence: 1,
			RepositoryEpoch: 1, Payload: envelope.Payload})
		if err != nil {
			return err
		}
		businessAfter, receiptsAfter, commitsAfter, err := operationCounts(ctx, db, runID, in.OperationID)
		if err != nil {
			return err
		}
		countersAfterRetry, err := inspectCounters(ctx, db, runID)
		if err != nil {
			return err
		}
		passed := businessBefore == test.wantBefore && receiptsBefore == test.wantBefore && commitsBefore == test.wantBefore &&
			restarted.Disposition == test.wantRetry && businessAfter == 1 && receiptsAfter == 1 && commitsAfter == 1 &&
			countersBefore.consistent() && countersAfterKill.consistent() && countersAfterRetry.consistent() &&
			countersAfterKill.StoredReceipts == countersBefore.StoredReceipts+test.wantBefore &&
			countersAfterRetry.StoredReceipts == countersBefore.StoredReceipts+1 &&
			((test.wantBefore == 0 && countersAfterKill.Version == countersBefore.Version) ||
				(test.wantBefore == 1 && countersAfterKill.Version == countersBefore.Version+1)) &&
			countersAfterRetry.Version == countersBefore.Version+1
		scenarios[test.name] = scenarioEvidence{Passed: passed, Results: []operationResult{restarted},
			Notes: []string{fmt.Sprintf("ledger receipt counter/version before=%d/%d after-kill=%d/%d after-retry=%d/%d", countersBefore.StoredReceipts, countersBefore.Version, countersAfterKill.StoredReceipts, countersAfterKill.Version, countersAfterRetry.StoredReceipts, countersAfterRetry.Version), fmt.Sprintf("after kill business=%d receipts=%d commits=%d; after restart business=%d receipts=%d commits=%d", businessBefore, receiptsBefore, commitsBefore, businessAfter, receiptsAfter, commitsAfter)}}
		if !passed {
			return fmt.Errorf("%s proof failed", test.name)
		}
	}
	return nil
}

func proveRepositoryRestoreFence(ctx context.Context, db *sql.DB, ring *verificationRing, scenarios map[string]scenarioEvidence, runID, epochOneHead string, report *proofReport) error {
	countersBeforeReset, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	if err := rotateRepositoryEpoch(ctx, db, runID, 2); err != nil {
		return err
	}
	countersAfterReset, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	branch, err := createStaleBranch(ctx, db, runID, epochOneHead)
	if err != nil {
		return err
	}
	report.StaleBranch = branch
	staleRequest, _ := newRequest(runID, "p-restore", 2, 1, 1, operationPrefix+" stale restore", ring)
	stale, before, after, err := staleRestoreDecision(ctx, db, ring, branch, staleRequest)
	if err != nil {
		return err
	}
	oldClientRequest, _ := newRequest(runID, "p-restore", 1, 1, 1, operationPrefix+" old repository epoch", ring)
	oldClient, err := executeOperation(ctx, db, ring, oldClientRequest, "", "")
	if err != nil {
		return err
	}
	registration, err := registerProducer(ctx, db, runID, "p-main2", 2, 1, subjectHash("p-main2"))
	if err != nil {
		return err
	}
	countersAfterRegistration, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	newRequest, _ := newRequest(runID, "p-main2", 2, 1, 1, operationPrefix+" repository epoch two", ring)
	newResult, err := executeOperation(ctx, db, ring, newRequest, "", "")
	if err != nil {
		return err
	}
	countersAfterOperation, err := inspectCounters(ctx, db, runID)
	if err != nil {
		return err
	}
	report.EpochTwoHead, err = currentHead(ctx, db)
	if err != nil {
		return err
	}
	mainInspection, err := inspectRun(ctx, db, runID, 2)
	if err != nil {
		return err
	}
	branchFound := false
	for _, candidate := range mainInspection.Branches {
		branchFound = branchFound || candidate == branch
	}
	passed := stale.Disposition == dispositionFenced && stale.Reason == "repository_epoch_not_activated" && before == 0 && after == 0 &&
		oldClient.Disposition == dispositionGone && oldClient.Reason == "repository_epoch_retired" && newResult.Disposition == dispositionExecuted &&
		registration.Disposition == dispositionExecuted && mainInspection.RepositoryEpoch == 2 && mainInspection.ProducerRows == 1 && mainInspection.ReceiptRows == 1 && branchFound &&
		countersBeforeReset.consistent() && countersAfterReset.consistent() && countersAfterRegistration.consistent() && countersAfterOperation.consistent() &&
		countersBeforeReset.StoredProducers == proofProducerRows && countersBeforeReset.StoredReceipts == proofReceiptRows &&
		countersAfterReset.StoredProducers == 0 && countersAfterReset.StoredReceipts == 0 &&
		countersAfterRegistration.StoredProducers == 1 && countersAfterRegistration.StoredReceipts == 0 &&
		countersAfterOperation.StoredProducers == 1 && countersAfterOperation.StoredReceipts == 1
	scenarios["repository_epoch_restore_fence"] = scenarioEvidence{Passed: passed,
		Results: []operationResult{stale, oldClient, newResult}, Registrations: []registrationResult{registration}, Inspection: &mainInspection,
		Notes: []string{fmt.Sprintf("Epoch reset atomically changed producer/receipt counters %d/%d -> 0/0; registration and operation rebuilt them to %d/%d.", countersBeforeReset.StoredProducers, countersBeforeReset.StoredReceipts, countersAfterOperation.StoredProducers, countersAfterOperation.StoredReceipts), "A writable branch at the exact epoch-1 head modeled a stale restore. External expected epoch 2 fenced it before mutation; main rejected an epoch-1 client as Gone."}}
	if !passed {
		return fmt.Errorf("repository restore fencing proof failed")
	}
	return nil
}

func finishProof(report proofReport, started time.Time) proofReport {
	report.FinishedAt = time.Now().UTC()
	report.Elapsed = report.FinishedAt.Sub(started).Round(time.Millisecond).String()
	return report
}
