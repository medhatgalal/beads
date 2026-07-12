package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"
)

type soakConfig struct {
	Path         string
	Operations   int
	Producers    int
	BatchSize    int
	SampleEvery  int
	LogicalRate  float64
	OutcomeBytes int
	MemoryBound  uint64
	Binding      Binding
	Policy       Policy
}

type soakSample struct {
	OperationsComplete int        `json:"operations_complete"`
	LogicalTime        time.Time  `json:"logical_time"`
	Stats              StoreStats `json:"store"`
	StoreFileBytes     int64      `json:"store_file_bytes"`
	HeapAllocBytes     uint64     `json:"heap_alloc_bytes"`
	HeapSysBytes       uint64     `json:"heap_sys_bytes"`
}

type soakReport struct {
	Schema                       string       `json:"schema"`
	StartedAt                    time.Time    `json:"started_at"`
	FinishedAt                   time.Time    `json:"finished_at"`
	Elapsed                      string       `json:"elapsed"`
	Operations                   int          `json:"operations"`
	Producers                    int          `json:"producers"`
	LogicalOperationsPerSecond   float64      `json:"logical_operations_per_second"`
	ConfiguredMaxProducers       int64        `json:"configured_max_producers"`
	ConfiguredMaxReceipts        int64        `json:"configured_max_receipts"`
	ConfiguredMaxPerProducer     int64        `json:"configured_max_receipts_per_producer"`
	ConfiguredMaxOutcomeBytes    int          `json:"configured_max_outcome_bytes"`
	ConfiguredPayloadBoundBytes  int64        `json:"configured_payload_bound_bytes"`
	ConfiguredMemoryBoundBytes   uint64       `json:"configured_memory_bound_bytes"`
	MaxObservedReceipts          int64        `json:"max_observed_receipts"`
	MaxObservedStoreFileBytes    int64        `json:"max_observed_store_file_bytes"`
	MaxObservedHeapAllocBytes    uint64       `json:"max_observed_heap_alloc_bytes"`
	SteadyStateFinal             StoreStats   `json:"steady_state_final"`
	AfterEpochReclaim            StoreStats   `json:"after_epoch_reclaim"`
	AfterEpochReregistration     StoreStats   `json:"after_epoch_reregistration"`
	Samples                      []soakSample `json:"samples"`
	DiskPlateau                  bool         `json:"disk_plateau"`
	MemoryWithinBound            bool         `json:"memory_within_bound"`
	OldCompactedRetryDisposition Disposition  `json:"old_compacted_retry_disposition"`
	RecentRetryDisposition       Disposition  `json:"recent_retry_disposition"`
	ConflictingRetryDisposition  Disposition  `json:"conflicting_retry_disposition"`
	RetryMutationCalls           int          `json:"retry_mutation_calls"`
	LedgerEpochRetryDisposition  Disposition  `json:"ledger_epoch_retry_disposition"`
	LedgerEpochRetryMutations    int          `json:"ledger_epoch_retry_mutations"`
	PostRotationNewDisposition   Disposition  `json:"post_rotation_new_disposition"`
	LedgerEpochAfterRotation     uint64       `json:"ledger_epoch_after_rotation"`
	RetiredKeyRejected           bool         `json:"retired_key_rejected"`
	ActiveKeyEpoch               uint64       `json:"active_key_epoch"`
	OldestVerificationKeyEpoch   uint64       `json:"oldest_verification_key_epoch"`
	VerificationKeyCount         int          `json:"verification_key_count"`
	QuickCheckPassed             bool         `json:"quick_check_passed"`
	Pass                         bool         `json:"pass"`
	Limits                       []string     `json:"limits"`
}

func runSoak(ctx context.Context, config soakConfig) (soakReport, error) {
	if config.Operations <= 0 || config.Producers <= 0 || config.BatchSize <= 0 ||
		config.SampleEvery <= 0 || config.LogicalRate <= 0 || config.OutcomeBytes <= 0 {
		return soakReport{}, fmt.Errorf("soak dimensions and rate must be positive")
	}
	if config.OutcomeBytes > config.Policy.MaxOutcomeBytes {
		return soakReport{}, fmt.Errorf("soak outcome exceeds configured outcome bound")
	}
	if _, err := os.Lstat(config.Path); err == nil {
		return soakReport{}, fmt.Errorf("refusing to reuse existing soak path %q", config.Path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return soakReport{}, fmt.Errorf("inspect soak path: %w", err)
	}
	started := time.Now().UTC()
	store, err := openCanonicalStore(ctx, config.Path, config.Binding, config.Policy)
	if err != nil {
		return soakReport{}, err
	}
	defer store.Close()
	secret := bytes.Repeat([]byte{0x41}, 32)
	keys, err := NewVerificationRing(3, 1, secret)
	if err != nil {
		return soakReport{}, err
	}
	ledger, err := newLedger(store, keys)
	if err != nil {
		return soakReport{}, err
	}
	const projectID = "ae-soak"
	logicalStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for producer := 0; producer < config.Producers; producer++ {
		if err := store.RegisterProducer(ctx, projectID, producerName(producer), subjectHashFor(producerName(producer)), 1, 1, logicalStart); err != nil {
			return soakReport{}, err
		}
	}
	if err := createBusinessProbeTable(ctx, store.db); err != nil {
		return soakReport{}, fmt.Errorf("create business probe: %w", err)
	}

	interval := time.Duration(float64(time.Second) / config.LogicalRate)
	sequences := make([]uint64, config.Producers)
	outcomePayload := bytes.Repeat([]byte{0x78}, config.OutcomeBytes)
	var oldSigned Request
	report := soakReport{
		Schema:                      "beads.idempotency_retention_soak.v1",
		StartedAt:                   started,
		Operations:                  config.Operations,
		Producers:                   config.Producers,
		LogicalOperationsPerSecond:  config.LogicalRate,
		ConfiguredMaxProducers:      config.Policy.MaxProducers,
		ConfiguredMaxReceipts:       config.Policy.MaxReceipts,
		ConfiguredMaxPerProducer:    config.Policy.MaxReceiptsPerProducer,
		ConfiguredMaxOutcomeBytes:   config.Policy.MaxOutcomeBytes,
		ConfiguredPayloadBoundBytes: config.Policy.MaxReceipts * int64(config.Policy.MaxOutcomeBytes),
		ConfiguredMemoryBoundBytes:  config.MemoryBound,
	}
	for offset := 0; offset < config.Operations; offset += config.BatchSize {
		end := offset + config.BatchSize
		if end > config.Operations {
			end = config.Operations
		}
		records := make([]batchRecord, 0, end-offset)
		for operation := offset; operation < end; operation++ {
			producer := operation % config.Producers
			sequences[producer]++
			request := Request{
				ProjectID:     projectID,
				ProducerID:    producerName(producer),
				SubjectHash:   subjectHashFor(producerName(producer)),
				LedgerEpoch:   1,
				ProducerEpoch: 1,
				Sequence:      sequences[producer],
				RequestHash:   operationHash(projectID, producerName(producer), 1, sequences[producer]),
			}
			request, err = keys.Sign(request)
			if err != nil {
				return soakReport{}, err
			}
			if operation == 0 {
				oldSigned = request
			}
			records = append(records, batchRecord{
				Request:     request,
				Outcome:     Outcome{Code: "ok", Payload: outcomePayload},
				CommittedAt: logicalStart.Add(time.Duration(operation) * interval),
			})
		}
		compactAt := logicalStart.Add(time.Duration(end-1) * interval)
		if err := ledger.appendBatch(ctx, records, compactAt); err != nil {
			return soakReport{}, fmt.Errorf("append operations %d..%d: %w", offset, end, err)
		}
		statsBefore, err := store.Stats(ctx)
		if err != nil {
			return soakReport{}, err
		}
		if statsBefore.Receipts > report.MaxObservedReceipts {
			report.MaxObservedReceipts = statsBefore.Receipts
		}
		if _, err := store.Compact(ctx, compactAt, nil); err != nil {
			return soakReport{}, err
		}
		if end%config.SampleEvery == 0 || end == config.Operations {
			if err := store.Checkpoint(ctx); err != nil {
				return soakReport{}, err
			}
			stats, err := store.Stats(ctx)
			if err != nil {
				return soakReport{}, err
			}
			runtime.GC()
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			fileBytes, err := storeFileBytes(config.Path)
			if err != nil {
				return soakReport{}, err
			}
			sample := soakSample{
				OperationsComplete: end,
				LogicalTime:        compactAt,
				Stats:              stats,
				StoreFileBytes:     fileBytes,
				HeapAllocBytes:     memory.HeapAlloc,
				HeapSysBytes:       memory.HeapSys,
			}
			report.Samples = append(report.Samples, sample)
			if fileBytes > report.MaxObservedStoreFileBytes {
				report.MaxObservedStoreFileBytes = fileBytes
			}
			if memory.HeapAlloc > report.MaxObservedHeapAllocBytes {
				report.MaxObservedHeapAllocBytes = memory.HeapAlloc
			}
		}
	}

	for epoch := uint64(2); epoch <= 5; epoch++ {
		secret := bytes.Repeat([]byte{byte(0x40 + epoch)}, 32)
		if err := keys.Rotate(epoch, secret); err != nil {
			return soakReport{}, err
		}
	}
	report.RetiredKeyRejected = errors.Is(keys.Verify(oldSigned), ErrVerification)
	var retryMutations int
	oldRetry := Request{
		ProjectID:     projectID,
		ProducerID:    producerName(0),
		SubjectHash:   subjectHashFor(producerName(0)),
		LedgerEpoch:   1,
		ProducerEpoch: 1,
		Sequence:      1,
		RequestHash:   operationHash(projectID, producerName(0), 1, 1),
	}
	oldRetry, err = keys.Sign(oldRetry)
	if err != nil {
		return soakReport{}, err
	}
	decision, err := ledger.Execute(ctx, oldRetry, Outcome{Code: "should_not_execute"}, time.Now().UTC(), func(context.Context, *sql.Tx) error {
		retryMutations++
		return nil
	})
	if err != nil {
		return soakReport{}, err
	}
	report.OldCompactedRetryDisposition = decision.Disposition
	lastSequence := sequences[0]
	recent := Request{
		ProjectID:     projectID,
		ProducerID:    producerName(0),
		SubjectHash:   subjectHashFor(producerName(0)),
		LedgerEpoch:   1,
		ProducerEpoch: 1,
		Sequence:      lastSequence,
		RequestHash:   operationHash(projectID, producerName(0), 1, lastSequence),
	}
	recent, err = keys.Sign(recent)
	if err != nil {
		return soakReport{}, err
	}
	decision, err = ledger.Execute(ctx, recent, Outcome{Code: "should_not_replace"}, time.Now().UTC(), func(context.Context, *sql.Tx) error {
		retryMutations++
		return nil
	})
	if err != nil {
		return soakReport{}, err
	}
	report.RecentRetryDisposition = decision.Disposition
	conflict := recent
	conflict.RequestHash = sha256.Sum256([]byte("different-request"))
	conflict, err = keys.Sign(conflict)
	if err != nil {
		return soakReport{}, err
	}
	decision, err = ledger.Execute(ctx, conflict, Outcome{Code: "should_not_execute"}, time.Now().UTC(), func(context.Context, *sql.Tx) error {
		retryMutations++
		return nil
	})
	if err != nil {
		return soakReport{}, err
	}
	report.ConflictingRetryDisposition = decision.Disposition
	report.RetryMutationCalls = retryMutations
	report.ActiveKeyEpoch, report.OldestVerificationKeyEpoch, report.VerificationKeyCount = keys.Bounds()
	if err := store.Checkpoint(ctx); err != nil {
		return soakReport{}, err
	}
	report.SteadyStateFinal, err = store.Stats(ctx)
	if err != nil {
		return soakReport{}, err
	}
	if err := store.RotateLedgerEpoch(ctx, 2, time.Now().UTC()); err != nil {
		return soakReport{}, err
	}
	var epochRetryMutations int
	decision, err = ledger.Execute(ctx, recent, Outcome{Code: "should_not_execute"}, time.Now().UTC(), func(context.Context, *sql.Tx) error {
		epochRetryMutations++
		return nil
	})
	if err != nil {
		return soakReport{}, err
	}
	report.LedgerEpochRetryDisposition = decision.Disposition
	report.LedgerEpochRetryMutations = epochRetryMutations
	report.AfterEpochReclaim, err = store.Stats(ctx)
	if err != nil {
		return soakReport{}, err
	}
	replacementSubject := subjectHashFor("replacement-subject")
	if err := store.RegisterProducer(ctx, projectID, producerName(0), replacementSubject, 2, 1, time.Now().UTC()); err != nil {
		return soakReport{}, err
	}
	postRotation := Request{
		ProjectID:     projectID,
		ProducerID:    producerName(0),
		SubjectHash:   replacementSubject,
		LedgerEpoch:   2,
		ProducerEpoch: 1,
		Sequence:      1,
		RequestHash:   operationHash(projectID, producerName(0), 2, 1),
	}
	postRotation, err = keys.Sign(postRotation)
	if err != nil {
		return soakReport{}, err
	}
	decision, err = ledger.Execute(ctx, postRotation, Outcome{Code: "ok", Payload: outcomePayload}, time.Now().UTC(), nil)
	if err != nil {
		return soakReport{}, err
	}
	report.PostRotationNewDisposition = decision.Disposition
	report.AfterEpochReregistration, err = store.Stats(ctx)
	if err != nil {
		return soakReport{}, err
	}
	report.LedgerEpochAfterRotation, err = store.LedgerEpoch(ctx)
	if err != nil {
		return soakReport{}, err
	}
	report.QuickCheckPassed = store.QuickCheck(ctx) == nil
	if err := store.Checkpoint(ctx); err != nil {
		return soakReport{}, err
	}
	report.DiskPlateau = diskPlateau(report.Samples)
	report.MemoryWithinBound = report.MaxObservedHeapAllocBytes <= config.MemoryBound
	report.FinishedAt = time.Now().UTC()
	report.Elapsed = report.FinishedAt.Sub(started).Round(time.Millisecond).String()
	report.Limits = []string{
		"The soak uses SQLite as an ACID stand-in; Dolt transaction and commit-GC behavior still require multi-process GCP proof.",
		"The accelerated batch path proves storage cardinality, not one-request-per-transaction crash granularity.",
		"A finite retry horizon plus finite storage requires fail-closed backpressure when no receipt is eligible for compaction.",
	}
	report.Pass = report.SteadyStateFinal.Producers == int64(config.Producers) &&
		report.SteadyStateFinal.Receipts <= config.Policy.MaxReceipts &&
		report.MaxObservedReceipts <= config.Policy.MaxReceipts &&
		report.OldCompactedRetryDisposition == DispositionGone &&
		report.RecentRetryDisposition == DispositionReplay &&
		report.ConflictingRetryDisposition == DispositionConflict &&
		report.RetryMutationCalls == 0 && report.RetiredKeyRejected &&
		report.LedgerEpochRetryDisposition == DispositionGone && report.LedgerEpochRetryMutations == 0 &&
		report.AfterEpochReclaim.Producers == 0 && report.AfterEpochReclaim.Receipts == 0 &&
		report.PostRotationNewDisposition == DispositionExecuted && report.LedgerEpochAfterRotation == 2 &&
		report.AfterEpochReregistration.Producers == 1 && report.AfterEpochReregistration.Receipts == 1 &&
		report.VerificationKeyCount <= 3 && report.QuickCheckPassed &&
		report.DiskPlateau && report.MemoryWithinBound
	return report, nil
}

func producerName(index int) string {
	return fmt.Sprintf("producer-%04d", index)
}

func subjectHashFor(producerID string) [32]byte {
	return sha256.Sum256([]byte("subject:" + producerID))
}

func operationHash(projectID, producerID string, epoch, sequence uint64) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(projectID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(producerID))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], epoch)
	_, _ = hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], sequence)
	_, _ = hash.Write(number[:])
	var out [32]byte
	copy(out[:], hash.Sum(nil))
	return out
}

func storeFileBytes(path string) (int64, error) {
	var total int64
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("stat canonical store file: %w", err)
		}
		total += info.Size()
	}
	return total, nil
}

func diskPlateau(samples []soakSample) bool {
	if len(samples) < 3 {
		return false
	}
	window := samples[len(samples)-3:]
	minimum, maximum := window[0].StoreFileBytes, window[0].StoreFileBytes
	for _, sample := range window[1:] {
		if sample.StoreFileBytes < minimum {
			minimum = sample.StoreFileBytes
		}
		if sample.StoreFileBytes > maximum {
			maximum = sample.StoreFileBytes
		}
	}
	if maximum == 0 {
		return false
	}
	return float64(maximum-minimum)/float64(maximum) <= 0.05
}
