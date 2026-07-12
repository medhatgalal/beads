package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	maxSchemaOutcomeBytes = 64 << 10
	maxIdentityBytes      = 128
)

var (
	ErrCapacity       = errors.New("canonical receipt capacity exhausted")
	ErrProducerLimit  = errors.New("canonical producer tombstone capacity exhausted")
	ErrCorruptState   = errors.New("canonical idempotency state is inconsistent")
	ErrInvalidRequest = errors.New("invalid idempotency request")
)

// Binding prevents a ledger file from being silently adopted by another team,
// cell, repository project, or repository-ledger epoch.
type Binding struct {
	CellID      string
	TeamID      string
	ProjectID   string
	LedgerEpoch uint64
}

// Policy places hard upper bounds on every variable-cardinality table and on
// each variable-size outcome. When the retry horizon and a hard capacity are
// simultaneously exhausted, writes fail closed with ErrCapacity.
type Policy struct {
	MaxProducers           int64
	MaxReceipts            int64
	MaxReceiptsPerProducer int64
	KeepRecentPerProducer  int64
	MaxOutcomeBytes        int
	MinimumRetryHorizon    time.Duration
}

func (p Policy) validate() error {
	switch {
	case p.MaxProducers <= 0:
		return fmt.Errorf("max producers must be positive")
	case p.MaxReceipts <= 0:
		return fmt.Errorf("max receipts must be positive")
	case p.MaxReceiptsPerProducer <= 0:
		return fmt.Errorf("max receipts per producer must be positive")
	case p.KeepRecentPerProducer < 0 || p.KeepRecentPerProducer > p.MaxReceiptsPerProducer:
		return fmt.Errorf("keep-recent must be between zero and the per-producer maximum")
	case p.MaxOutcomeBytes <= 0 || p.MaxOutcomeBytes > maxSchemaOutcomeBytes:
		return fmt.Errorf("max outcome bytes must be between 1 and %d", maxSchemaOutcomeBytes)
	case p.MinimumRetryHorizon < 0:
		return fmt.Errorf("minimum retry horizon must not be negative")
	default:
		return nil
	}
}

// Request identifies one ordered operation in a producer epoch. KeyEpoch and
// Signature authenticate the envelope but are deliberately excluded from the
// idempotency identity, so a retry can be re-signed after key rotation.
type Request struct {
	ProjectID     string
	ProducerID    string
	SubjectHash   [32]byte
	LedgerEpoch   uint64
	ProducerEpoch uint64
	Sequence      uint64
	RequestHash   [32]byte
	KeyEpoch      uint64
	Signature     [32]byte
}

func (r Request) validate() error {
	switch {
	case r.ProjectID == "" || len(r.ProjectID) > maxIdentityBytes:
		return fmt.Errorf("%w: project ID must contain 1..%d bytes", ErrInvalidRequest, maxIdentityBytes)
	case r.ProducerID == "" || len(r.ProducerID) > maxIdentityBytes:
		return fmt.Errorf("%w: producer ID must contain 1..%d bytes", ErrInvalidRequest, maxIdentityBytes)
	case zeroDigest(r.SubjectHash):
		return fmt.Errorf("%w: subject hash is required", ErrInvalidRequest)
	case r.LedgerEpoch == 0 || r.LedgerEpoch > math.MaxInt64:
		return fmt.Errorf("%w: ledger epoch is outside the canonical integer range", ErrInvalidRequest)
	case r.ProducerEpoch == 0 || r.ProducerEpoch > math.MaxInt64:
		return fmt.Errorf("%w: producer epoch is outside the canonical integer range", ErrInvalidRequest)
	case r.Sequence == 0 || r.Sequence >= math.MaxInt64:
		return fmt.Errorf("%w: sequence is outside the canonical integer range", ErrInvalidRequest)
	case r.KeyEpoch == 0 || r.KeyEpoch > math.MaxInt64:
		return fmt.Errorf("%w: key epoch is outside the canonical integer range", ErrInvalidRequest)
	case zeroDigest(r.RequestHash):
		return fmt.Errorf("%w: request hash is required", ErrInvalidRequest)
	default:
		return nil
	}
}

func zeroDigest(value [32]byte) bool {
	return value == [32]byte{}
}

type Outcome struct {
	Code    string
	Payload []byte
}

type Disposition string

const (
	DispositionExecuted     Disposition = "executed"
	DispositionReplay       Disposition = "replay"
	DispositionGone         Disposition = "gone"
	DispositionConflict     Disposition = "conflict"
	DispositionGap          Disposition = "sequence_gap"
	DispositionFenced       Disposition = "producer_fenced"
	DispositionUnknown      Disposition = "unknown_producer"
	DispositionUnauthorized Disposition = "unauthorized"
)

// Decision maps directly to an API result. Gone is HTTP 410 and is the
// permanent promise that a compacted or retired request was not re-executed.
type Decision struct {
	Disposition        Disposition
	Reason             string
	Outcome            Outcome
	CurrentLedgerEpoch uint64
	CurrentEpoch       uint64
	NextSequence       uint64
	CompactedThrough   uint64
}

type Mutation func(context.Context, *sql.Tx) error

type CompactPhase string

const (
	CompactAfterWatermarks CompactPhase = "after_watermarks"
	CompactAfterDeletes    CompactPhase = "after_deletes"
	CompactBeforeCommit    CompactPhase = "before_commit"
	CompactAfterCommit     CompactPhase = "after_commit"
)

type CompactHook func(CompactPhase) error

type CompactResult struct {
	ProducersAdvanced int64
	ReceiptsDeleted   int64
}

type StoreStats struct {
	Producers           int64 `json:"producers"`
	Receipts            int64 `json:"receipts"`
	OutcomePayloadBytes int64 `json:"outcome_payload_bytes"`
}

type batchRecord struct {
	Request     Request
	Outcome     Outcome
	CommittedAt time.Time
}
