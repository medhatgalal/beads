package main

import "time"

const (
	labDatabase       = "beads_perf_lab_retention_ae"
	labHost           = "127.0.0.1"
	labPort           = 13360
	labID             = "f42f2fbb-cdd5-4b86-b600-e68446e71f23"
	schemaVersion     = 2
	maxAttempts       = 8
	maxProducerRows   = 512
	maxReceiptRows    = 20_000
	maxOutcomeBytes   = 4096
	proofProducerRows = 11
	proofReceiptRows  = 8
	proofOutcomeBytes = 256
	operationPrefix   = "[beads-perf-lab]"
	commitPrefix      = "beads-perf-lab retention "
	staleBranchPrefix = "retention_stale_"
)

type disposition string

const (
	dispositionExecuted     disposition = "executed"
	dispositionReplay       disposition = "replay"
	dispositionGone         disposition = "gone"
	dispositionConflict     disposition = "conflict"
	dispositionGap          disposition = "sequence_gap"
	dispositionFenced       disposition = "fenced"
	dispositionUnknown      disposition = "unknown_producer"
	dispositionUnauthorized disposition = "unauthorized"
	dispositionCapacity     disposition = "capacity_exhausted"
)

type ledgerState struct {
	RepositoryEpoch uint64
	MaxProducers    int64
	MaxReceipts     int64
	MaxOutcomeBytes int64
	ProducerCount   int64
	ReceiptCount    int64
	Version         uint64
}

type request struct {
	RunID                   string
	ProducerID              string
	SubjectHash             string
	ExpectedRepositoryEpoch uint64
	ProducerEpoch           uint64
	Sequence                uint64
	RequestHash             string
	OperationID             string
	Payload                 string
	KeyEpoch                uint64
	Signature               [32]byte
}

type operationResult struct {
	Disposition          disposition `json:"disposition"`
	Reason               string      `json:"reason"`
	OperationID          string      `json:"operation_id"`
	ProducerID           string      `json:"producer_id"`
	RepositoryEpoch      uint64      `json:"repository_epoch"`
	ProducerEpoch        uint64      `json:"producer_epoch"`
	Sequence             uint64      `json:"sequence"`
	NextSequence         uint64      `json:"next_sequence"`
	CompactedThrough     uint64      `json:"compacted_through"`
	Attempts             int         `json:"attempts"`
	SerializationRetries int         `json:"serialization_retries"`
	DuplicateRetries     int         `json:"duplicate_retries"`
	CASRetries           int         `json:"cas_retries"`
	CommitMessage        string      `json:"commit_message,omitempty"`
	WallMS               float64     `json:"wall_ms"`
}

type registrationResult struct {
	Disposition          disposition `json:"disposition"`
	Reason               string      `json:"reason"`
	ProducerID           string      `json:"producer_id"`
	RepositoryEpoch      uint64      `json:"repository_epoch"`
	ProducerEpoch        uint64      `json:"producer_epoch"`
	ProducerCount        int64       `json:"producer_count"`
	Attempts             int         `json:"attempts"`
	SerializationRetries int         `json:"serialization_retries"`
	DuplicateRetries     int         `json:"duplicate_retries"`
	CASRetries           int         `json:"cas_retries"`
	CommitMessage        string      `json:"commit_message,omitempty"`
	WallMS               float64     `json:"wall_ms"`
}

type producerInspection struct {
	ProducerID       string `json:"producer_id"`
	Exists           bool   `json:"exists"`
	ProducerEpoch    uint64 `json:"producer_epoch"`
	NextSequence     uint64 `json:"next_sequence"`
	CompactedThrough uint64 `json:"compacted_through"`
	ReceiptRows      int64  `json:"receipt_rows"`
	BusinessRows     int64  `json:"business_rows"`
}

type inspection struct {
	InspectedAt           time.Time                     `json:"inspected_at"`
	Database              string                        `json:"database"`
	MySQLCompatVersion    string                        `json:"mysql_compat_version"`
	DoltVersion           string                        `json:"dolt_version"`
	LabID                 string                        `json:"lab_id"`
	SchemaVersion         int                           `json:"schema_version"`
	RunID                 string                        `json:"run_id"`
	RepositoryEpoch       uint64                        `json:"repository_epoch"`
	MaxProducers          int64                         `json:"max_producers"`
	MaxReceipts           int64                         `json:"max_receipts"`
	MaxOutcomeBytes       int64                         `json:"max_outcome_bytes"`
	StoredProducerCount   int64                         `json:"stored_producer_count"`
	StoredReceiptCount    int64                         `json:"stored_receipt_count"`
	LedgerVersion         uint64                        `json:"ledger_version"`
	CounterInvariant      bool                          `json:"counter_invariant"`
	ProducerRows          int64                         `json:"producer_rows"`
	ReceiptRows           int64                         `json:"receipt_rows"`
	BusinessRows          int64                         `json:"business_rows"`
	HeadHash              string                        `json:"head_hash"`
	DirtyTableRows        int64                         `json:"dirty_table_rows"`
	RunCommitRows         int64                         `json:"run_commit_rows"`
	Branches              []string                      `json:"branches"`
	Producers             map[string]producerInspection `json:"producers"`
	OperationBusinessRows map[string]int64              `json:"operation_business_rows"`
	OperationCommitRows   map[string]int64              `json:"operation_commit_rows"`
	Passed                bool                          `json:"passed"`
}

type scenarioEvidence struct {
	Passed        bool                 `json:"passed"`
	Results       []operationResult    `json:"results,omitempty"`
	Registrations []registrationResult `json:"registrations,omitempty"`
	Inspection    *inspection          `json:"inspection,omitempty"`
	Notes         []string             `json:"notes,omitempty"`
}

type proofReport struct {
	Schema             string                      `json:"schema"`
	StartedAt          time.Time                   `json:"started_at"`
	FinishedAt         time.Time                   `json:"finished_at"`
	Elapsed            string                      `json:"elapsed"`
	Database           string                      `json:"database"`
	Host               string                      `json:"host"`
	Port               int                         `json:"port"`
	RunID              string                      `json:"run_id"`
	MySQLCompatVersion string                      `json:"mysql_compat_version"`
	DoltVersion        string                      `json:"dolt_version"`
	SetupHead          string                      `json:"setup_head"`
	EpochOneHead       string                      `json:"epoch_one_head"`
	EpochTwoHead       string                      `json:"epoch_two_head"`
	StaleBranch        string                      `json:"stale_branch"`
	Scenarios          map[string]scenarioEvidence `json:"scenarios"`
	FinalInspection    inspection                  `json:"final_inspection"`
	Passed             bool                        `json:"passed"`
	PromotionDecision  string                      `json:"promotion_decision"`
	Error              string                      `json:"error,omitempty"`
	RemainingGaps      []string                    `json:"remaining_gaps"`
}

type workerEnvelope struct {
	RunID           string `json:"run_id"`
	ProducerID      string `json:"producer_id"`
	ProducerEpoch   uint64 `json:"producer_epoch"`
	Sequence        uint64 `json:"sequence"`
	RepositoryEpoch uint64 `json:"repository_epoch"`
	Payload         string `json:"payload"`
	StartAtUnixNano int64  `json:"start_at_unix_nano,omitempty"`
	Failpoint       string `json:"failpoint,omitempty"`
	ReadyFile       string `json:"ready_file,omitempty"`
}

type registrationEnvelope struct {
	RunID           string `json:"run_id"`
	ProducerID      string `json:"producer_id"`
	ProducerEpoch   uint64 `json:"producer_epoch"`
	RepositoryEpoch uint64 `json:"repository_epoch"`
	StartAtUnixNano int64  `json:"start_at_unix_nano,omitempty"`
	Failpoint       string `json:"failpoint,omitempty"`
	ReadyFile       string `json:"ready_file,omitempty"`
}

type counterSnapshot struct {
	StoredProducers   int64
	PhysicalProducers int64
	StoredReceipts    int64
	PhysicalReceipts  int64
	Version           uint64
}

func (s counterSnapshot) consistent() bool {
	return s.StoredProducers == s.PhysicalProducers && s.StoredReceipts == s.PhysicalReceipts
}
