package main

import (
	"encoding/json"
	"time"
)

const (
	inventorySchemaVersion  = 1
	reportSchemaVersion     = 2
	inspectionSchemaVersion = 1
	maxBatchSize            = 200
	maxExpectedOperations   = 100_000
	maxExpectedIssues       = 200_000
	maxQueueEvidenceBytes   = 256 << 20
	maxDurableRetainedBytes = 128 << 20
)

const (
	labAttestationKey = "_beads_perf_lab_attestation_v1"
	receiptPrefix     = "_beads_perf_gateway_receipt_v1/"
	outboxPrefix      = "_beads_perf_gateway_outbox_v1/"
	commitPrefix      = "beads-perf-lab gateway "
)

type inventory struct {
	SchemaVersion int               `json:"schema_version"`
	LabID         string            `json:"lab_id"`
	Targets       []inventoryTarget `json:"targets"`
}

type inventoryTarget struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	DatabaseID   string `json:"database_id"`
	ProjectID    string `json:"project_id"`
	PasswordFile string `json:"password_file"`
	QueuePath    string `json:"queue_path"`
}

type boundedStoreEvidence struct {
	Seen      int64 `json:"seen"`
	Retained  int   `json:"retained"`
	Limit     int   `json:"limit"`
	Truncated bool  `json:"truncated"`
}

type runnerSample struct {
	Identity        string  `json:"identity"`
	Ordinal         int64   `json:"ordinal"`
	Kind            string  `json:"kind"`
	Command         string  `json:"command"`
	OriginTeam      string  `json:"origin_team"`
	DestinationTeam string  `json:"destination_team"`
	DatabaseID      string  `json:"database_id"`
	OperationID     string  `json:"operation_id,omitempty"`
	QueueMS         float64 `json:"client_dispatch_queue_ms"`
	ServiceMS       float64 `json:"request_service_ms"`
	EndToEndMS      float64 `json:"end_to_end_ms"`
	HTTPStatus      int     `json:"http_status,omitempty"`
	Passed          bool    `json:"passed"`
	Unclassified    bool    `json:"unclassified"`
	ErrorClass      string  `json:"error_class,omitempty"`
}

type runnerMetricGroup struct {
	Commands     int64 `json:"commands"`
	Passed       int64 `json:"passed"`
	Failed       int64 `json:"failed"`
	Unclassified int64 `json:"unclassified"`
}

type runnerReport struct {
	SchemaVersion int    `json:"schema_version"`
	Mode          string `json:"mode"`
	Profile       string `json:"profile"`
	Config        struct {
		RunID               string   `json:"run_id"`
		LabID               string   `json:"lab_id"`
		RegisteredDatabases int      `json:"registered_databases"`
		ActiveDatabases     int      `json:"active_databases"`
		ActiveDatabaseIDs   []string `json:"active_database_ids"`
	} `json:"config"`
	Attestation struct {
		Required int `json:"required"`
		Verified int `json:"verified"`
	} `json:"attestation_inventory"`
	Metrics struct {
		Generated          int64                        `json:"generated"`
		Completed          int64                        `json:"completed"`
		Dropped            int64                        `json:"dropped"`
		Unclassified       int64                        `json:"unclassified"`
		AccountingBalanced bool                         `json:"accounting_balanced"`
		Passed             int64                        `json:"passed"`
		Failed             int64                        `json:"failed"`
		ReadCommands       int64                        `json:"read_commands"`
		PointWrites        int64                        `json:"point_writes"`
		Graphs             int64                        `json:"graphs"`
		ByDatabase         map[string]runnerMetricGroup `json:"by_database"`
	} `json:"metrics"`
	Correctness struct {
		HTTPFailures          int64 `json:"http_failures"`
		InvalidJSON           int64 `json:"invalid_json"`
		TerminalFailures      int64 `json:"terminal_failures"`
		NonterminalTimeouts   int64 `json:"nonterminal_timeouts"`
		ProjectMismatches     int64 `json:"project_mismatches"`
		DuplicateOperationIDs int64 `json:"duplicate_operation_ids"`
		MissingOperationIDs   int64 `json:"missing_operation_ids"`
		DroppedAtAdmission    int64 `json:"dropped_at_client_admission"`
		UnclassifiedOutcomes  int64 `json:"unclassified_outcomes"`
	} `json:"correctness_counters"`
	BoundedEvidence struct {
		ResultLatency    boundedStoreEvidence `json:"result_latency_reservoir"`
		OperationIDProof boundedStoreEvidence `json:"operation_id_duplicate_proof"`
		SampleRecords    boundedStoreEvidence `json:"sample_records"`
	} `json:"bounded_evidence"`
	Samples []runnerSample `json:"samples"`
}

type expectedOperation struct {
	ID       string
	Identity string
	Kind     string
	Database string
	Titles   []string
	Edges    []expectedEdge
}

type expectedEdge struct {
	FromTitle string
	ToTitle   string
	Type      string
}

type expectedDatabase struct {
	DatabaseID string
	ProjectID  string
	Operations map[string]expectedOperation
	Titles     map[string]struct{}
	Edges      map[string]struct{}
	Events     map[string]struct{}
}

type expectedRun struct {
	RunID      string
	LabID      string
	ByDatabase map[string]*expectedDatabase
	Operations map[string]expectedOperation
	IssueCount int
	EdgeCount  int
	EventCount int
}

type queueOperation struct {
	ID          string
	ProjectID   string
	KeyHash     string
	SubjectHash string
	RequestHash string
	Kind        string
	Payload     []byte
	Status      string
	Attempts    int
	Result      []byte
}

type queueOutbox struct {
	OperationID string
	Sequence    int
	EventType   string
	Payload     []byte
}

type outboxMarker struct {
	OperationID string   `json:"operation_id"`
	Kind        string   `json:"kind"`
	AffectedIDs []string `json:"affected_ids"`
	ResultHash  string   `json:"result_hash"`
}

type queueEvidence struct {
	Operations map[string]queueOperation
	Outbox     map[string]queueOutbox
	Bytes      int64
}

type receipt struct {
	Version     int             `json:"version"`
	OperationID string          `json:"operation_id"`
	ProjectID   string          `json:"project_id"`
	KeyHash     string          `json:"key_hash"`
	SubjectHash string          `json:"subject_hash"`
	RequestHash string          `json:"request_hash"`
	Kind        string          `json:"kind"`
	Result      json.RawMessage `json:"result"`
	Outbox      struct {
		OperationID string          `json:"operation_id"`
		Sequence    int             `json:"sequence"`
		EventType   string          `json:"event_type"`
		Payload     json.RawMessage `json:"payload"`
		CreatedAt   time.Time       `json:"created_at"`
	} `json:"outbox"`
	CommittedAt time.Time `json:"committed_at"`
	Reconciled  bool      `json:"reconciled,omitempty"`
}

type databaseInspection struct {
	DatabaseID                  string `json:"database_id"`
	ProjectID                   string `json:"project_id"`
	SQLUser                     string `json:"sql_user"`
	IdentityVerified            bool   `json:"identity_verified"`
	AttestationVerified         bool   `json:"attestation_verified"`
	DirtyTableCount             int    `json:"dirty_table_count"`
	ExpectedOperations          int    `json:"expected_operations"`
	QueueSucceededOperations    int    `json:"queue_succeeded_operations"`
	QueueOutboxEvents           int    `json:"queue_outbox_events"`
	ExpectedIssues              int    `json:"expected_issues"`
	ActualIssues                int    `json:"actual_issues"`
	ExpectedDependencies        int    `json:"expected_dependencies"`
	ActualDependencies          int    `json:"actual_dependencies"`
	ExpectedEvents              int    `json:"expected_events"`
	ActualEvents                int    `json:"actual_events"`
	ReceiptCount                int    `json:"receipt_count"`
	OutboxMarkerCount           int    `json:"outbox_marker_count"`
	OperationCommitCount        int    `json:"operation_commit_count"`
	ForeignReceiptCount         int    `json:"foreign_receipt_count"`
	ForeignOutboxMarkerCount    int    `json:"foreign_outbox_marker_count"`
	ForeignOperationCommitCount int    `json:"foreign_operation_commit_count"`
	Passed                      bool   `json:"passed"`
	ErrorClass                  string `json:"error_class,omitempty"`
}

type inspectionReport struct {
	SchemaVersion        int                  `json:"schema_version"`
	InspectedAt          time.Time            `json:"inspected_at"`
	RunID                string               `json:"run_id"`
	LabID                string               `json:"lab_id"`
	RunnerReportSHA256   string               `json:"runner_report_sha256"`
	InventorySHA256      string               `json:"inventory_sha256"`
	ReportEvidenceExact  bool                 `json:"report_evidence_exact"`
	ExpectedOperations   int                  `json:"expected_operations"`
	ExpectedIssues       int                  `json:"expected_issues"`
	ExpectedDependencies int                  `json:"expected_dependencies"`
	ExpectedEvents       int                  `json:"expected_events"`
	Databases            []databaseInspection `json:"databases"`
	Passed               bool                 `json:"passed"`
	Notes                []string             `json:"notes"`
}
