package main

import (
	"encoding/json"
	"time"
)

const labAttestationMetadataKey = "_beads_perf_lab_attestation_v1"

const (
	statusAccepted  = "accepted"
	statusRunning   = "running"
	statusRetryWait = "retry_wait"
	statusSucceeded = "succeeded"
	statusFailed    = "failed"
	statusUnknown   = "unknown"
)

type operationRequest struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type operation struct {
	ID            string          `json:"id"`
	ProjectID     string          `json:"project_id"`
	KeyHash       string          `json:"-"`
	SubjectHash   string          `json:"-"`
	RequestHash   string          `json:"request_hash"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"-"`
	Status        string          `json:"status"`
	Attempts      int             `json:"attempts"`
	AvailableAt   time.Time       `json:"available_at"`
	LeaseUntil    time.Time       `json:"lease_until,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	ErrorCode     string          `json:"error_code,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	IdempotentHit bool            `json:"idempotent_hit,omitempty"`
}

func (o operation) terminal() bool {
	return o.Status == statusSucceeded || o.Status == statusFailed
}

type outboxEvent struct {
	OperationID string          `json:"operation_id"`
	Sequence    int             `json:"sequence"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

// doltReceipt is the legacy type name for the canonical Dolt record. Version 1
// represents only a successful legacy receipt; version 2 represents a terminal
// outcome and therefore carries an explicit succeeded/failed status.
type doltReceipt struct {
	Version      int             `json:"version"`
	OperationID  string          `json:"operation_id"`
	ProjectID    string          `json:"project_id"`
	KeyHash      string          `json:"key_hash"`
	SubjectHash  string          `json:"subject_hash"`
	RequestHash  string          `json:"request_hash"`
	Kind         string          `json:"kind"`
	Status       string          `json:"status,omitempty"`
	Result       json.RawMessage `json:"result"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	Outbox       outboxEvent     `json:"outbox"`
	CommittedAt  time.Time       `json:"committed_at"`
	Reconciled   bool            `json:"reconciled,omitempty"`
}

// labAttestation is stored inside the synthetic Dolt database and verified by
// the gateway before it will serve traffic. The HTTP signature is added only
// when returning the attestation to a benchmark client.
type labAttestation struct {
	Version      int    `json:"version"`
	Environment  string `json:"environment"`
	LabID        string `json:"lab_id"`
	DatabaseName string `json:"database_name"`
	ProjectID    string `json:"project_id"`
	Signature    string `json:"signature,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
