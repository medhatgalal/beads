package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxRequestBytes = 5 << 20

func (s *gatewayService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.beginHandler() {
		writeError(w, http.StatusServiceUnavailable, "gateway_draining", "gateway is draining")
		return
	}
	defer s.endHandler()
	// The process liveness probe is constant-time and must remain available even
	// when terminal callers are holding every bounded work slot during an outage.
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	requestSlots := s.requests
	if isControlRequest(r) {
		requestSlots = s.controlRequests
	}
	select {
	case requestSlots <- struct{}{}:
		defer func() { <-requestSlots }()
	default:
		if isTerminalOperationRequest(r) {
			writeTerminalNoAcknowledgement(w, "request_limit")
			return
		}
		if isControlRequest(r) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "control_limit", "gateway control request limit reached")
			return
		}
		writeError(w, http.StatusTooManyRequests, "request_limit", "gateway request limit reached")
		return
	}
	subjectHash, ok := s.authorize(w, r)
	if !ok {
		return
	}
	parts := splitPath(r.URL.Path)
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "projects" {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	projectID := parts[2]
	if projectID != s.projectID || r.Header.Get("X-Beads-Project-ID") != s.projectID {
		writeError(w, http.StatusForbidden, "project_mismatch", "project identity mismatch")
		return
	}
	rest := parts[3:]
	switch {
	case len(rest) == 1 && rest[0] == "attestation" && r.Method == http.MethodGet:
		attestation, err := s.backend.Attestation(r.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "attestation_failed", sanitizeError(err))
			return
		}
		attestation.Signature = signLabAttestation(s.token, attestation)
		writeJSON(w, http.StatusOK, attestation)
	case len(rest) == 1 && rest[0] == "ping" && r.Method == http.MethodGet:
		s.handleRead(w, r, s.backend.Ping)
	case len(rest) == 1 && rest[0] == "issues" && r.Method == http.MethodGet:
		limit, valid := parseLimit(w, r)
		if valid {
			s.handleRead(w, r, func(ctx context.Context) (any, error) { return s.backend.List(ctx, limit) })
		}
	case len(rest) == 2 && rest[0] == "issues" && r.Method == http.MethodGet:
		id := rest[1]
		s.handleRead(w, r, func(ctx context.Context) (any, error) { return s.backend.Show(ctx, id) })
	case len(rest) == 1 && rest[0] == "ready" && r.Method == http.MethodGet:
		limit, valid := parseLimit(w, r)
		if valid {
			s.handleRead(w, r, func(ctx context.Context) (any, error) { return s.backend.Ready(ctx, limit) })
		}
	case len(rest) == 1 && rest[0] == "pool" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.backend.PoolStats())
	case len(rest) == 1 && rest[0] == "readyz" && r.Method == http.MethodGet:
		if err := s.queue.quickCheck(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "queue_unhealthy", sanitizeError(err))
			return
		}
		ping, err := s.backend.Ping(r.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "backend_unhealthy", sanitizeError(err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "backend": ping, "pool": s.backend.PoolStats()})
	case len(rest) == 1 && rest[0] == "operations" && r.Method == http.MethodPost:
		if !s.allowLegacyOperations {
			writeError(w, http.StatusGone, "legacy_lane_disabled", "use terminal-operations")
			return
		}
		s.handleSubmit(w, r, subjectHash)
	case len(rest) == 1 && rest[0] == "terminal-operations" && r.Method == http.MethodPost:
		s.handleTerminalSubmit(w, r, subjectHash)
	case len(rest) == 2 && rest[0] == "operations" && r.Method == http.MethodGet:
		s.handleStatus(w, r, rest[1], subjectHash)
	case len(rest) == 3 && rest[0] == "operations" && rest[2] == "outbox" && r.Method == http.MethodGet:
		s.handleOutbox(w, r, rest[1], subjectHash)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (s *gatewayService) authorize(w http.ResponseWriter, r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		writeError(w, http.StatusUnauthorized, "unauthorized", "authorization required")
		return "", false
	}
	provided := []byte(strings.TrimPrefix(header, "Bearer "))
	if len(provided) != len(s.token) || subtle.ConstantTimeCompare(provided, s.token) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized", "authorization failed")
		return "", false
	}
	if asserted := r.Header.Get("X-Beads-Subject"); asserted != "" && asserted != s.subject {
		writeError(w, http.StatusForbidden, "identity_mismatch", "subject identity mismatch")
		return "", false
	}
	return s.subjectHash, true
}

func (s *gatewayService) handleRead(w http.ResponseWriter, r *http.Request, fn func(context.Context) (any, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := fn(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read_failed", sanitizeError(err))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *gatewayService) handleSubmit(w http.ResponseWriter, r *http.Request, subjectHash string) {
	in, ok := s.prepareOperationSubmission(w, r, subjectHash, false)
	if !ok {
		return
	}
	op, ok := s.admitPreparedOperation(w, r, in, false)
	if !ok {
		return
	}
	s.signal()
	wait := parseWait(r.URL.Query().Get("wait_ms"))
	if wait > 0 && !op.terminal() {
		deadline := time.NewTimer(wait)
		defer deadline.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for !op.terminal() {
			select {
			case <-r.Context().Done():
				writeError(w, http.StatusRequestTimeout, "request_canceled", "request canceled")
				return
			case <-deadline.C:
				writeJSON(w, http.StatusAccepted, op)
				return
			case <-ticker.C:
				fresh, err := s.queue.get(r.Context(), op.ID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "queue_failed", sanitizeError(err))
					return
				}
				op = fresh
			}
		}
	}
	status := http.StatusAccepted
	if op.terminal() {
		status = http.StatusOK
	}
	writeJSON(w, status, op)
}

func (s *gatewayService) prepareOperationSubmission(
	w http.ResponseWriter, r *http.Request, subjectHash string, deterministicID bool,
) (operation, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) < 8 || len(key) > 200 || strings.ContainsAny(key, "\r\n\x00") {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must be 8-200 characters")
		return operation{}, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var request operationRequest
	if err := dec.Decode(&request); err != nil {
		if deterministicID && r.Context().Err() != nil {
			writeTerminalNoAcknowledgement(w, "request_canceled")
			return operation{}, false
		}
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid operation request")
		return operation{}, false
	}
	if err := requireJSONEOF(dec); err != nil {
		if deterministicID && r.Context().Err() != nil {
			writeTerminalNoAcknowledgement(w, "request_canceled")
			return operation{}, false
		}
		writeError(w, http.StatusBadRequest, "invalid_request", "operation request has trailing data")
		return operation{}, false
	}
	if !allowedOperationKind(request.Kind) {
		writeError(w, http.StatusBadRequest, "invalid_kind", "unsupported operation kind")
		return operation{}, false
	}
	canonical, err := canonicalJSON(request.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", "payload must be valid JSON")
		return operation{}, false
	}
	requestHash := digestHex(bytes.Join([][]byte{[]byte(s.projectID), []byte(request.Kind), canonical}, []byte{0}))
	keyHash := hmacHex(s.stableKey, "key\x00"+key)
	if deterministicID {
		keyHash = terminalIdempotencyKeyHash(s.stableKey, key)
	}
	in := operation{
		ProjectID:   s.projectID,
		KeyHash:     keyHash,
		SubjectHash: subjectHash,
		RequestHash: requestHash,
		Kind:        request.Kind,
		Payload:     canonical,
	}
	if deterministicID {
		in.ID = deterministicTerminalOperationID(s.stableKey, s.projectID, subjectHash, keyHash)
	}
	return in, true
}

func (s *gatewayService) admitPreparedOperation(
	w http.ResponseWriter, r *http.Request, in operation, terminalOnly bool,
) (operation, bool) {
	op, err := s.queue.admit(r.Context(), in)
	if err != nil && terminalOnly && r.Context().Err() != nil {
		writeTerminalNoAcknowledgement(w, "request_canceled")
		return operation{}, false
	}
	if errors.Is(err, errQueueFull) {
		if terminalOnly {
			writeTerminalNoAcknowledgement(w, "queue_full")
			return operation{}, false
		}
		writeError(w, http.StatusTooManyRequests, "queue_full", err.Error())
		return operation{}, false
	}
	if errors.Is(err, errIdempotencyConflict) {
		writeError(w, http.StatusConflict, "idempotency_conflict", err.Error())
		return operation{}, false
	}
	if err != nil {
		if terminalOnly {
			writeTerminalNoAcknowledgement(w, "queue_admission_ambiguous")
			return operation{}, false
		}
		writeError(w, http.StatusInternalServerError, "queue_failed", sanitizeError(err))
		return operation{}, false
	}
	return op, true
}

func isTerminalOperationRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	parts := splitPath(r.URL.Path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "terminal-operations"
}

func isControlRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	parts := splitPath(r.URL.Path)
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "projects" {
		return false
	}
	switch parts[3] {
	case "attestation", "pool", "readyz":
		return true
	default:
		return false
	}
}

func (s *gatewayService) handleStatus(w http.ResponseWriter, r *http.Request, id, subjectHash string) {
	op, err := s.queue.get(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "operation not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "queue_failed", sanitizeError(err))
		return
	}
	if op.ProjectID != s.projectID || op.SubjectHash != subjectHash {
		writeError(w, http.StatusNotFound, "not_found", "operation not found")
		return
	}
	writeJSON(w, http.StatusOK, op)
}

func (s *gatewayService) handleOutbox(w http.ResponseWriter, r *http.Request, id, subjectHash string) {
	op, err := s.queue.get(r.Context(), id)
	if err != nil || op.ProjectID != s.projectID || op.SubjectHash != subjectHash {
		writeError(w, http.StatusNotFound, "not_found", "operation not found")
		return
	}
	events, err := s.queue.outbox(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "queue_failed", sanitizeError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 500 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 500")
			return 0, false
		}
		limit = value
	}
	return limit, true
}

func parseWait(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return 0
	}
	if ms > 30000 {
		ms = 30000
	}
	return time.Duration(ms) * time.Millisecond
}

func allowedOperationKind(kind string) bool {
	switch kind {
	case "issue.create", "issue.update", "issue.close", "issue.claim", "batch.apply", "graph.apply":
		return true
	default:
		return false
	}
}

func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
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

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("trailing JSON")
	}
	return err
}

func hmacHex(key []byte, value string) string {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}

func signLabAttestation(key []byte, attestation labAttestation) string {
	return hmacHex(key, strings.Join([]string{
		"beads-perf-attestation-v1",
		attestation.Environment,
		attestation.LabID,
		attestation.DatabaseName,
		attestation.ProjectID,
	}, "\x00"))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Error: message})
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	return "internal operation failed"
}
