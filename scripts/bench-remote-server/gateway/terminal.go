package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	terminalOperationIDDomain    = "beads-perf-terminal-operation-v2"
	terminalIdempotencyNamespace = "terminal-v2/"
	terminalWaitLimit            = 10 * time.Minute
)

var errTerminalRetryExhausted = errors.New("terminal retry budget exhausted")

func terminalIdempotencyKeyHash(stableKey []byte, key string) string {
	return terminalIdempotencyNamespace + hmacHex(stableKey, "key\x00terminal-v2\x00"+key)
}

func isTerminalIdempotencyNamespace(keyHash string) bool {
	return strings.HasPrefix(keyHash, terminalIdempotencyNamespace)
}

func (s *gatewayService) handleTerminalSubmit(w http.ResponseWriter, r *http.Request, subjectHash string) {
	in, ok := s.prepareOperationSubmission(w, r, subjectHash, true)
	if !ok {
		return
	}

	existing, err := s.queue.getByKey(r.Context(), in.ProjectID, in.KeyHash)
	localExists := err == nil
	localConflict := false
	switch {
	case err == nil:
		localConflict = !sameOperationIdentity(existing, in)
	case !errors.Is(err, sql.ErrNoRows):
		writeTerminalNoAcknowledgement(w, "queue_unavailable")
		return
	}

	receipt, err := s.backend.LookupReceipt(r.Context(), in)
	if err != nil {
		if errors.Is(err, errReceiptIdentityMismatch) {
			writeError(w, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
			return
		}
		writeTerminalNoAcknowledgement(w, "receipt_lookup_unavailable")
		return
	}
	if receipt != nil {
		if err := validateReceipt(*receipt, in); err != nil {
			if errors.Is(err, errReceiptIdentityMismatch) {
				writeError(w, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
				return
			}
			writeTerminalNoAcknowledgement(w, "receipt_validation_failed")
			return
		}
		if err := s.backend.VerifyTerminalDurability(r.Context(), in, receipt); err != nil {
			writeTerminalNoAcknowledgement(w, "cluster_ack_unavailable")
			return
		}
	}
	if receipt != nil {
		receipt.Reconciled = true
		op, err := s.queue.restoreCanonicalOutcome(r.Context(), in, receipt)
		if err != nil {
			writeTerminalNoAcknowledgement(w, "receipt_recovery_deferred")
			return
		}
		if !op.terminal() {
			writeTerminalNoAcknowledgement(w, "receipt_recovery_deferred")
			return
		}
		op.IdempotentHit = true
		writeJSON(w, http.StatusOK, op)
		return
	}
	if localConflict {
		writeError(w, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
		return
	}

	legacyHash := hmacHex(s.stableKey, "key\x00"+strings.TrimSpace(r.Header.Get("Idempotency-Key")))
	if _, legacyErr := s.queue.getByKey(r.Context(), in.ProjectID, legacyHash); legacyErr == nil {
		writeTerminalMigrationConflict(w)
		return
	} else if !errors.Is(legacyErr, sql.ErrNoRows) {
		writeTerminalNoAcknowledgement(w, "legacy_fence_unavailable")
		return
	}
	legacyExists, legacyErr := s.backend.LegacyOutcomeExists(r.Context(), in.ProjectID, legacyHash)
	if legacyErr != nil {
		writeTerminalNoAcknowledgement(w, "legacy_fence_unavailable")
		return
	}
	if legacyExists {
		writeTerminalMigrationConflict(w)
		return
	}

	if localExists {
		if existing.terminal() {
			// A queue-only terminal state is never authoritative.
			writeTerminalNoAcknowledgement(w, "canonical_outcome_missing")
			return
		}
		if existing.Status == statusUnknown && existing.ErrorCode == "retry_exhausted" {
			writeTerminalNoAcknowledgement(w, "retry_exhausted")
			return
		}
		existing.IdempotentHit = true
		s.signal()
		s.waitAndWriteTerminal(w, r, existing.ID)
		return
	}

	op, ok := s.admitPreparedOperation(w, r, in, true)
	if !ok {
		return
	}

	s.signal()
	s.waitAndWriteTerminal(w, r, op.ID)
}

func writeTerminalMigrationConflict(w http.ResponseWriter) {
	writeError(w, http.StatusConflict, "idempotency_migration_required",
		"idempotency key belongs to the legacy operations lane; use a new key or an explicit migration")
}

func (s *gatewayService) waitAndWriteTerminal(w http.ResponseWriter, r *http.Request, operationID string) {
	ctx, cancel := context.WithTimeout(r.Context(), terminalWaitLimit)
	defer cancel()
	op, err := s.waitForTerminal(ctx, operationID)
	if err != nil {
		if errors.Is(err, errTerminalRetryExhausted) {
			writeTerminalNoAcknowledgement(w, "retry_exhausted")
			return
		}
		writeTerminalNoAcknowledgement(w, "terminal_state_unavailable")
		return
	}
	receipt, err := s.backend.LookupReceipt(ctx, op)
	if err != nil || receipt == nil {
		writeTerminalNoAcknowledgement(w, "canonical_outcome_unavailable")
		return
	}
	if err := validateReceipt(*receipt, op); err != nil {
		writeTerminalNoAcknowledgement(w, "canonical_outcome_invalid")
		return
	}
	if err := s.backend.VerifyTerminalDurability(ctx, op, receipt); err != nil {
		writeTerminalNoAcknowledgement(w, "cluster_ack_unavailable")
		return
	}
	if err := s.queue.markOutcome(ctx, op, receipt); err != nil {
		writeTerminalNoAcknowledgement(w, "terminal_projection_invalid")
		return
	}
	op, err = s.queue.get(ctx, operationID)
	if err != nil || !op.terminal() {
		writeTerminalNoAcknowledgement(w, "terminal_projection_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, op)
}

func (s *gatewayService) waitForTerminal(ctx context.Context, operationID string) (operation, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		op, err := s.queue.get(ctx, operationID)
		if err != nil {
			return operation{}, err
		}
		if op.terminal() {
			return op, nil
		}
		if op.Status == statusUnknown && op.ErrorCode == "retry_exhausted" {
			return op, errTerminalRetryExhausted
		}
		select {
		case <-ctx.Done():
			return operation{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeTerminalNoAcknowledgement(w http.ResponseWriter, code string) {
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, code,
		"terminal state not acknowledged; retry with the same idempotency key and body")
}

func deterministicTerminalOperationID(stableKey []byte, projectID, subjectHash, keyHash string) string {
	message := bytes.Join([][]byte{
		[]byte(terminalOperationIDDomain), []byte(projectID), []byte(subjectHash), []byte(keyHash),
	}, []byte{0})
	mac := hmac.New(sha256.New, stableKey)
	_, _ = mac.Write(message)
	sum := mac.Sum(nil)
	var id uuid.UUID
	copy(id[:], sum[:len(id)])
	// UUIDv8 carries application-defined deterministic bits while preserving
	// the RFC 4122 variant and an opaque operation-ID shape.
	id[6] = (id[6] & 0x0f) | 0x80
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}
