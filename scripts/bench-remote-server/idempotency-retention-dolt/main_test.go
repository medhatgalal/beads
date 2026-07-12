package main

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestServerControlIdentityIsExactAndFailClosed(t *testing.T) {
	tests := []struct {
		name        string
		count       int
		environment string
		identity    string
		queryErr    error
		wantError   bool
	}{
		{name: "exact", count: 1, environment: "synthetic", identity: labID},
		{name: "absent", count: 0, wantError: true},
		{name: "multiple", count: 2, environment: "synthetic", identity: labID, wantError: true},
		{name: "wrong environment", count: 1, environment: "production", identity: labID, wantError: true},
		{name: "wrong lab", count: 1, environment: "synthetic", identity: "11111111-1111-4111-8111-111111111111", wantError: true},
		{name: "missing table", queryErr: errors.New("table not found"), wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectation := mock.ExpectQuery(regexp.QuoteMeta(retentionLabControlIdentityQuery))
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(test.count, test.environment, test.identity))
			}
			err = verifyServerControlIdentityOnDB(context.Background(), db)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v want_error=%v", err, test.wantError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVerificationRingRotationAndResigningPreserveIdentity(t *testing.T) {
	ring, err := ringAt(1)
	if err != nil {
		t.Fatal(err)
	}
	request, err := newRequest("11111111-1111-4111-8111-111111111111", "p-compact", 1, 1, 1, operationPrefix+" key test", ring)
	if err != nil {
		t.Fatal(err)
	}
	originalHash, originalOperation := request.RequestHash, request.OperationID
	if err := ring.verify(request); err != nil {
		t.Fatal(err)
	}
	for epoch := uint64(2); epoch <= 4; epoch++ {
		if err := ring.rotate(epoch); err != nil {
			t.Fatal(err)
		}
	}
	if err := ring.verify(request); !errors.Is(err, errVerification) {
		t.Fatalf("retired signature err=%v", err)
	}
	request, err = ring.sign(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.verify(request); err != nil {
		t.Fatal(err)
	}
	if request.RequestHash != originalHash || request.OperationID != originalOperation || request.KeyEpoch != 4 {
		t.Fatalf("resigning changed idempotency identity: %+v", request)
	}
}

func TestRequestAndEndpointIsolationGuards(t *testing.T) {
	ring, _ := ringAt(1)
	request, err := newRequest("11111111-1111-4111-8111-111111111111", "p-same", 1, 1, 1, operationPrefix+" valid", ring)
	if err != nil || validateRequest(request) != nil {
		t.Fatalf("valid request err=%v validate=%v", err, validateRequest(request))
	}
	tampered := request
	tampered.ProducerID = "production"
	if err := validateRequest(tampered); err == nil {
		t.Fatal("unknown producer passed validation")
	}
	tampered = request
	tampered.Payload = "not synthetic"
	if err := validateRequest(tampered); err == nil {
		t.Fatal("non-synthetic payload passed validation")
	}
	if db, err := openDB("production", 1); err == nil {
		_ = db.Close()
		t.Fatal("connector accepted a foreign database")
	}
	if err := writeJSON("/private/tmp/outside-retention-evidence.json", map[string]bool{"bad": true}); err == nil {
		t.Fatal("writer accepted an output outside the evidence root")
	}
}

func TestRetryClassificationIsNarrow(t *testing.T) {
	serialization, duplicate := isRetryable(&mysql.MySQLError{Number: 1213})
	if !serialization || duplicate {
		t.Fatal("1213 was not classified as serialization")
	}
	serialization, duplicate = isRetryable(&mysql.MySQLError{Number: 1205})
	if !serialization || duplicate {
		t.Fatal("1205 was not classified as serialization")
	}
	serialization, duplicate = isRetryable(&mysql.MySQLError{Number: 1062})
	if serialization || !duplicate {
		t.Fatal("1062 was not classified as duplicate convergence")
	}
	serialization, duplicate = isRetryable(&mysql.MySQLError{Number: 1045})
	if serialization || duplicate {
		t.Fatal("authentication error was classified as retryable")
	}
}

func TestLedgerPolicyUsesStoredLimitsInsideBinaryCeilings(t *testing.T) {
	valid := ledgerState{
		RepositoryEpoch: 1,
		MaxProducers:    proofProducerRows,
		MaxReceipts:     proofReceiptRows,
		MaxOutcomeBytes: proofOutcomeBytes,
		ProducerCount:   proofProducerRows,
		ReceiptCount:    proofReceiptRows,
		Version:         9,
	}
	if err := validateLedgerState(valid); err != nil {
		t.Fatalf("valid at-cap ledger rejected: %v", err)
	}
	tests := map[string]ledgerState{
		"producer binary ceiling":   func() ledgerState { s := valid; s.MaxProducers = maxProducerRows + 1; return s }(),
		"receipt binary ceiling":    func() ledgerState { s := valid; s.MaxReceipts = maxReceiptRows + 1; return s }(),
		"outcome binary ceiling":    func() ledgerState { s := valid; s.MaxOutcomeBytes = maxOutcomeBytes + 1; return s }(),
		"producer counter overflow": func() ledgerState { s := valid; s.ProducerCount = s.MaxProducers + 1; return s }(),
		"receipt counter overflow":  func() ledgerState { s := valid; s.ReceiptCount = s.MaxReceipts + 1; return s }(),
		"negative producer counter": func() ledgerState { s := valid; s.ProducerCount = -1; return s }(),
		"zero version":              func() ledgerState { s := valid; s.Version = 0; return s }(),
	}
	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateLedgerState(state); err == nil {
				t.Fatal("invalid ledger policy was accepted")
			}
		})
	}
}

func TestProofCapsCreateOneExactBoundarySlot(t *testing.T) {
	if got, want := len(initialProofProducers), proofProducerRows-1; got != want {
		t.Fatalf("initial producers=%d want=%d", got, want)
	}
	if proofProducerRows > maxProducerRows || proofReceiptRows > maxReceiptRows || proofOutcomeBytes > maxOutcomeBytes {
		t.Fatal("proof policy exceeds binary ceiling")
	}
	if !isProofProducer("p-cap-a") || !isProofProducer("p-cap-b") || !isProofProducer("p-cap-r1") || !isProofProducer("p-cap-r2") {
		t.Fatal("capacity-race identities are not fixed proof producers")
	}
}

func TestPayloadHashSignatureAndOperationIdentityAreCanonicallyBound(t *testing.T) {
	ring, err := ringAt(1)
	if err != nil {
		t.Fatal(err)
	}
	original, err := newRequest("11111111-1111-4111-8111-111111111111", "p-same", 1, 1, 1, operationPrefix+" original", ring)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRequest(original); err != nil {
		t.Fatalf("canonical request rejected: %v", err)
	}
	if err := ring.verify(original); err != nil {
		t.Fatalf("canonical signature rejected: %v", err)
	}

	t.Run("payload changed without resigning", func(t *testing.T) {
		tampered := original
		tampered.Payload = operationPrefix + " altered"
		if err := validateRequest(tampered); err == nil {
			t.Fatal("payload changed without matching request hash passed validation")
		}
		if err := ring.verify(tampered); !errors.Is(err, errVerification) {
			t.Fatalf("payload omitted from signature: %v", err)
		}
	})

	t.Run("payload and canonical IDs changed without resigning", func(t *testing.T) {
		tampered := original
		tampered.Payload = operationPrefix + " altered with canonical identity"
		tampered.RequestHash = canonicalRequestHash(tampered.Payload)
		tampered.OperationID = canonicalOperationID(tampered, tampered.RequestHash)
		if err := validateRequest(tampered); err != nil {
			t.Fatalf("canonical altered request should pass structural validation: %v", err)
		}
		if err := ring.verify(tampered); !errors.Is(err, errVerification) {
			t.Fatalf("altered canonical payload retained old signature: %v", err)
		}
	})

	t.Run("signed supplied digest cannot disagree with payload", func(t *testing.T) {
		tampered := original
		tampered.RequestHash = canonicalRequestHash(operationPrefix + " unrelated")
		tampered.OperationID = canonicalOperationID(tampered, tampered.RequestHash)
		tampered, err = ring.sign(tampered)
		if err != nil {
			t.Fatal(err)
		}
		if err := ring.verify(tampered); err != nil {
			t.Fatalf("test precondition invalid signature: %v", err)
		}
		if err := validateRequest(tampered); err == nil {
			t.Fatal("valid signature over a supplied noncanonical digest bypassed server recomputation")
		}
	})

	t.Run("signed operation ID must be canonical", func(t *testing.T) {
		tampered := original
		tampered.OperationID = "ret-00000000000000000000000000000000"
		tampered, err = ring.sign(tampered)
		if err != nil {
			t.Fatal(err)
		}
		if err := ring.verify(tampered); err != nil {
			t.Fatalf("test precondition invalid signature: %v", err)
		}
		if err := validateRequest(tampered); err == nil {
			t.Fatal("signed noncanonical operation ID passed validation")
		}
	})
}
