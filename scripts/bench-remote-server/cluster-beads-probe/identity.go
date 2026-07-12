package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/steveyegge/beads/internal/storage/uow"
)

const probeAttestationMetadataKey = "_beads_perf_lab_attestation_v1"

type probeLabAttestation struct {
	Version      int    `json:"version"`
	Environment  string `json:"environment"`
	LabID        string `json:"lab_id"`
	DatabaseName string `json:"database_name"`
	ProjectID    string `json:"project_id"`
}

func canonicalProbeUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func verifyProbeIdentity(ctx context.Context, unit uow.UnitOfWork, projectID, labID string) error {
	verified, err := unit.ConfigUseCase().VerifyInit(ctx)
	if err != nil {
		return fmt.Errorf("verify initialized database identity: %w", err)
	}
	if len(verified.Missing) != 0 {
		return fmt.Errorf("required initialized fields are missing: %s", strings.Join(verified.Missing, ","))
	}
	if verified.ProjectID != projectID {
		return fmt.Errorf("database project identity mismatch")
	}
	databaseRows, err := unit.RawSQLUseCase().Query(ctx, "SELECT DATABASE()")
	if err != nil {
		return fmt.Errorf("read selected database identity: %w", err)
	}
	actualDatabase, err := singleProbeString(databaseRows.Rows)
	if err != nil || actualDatabase != probeDatabase {
		return fmt.Errorf("selected database identity mismatch")
	}
	markerRows, err := unit.RawSQLUseCase().Query(ctx,
		"SELECT value FROM metadata WHERE `key` = ?", probeAttestationMetadataKey)
	if err != nil {
		return fmt.Errorf("read lab attestation marker: %w", err)
	}
	markerJSON, err := singleProbeString(markerRows.Rows)
	if err != nil {
		return fmt.Errorf("lab attestation marker is missing or malformed")
	}
	marker, err := decodeProbeAttestation(markerJSON)
	if err != nil {
		return err
	}
	return validateProbeAttestation(marker, projectID, labID)
}

func singleProbeString(rows [][]any) (string, error) {
	if len(rows) != 1 || len(rows[0]) != 1 {
		return "", fmt.Errorf("expected exactly one value")
	}
	switch value := rows[0][0].(type) {
	case string:
		if value != "" {
			return value, nil
		}
	case []byte:
		if len(value) != 0 {
			return string(value), nil
		}
	}
	return "", fmt.Errorf("expected a non-empty string")
}

func decodeProbeAttestation(value string) (probeLabAttestation, error) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(value)))
	decoder.DisallowUnknownFields()
	var marker probeLabAttestation
	if err := decoder.Decode(&marker); err != nil {
		return marker, fmt.Errorf("lab attestation marker is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return marker, fmt.Errorf("lab attestation marker has trailing data")
	}
	return marker, nil
}

func validateProbeAttestation(marker probeLabAttestation, projectID, labID string) error {
	if marker.Version != 1 || marker.Environment != "synthetic" || marker.LabID != labID ||
		marker.DatabaseName != probeDatabase || marker.ProjectID != projectID {
		return fmt.Errorf("lab attestation identity mismatch")
	}
	return nil
}
