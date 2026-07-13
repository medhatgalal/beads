package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	attestationVersion     = 1
	attestationEnvironment = "synthetic"
	attestationDomain      = "beads-perf-attestation-v1"
)

type attestationResponse struct {
	Version      int    `json:"version"`
	Environment  string `json:"environment"`
	LabID        string `json:"lab_id"`
	DatabaseName string `json:"database_name"`
	ProjectID    string `json:"project_id"`
	Signature    string `json:"signature"`
}

type attestationInventory struct {
	Required int       `json:"required"`
	Verified int       `json:"verified"`
	At       time.Time `json:"at"`
}

func attestTargets(ctx context.Context, targets []*runtimeTarget) (attestationInventory, error) {
	inventory := attestationInventory{Required: len(targets), At: time.Now().UTC()}
	for _, target := range targets {
		if err := attestTarget(ctx, target); err != nil {
			return inventory, fmt.Errorf("attest %s: %w", target.cfg.DatabaseID, err)
		}
		inventory.Verified++
	}
	return inventory, nil
}

func attestTarget(ctx context.Context, target *runtimeTarget) error {
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	endpoint := strings.TrimSuffix(target.base.String(), "/") + "/v1/projects/" +
		url.PathEscape(target.cfg.ProjectID) + "/attestation"
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+string(target.token))
	request.Header.Set("X-Beads-Project-ID", target.cfg.ProjectID)
	response, err := target.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var attestation attestationResponse
	if err := dec.Decode(&attestation); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("decode: trailing JSON")
	}
	if attestation.Version != attestationVersion || attestation.Environment != attestationEnvironment {
		return fmt.Errorf("wrong synthetic attestation version or environment")
	}
	if attestation.LabID != target.cfg.LabID ||
		attestation.DatabaseName != target.cfg.DatabaseID ||
		attestation.ProjectID != target.cfg.ProjectID {
		return fmt.Errorf("attested identity does not match configured target")
	}
	provided, err := hex.DecodeString(attestation.Signature)
	if err != nil || len(provided) != sha256.Size {
		return fmt.Errorf("invalid signature encoding")
	}
	expected := attestationSignature(target.token, attestation)
	if !hmac.Equal(provided, expected) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func attestationSignature(token []byte, value attestationResponse) []byte {
	message := bytes.Join([][]byte{
		[]byte(attestationDomain),
		[]byte(value.Environment),
		[]byte(value.LabID),
		[]byte(value.DatabaseName),
		[]byte(value.ProjectID),
	}, []byte{0})
	mac := hmac.New(sha256.New, token)
	_, _ = mac.Write(message)
	return mac.Sum(nil)
}
