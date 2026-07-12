// gateway-client is a guarded HTTP benchmark driver for the disposable
// near-data gateway prototype. It never accepts a non-loopback endpoint.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxResponseBytes = 8 << 20

const operationPollInterval = 25 * time.Millisecond

type config struct {
	BaseURL         string
	ProjectID       string
	TokenFile       string
	TokenStdin      bool
	Method          string
	Resource        string
	BodyFile        string
	KeyTemplate     string
	Warmups         int
	Samples         int
	SimulatedRTTMS  int
	ConnectionMode  string
	Timeout         time.Duration
	IncludeResponse bool
	Concurrency     int
}

type sample struct {
	Phase          string          `json:"phase"`
	Ordinal        int             `json:"ordinal"`
	WallMS         float64         `json:"wall_ms"`
	HTTPStatus     int             `json:"http_status"`
	ResponseBytes  int             `json:"response_bytes"`
	ResponseSHA256 string          `json:"response_sha256"`
	JSONValid      bool            `json:"json_valid"`
	Passed         bool            `json:"passed"`
	Error          string          `json:"error,omitempty"`
	Response       json.RawMessage `json:"response,omitempty"`
}

type report struct {
	SchemaVersion  int       `json:"schema_version"`
	GeneratedAt    time.Time `json:"generated_at"`
	Topology       string    `json:"topology"`
	DelayModel     string    `json:"delay_model"`
	Method         string    `json:"method"`
	Resource       string    `json:"resource"`
	Protocol       string    `json:"protocol"`
	ConnectionMode string    `json:"connection_mode"`
	Concurrency    int       `json:"concurrency"`
	SimulatedRTTMS int       `json:"simulated_rtt_ms"`
	Warmups        int       `json:"warmups"`
	Samples        int       `json:"samples"`
	Passed         bool      `json:"passed"`
	P50MS          float64   `json:"p50_ms"`
	P95MS          float64   `json:"p95_ms"`
	P99MS          float64   `json:"p99_ms"`
	Records        []sample  `json:"records"`
}

type operationState struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
}

type wireResponse struct {
	Status int
	Body   []byte
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fatal(err)
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fatal(err)
	}
	if !result.Passed {
		os.Exit(1)
	}
}

func parseFlags() (config, error) {
	var cfg config
	flag.StringVar(&cfg.BaseURL, "base-url", "http://127.0.0.1:7707", "numeric loopback gateway URL")
	flag.StringVar(&cfg.ProjectID, "project-id", "", "expected lab project UUID")
	flag.StringVar(&cfg.TokenFile, "token-file", "", "0600 bearer token file")
	flag.BoolVar(&cfg.TokenStdin, "token-stdin", false, "read bearer token from stdin without local persistence")
	flag.StringVar(&cfg.Method, "method", http.MethodGet, "GET or POST")
	flag.StringVar(&cfg.Resource, "resource", "ping", "project-relative resource and optional query")
	flag.StringVar(&cfg.BodyFile, "body-file", "", "optional JSON request body template")
	flag.StringVar(&cfg.KeyTemplate, "idempotency-key", "", "POST idempotency key template; {{ordinal}} is replaced")
	flag.IntVar(&cfg.Warmups, "warmups", 1, "warmup request count")
	flag.IntVar(&cfg.Samples, "samples", 20, "measured request count")
	flag.IntVar(&cfg.SimulatedRTTMS, "simulated-rtt-ms", 0, "client-visible one-request RTT delay")
	flag.StringVar(&cfg.ConnectionMode, "connection-mode", "warm", "warm or cold HTTP connections")
	flag.DurationVar(&cfg.Timeout, "timeout", 11*time.Minute, "per-request timeout")
	flag.BoolVar(&cfg.IncludeResponse, "include-response", false, "include response JSON for functional checks")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "maximum concurrent measured requests")
	flag.Parse()
	if flag.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments")
	}
	if cfg.ProjectID == "" || (cfg.TokenFile == "") == !cfg.TokenStdin {
		return config{}, fmt.Errorf("project-id and exactly one of token-file or token-stdin are required")
	}
	if cfg.Method != http.MethodGet && cfg.Method != http.MethodPost {
		return config{}, fmt.Errorf("method must be GET or POST")
	}
	if cfg.Warmups < 0 || cfg.Warmups > 20 || cfg.Samples < 1 || cfg.Samples > 200 {
		return config{}, fmt.Errorf("warmups or samples outside safe bounds")
	}
	if cfg.SimulatedRTTMS < 0 || cfg.SimulatedRTTMS > 2000 {
		return config{}, fmt.Errorf("simulated RTT outside safe bounds")
	}
	if cfg.ConnectionMode != "warm" && cfg.ConnectionMode != "cold" {
		return config{}, fmt.Errorf("connection-mode must be warm or cold")
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 32 || cfg.Concurrency > cfg.Samples {
		return config{}, fmt.Errorf("concurrency must be between 1 and samples, with a maximum of 32")
	}
	if cfg.Timeout <= 0 || cfg.Timeout > 15*time.Minute {
		return config{}, fmt.Errorf("timeout outside safe bounds")
	}
	if strings.Contains(cfg.Resource, "..") || strings.HasPrefix(cfg.Resource, "/") {
		return config{}, fmt.Errorf("resource must be project-relative")
	}
	if cfg.Method == http.MethodPost && (cfg.BodyFile == "" || cfg.KeyTemplate == "") {
		return config{}, fmt.Errorf("POST requires body-file and idempotency-key")
	}
	return cfg, nil
}

func run(ctx context.Context, cfg config) (report, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Scheme != "http" || base.User != nil || base.Path != "" || base.RawQuery != "" {
		return report{}, fmt.Errorf("base-url must be a plain loopback HTTP origin")
	}
	host := net.ParseIP(base.Hostname())
	if host == nil || !host.IsLoopback() || base.Port() == "" {
		return report{}, fmt.Errorf("base-url must use a numeric loopback host and explicit port")
	}
	var token []byte
	if cfg.TokenStdin {
		token, err = readSecretReader(io.LimitReader(os.Stdin, 4097))
	} else {
		token, err = readSecret(cfg.TokenFile)
	}
	if err != nil {
		return report{}, err
	}
	defer zero(token)
	var bodyTemplate []byte
	if cfg.BodyFile != "" {
		bodyTemplate, err = os.ReadFile(cfg.BodyFile)
		if err != nil {
			return report{}, fmt.Errorf("read body template: %w", err)
		}
		if !json.Valid(bodyTemplate) {
			return report{}, fmt.Errorf("body template must be valid JSON before ordinal substitution")
		}
	}

	protocol := "http-200-json-v1"
	if cfg.Method == http.MethodPost {
		protocol = "gateway-terminal-submit-poll-v1"
	}
	result := report{
		SchemaVersion: 2, GeneratedAt: time.Now().UTC(),
		Topology: "near-data-loopback-http", DelayModel: "per-http-request-client-delay",
		Method: cfg.Method, Resource: cfg.Resource, Protocol: protocol, ConnectionMode: cfg.ConnectionMode,
		Concurrency:    cfg.Concurrency,
		SimulatedRTTMS: cfg.SimulatedRTTMS, Warmups: cfg.Warmups, Samples: cfg.Samples, Passed: true,
	}
	warmClient := newClient(cfg.Timeout)
	defer warmClient.CloseIdleConnections()
	for ordinal := 0; ordinal < cfg.Warmups; ordinal++ {
		client := warmClient
		if cfg.ConnectionMode == "cold" {
			client = newClient(cfg.Timeout)
		}
		record := execute(ctx, client, cfg, base, token, bodyTemplate, "warmup", ordinal)
		if cfg.ConnectionMode == "cold" {
			client.CloseIdleConnections()
		}
		result.Records = append(result.Records, record)
	}
	records := make(chan sample, cfg.Samples)
	semaphore := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	for ordinal := 0; ordinal < cfg.Samples; ordinal++ {
		wg.Add(1)
		go func(ordinal int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			client := warmClient
			if cfg.ConnectionMode == "cold" {
				client = newClient(cfg.Timeout)
				defer client.CloseIdleConnections()
			}
			records <- execute(ctx, client, cfg, base, token, bodyTemplate, "sample", ordinal)
		}(ordinal)
	}
	wg.Wait()
	close(records)
	var measured []sample
	for record := range records {
		measured = append(measured, record)
		if !record.Passed {
			result.Passed = false
		}
	}
	sort.Slice(measured, func(i, j int) bool { return measured[i].Ordinal < measured[j].Ordinal })
	result.Records = append(result.Records, measured...)
	var walls []float64
	for _, record := range result.Records {
		if record.Phase == "sample" && record.Passed {
			walls = append(walls, record.WallMS)
		}
	}
	if len(walls) != cfg.Samples {
		result.Passed = false
	}
	sort.Float64s(walls)
	result.P50MS = percentile(walls, 0.50)
	result.P95MS = percentile(walls, 0.95)
	result.P99MS = percentile(walls, 0.99)
	return result, nil
}

func execute(ctx context.Context, client *http.Client, cfg config, base *url.URL, token, bodyTemplate []byte, phase string, ordinal int) (record sample) {
	ordinalText := strconv.Itoa(ordinal)
	resource := strings.ReplaceAll(cfg.Resource, "{{ordinal}}", ordinalText)
	body := bytes.ReplaceAll(bodyTemplate, []byte("{{ordinal}}"), []byte(ordinalText))
	target := strings.TrimSuffix(base.String(), "/") + "/v1/projects/" + url.PathEscape(cfg.ProjectID) + "/" + resource
	record = sample{Phase: phase, Ordinal: ordinal}
	delay := time.Duration(cfg.SimulatedRTTMS) * time.Millisecond
	started := time.Now()
	defer func() {
		record.WallMS = float64(time.Since(started).Microseconds()) / 1000
	}()
	requestCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	key := ""
	if cfg.Method == http.MethodPost {
		key = strings.ReplaceAll(cfg.KeyTemplate, "{{ordinal}}", ordinalText)
	}
	wire, err := performRequest(requestCtx, client, cfg.Method, target, token, cfg.ProjectID, body, key, delay)
	if err != nil {
		record.Error = err.Error()
		return record
	}
	if cfg.Method == http.MethodPost {
		wire, err = awaitOperation(requestCtx, client, base, token, cfg.ProjectID, delay, wire)
	}
	populateRecord(&record, wire, cfg.IncludeResponse)
	if err != nil {
		record.Error = err.Error()
		return record
	}
	if cfg.Method == http.MethodGet && wire.Status != http.StatusOK {
		record.Error = fmt.Sprintf("unexpected HTTP status %d", wire.Status)
		return record
	}
	if !record.JSONValid {
		record.Error = "response is not JSON"
		return record
	}
	record.Passed = true
	return record
}

func performRequest(ctx context.Context, client *http.Client, method, target string, token []byte, projectID string, body []byte, idempotencyKey string, delay time.Duration) (wireResponse, error) {
	var reader io.Reader
	if method == http.MethodPost {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return wireResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+string(token))
	request.Header.Set("X-Beads-Project-ID", projectID)
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if err := sleepHalf(ctx, delay, true); err != nil {
		return wireResponse{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return wireResponse{}, err
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	closeErr := response.Body.Close()
	wire := wireResponse{Status: response.StatusCode, Body: responseBody}
	if readErr != nil {
		return wire, readErr
	}
	if closeErr != nil {
		return wire, closeErr
	}
	if len(responseBody) > maxResponseBytes {
		return wire, fmt.Errorf("response exceeded size limit")
	}
	if err := sleepHalf(ctx, delay, false); err != nil {
		return wire, err
	}
	return wire, nil
}

func awaitOperation(ctx context.Context, client *http.Client, base *url.URL, token []byte, projectID string, delay time.Duration, initial wireResponse) (wireResponse, error) {
	state, err := decodeOperationState(initial.Body)
	if err != nil {
		return initial, fmt.Errorf("invalid submit response: %w", err)
	}
	if err := validateOperationIdentity(state, projectID, ""); err != nil {
		return initial, fmt.Errorf("invalid submit response: %w", err)
	}
	operationID := state.ID
	switch initial.Status {
	case http.StatusOK:
		return classifyTerminalOperation(initial, state)
	case http.StatusAccepted:
		if !operationPending(state.Status) {
			return initial, fmt.Errorf("submit returned HTTP 202 with non-pending status %q", state.Status)
		}
	default:
		return initial, fmt.Errorf("unexpected submit HTTP status %d", initial.Status)
	}

	statusTarget := strings.TrimSuffix(base.String(), "/") + "/v1/projects/" + url.PathEscape(projectID) + "/operations/" + url.PathEscape(state.ID)
	for {
		if err := waitForPoll(ctx); err != nil {
			return initial, err
		}
		current, err := performRequest(ctx, client, http.MethodGet, statusTarget, token, projectID, nil, "", delay)
		if err != nil {
			return current, fmt.Errorf("poll operation status: %w", err)
		}
		if current.Status != http.StatusOK {
			return current, fmt.Errorf("unexpected status-poll HTTP status %d", current.Status)
		}
		state, err = decodeOperationState(current.Body)
		if err != nil {
			return current, fmt.Errorf("invalid status response: %w", err)
		}
		if err := validateOperationIdentity(state, projectID, operationID); err != nil {
			return current, fmt.Errorf("invalid status response: %w", err)
		}
		if operationPending(state.Status) {
			initial = current
			continue
		}
		return classifyTerminalOperation(current, state)
	}
}

func decodeOperationState(body []byte) (operationState, error) {
	var state operationState
	if err := json.Unmarshal(body, &state); err != nil {
		return operationState{}, err
	}
	if state.ID == "" || state.ProjectID == "" || state.Status == "" {
		return operationState{}, fmt.Errorf("operation id, project id, and status are required")
	}
	return state, nil
}

func validateOperationIdentity(state operationState, projectID, operationID string) error {
	if state.ProjectID != projectID {
		return fmt.Errorf("operation project identity mismatch")
	}
	if operationID != "" && state.ID != operationID {
		return fmt.Errorf("operation identity changed while polling")
	}
	return nil
}

func operationPending(status string) bool {
	switch status {
	case "accepted", "running", "retry_wait", "unknown":
		return true
	default:
		return false
	}
}

func classifyTerminalOperation(wire wireResponse, state operationState) (wireResponse, error) {
	switch state.Status {
	case "succeeded":
		return wire, nil
	case "failed":
		if state.ErrorCode != "" {
			return wire, fmt.Errorf("operation %s failed with code %q", state.ID, state.ErrorCode)
		}
		return wire, fmt.Errorf("operation %s failed", state.ID)
	default:
		return wire, fmt.Errorf("unexpected terminal operation status %q", state.Status)
	}
}

func waitForPoll(ctx context.Context) error {
	timer := time.NewTimer(operationPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation polling: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func populateRecord(record *sample, wire wireResponse, includeResponse bool) {
	record.HTTPStatus = wire.Status
	record.ResponseBytes = len(wire.Body)
	sum := sha256.Sum256(wire.Body)
	record.ResponseSHA256 = hex.EncodeToString(sum[:])
	record.JSONValid = json.Valid(wire.Body)
	if includeResponse && record.JSONValid {
		record.Response = append(record.Response[:0], wire.Body...)
	}
}

func newClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: nil, DisableCompression: true, ForceAttemptHTTP2: false,
		MaxIdleConns: 2, MaxIdleConnsPerHost: 2, IdleConnTimeout: time.Minute,
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	return &http.Client{Transport: transport, Timeout: timeout}
}

func readSecret(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect token file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("token file must be a private regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token file: %w", err)
	}
	return normalizeSecret(body)
}

func readSecretReader(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read token stdin: %w", err)
	}
	return normalizeSecret(body)
}

func normalizeSecret(body []byte) ([]byte, error) {
	secret := bytes.TrimSpace(body)
	if len(secret) < 32 || len(secret) > 4096 {
		zero(body)
		return nil, fmt.Errorf("token length is outside safe bounds")
	}
	out := append([]byte(nil), secret...)
	zero(body)
	return out, nil
}

func sleepHalf(ctx context.Context, delay time.Duration, first bool) error {
	half := delay / 2
	if !first {
		half = delay - half
	}
	if half <= 0 {
		return nil
	}
	timer := time.NewTimer(half)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func zero(body []byte) {
	for i := range body {
		body[i] = 0
	}
}

func fatal(err error) {
	if !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "gateway-client:", err)
	}
	os.Exit(2)
}
