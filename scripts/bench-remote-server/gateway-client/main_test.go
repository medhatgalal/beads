package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const clientTestProjectID = "11111111-2222-4333-8444-555555555555"

var clientTestToken = []byte("0123456789abcdef0123456789abcdef")

type scriptedResponse struct {
	status int
	body   string
}

type requestSnapshot struct {
	method         string
	path           string
	authorization  string
	projectID      string
	idempotencyKey string
}

type scriptedRoundTripper struct {
	mu        sync.Mutex
	responses []scriptedResponse
	requests  []requestSnapshot
}

func (s *scriptedRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, requestSnapshot{
		method: request.Method, path: request.URL.Path,
		authorization: request.Header.Get("Authorization"), projectID: request.Header.Get("X-Beads-Project-ID"),
		idempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	index := len(s.requests) - 1
	if index >= len(s.responses) {
		return nil, fmt.Errorf("unexpected request %d", index)
	}
	response := s.responses[index]
	return &http.Response{
		StatusCode: response.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func TestExecutePostPollsUntilSucceeded(t *testing.T) {
	transport := &scriptedRoundTripper{responses: []scriptedResponse{
		{status: http.StatusAccepted, body: operationJSON("op-1", "accepted", "")},
		{status: http.StatusOK, body: operationJSON("op-1", "running", "")},
		{status: http.StatusOK, body: operationJSON("op-1", "succeeded", "")},
	}}
	client := &http.Client{Transport: transport, Timeout: time.Second}
	record := execute(context.Background(), client, postConfig(), mustBaseURL(t), clientTestToken,
		[]byte(`{"kind":"issue.create","payload":{"title":"[beads-perf-lab] client"}}`), "sample", 7)

	if !record.Passed || record.HTTPStatus != http.StatusOK || !record.JSONValid || record.Error != "" {
		t.Fatalf("record = %#v", record)
	}
	if !strings.Contains(string(record.Response), `"status":"succeeded"`) {
		t.Fatalf("final response = %s", record.Response)
	}
	if len(transport.requests) != 3 {
		t.Fatalf("requests = %#v", transport.requests)
	}
	if transport.requests[0].method != http.MethodPost || transport.requests[0].idempotencyKey != "client-key-7" {
		t.Fatalf("submit request = %#v", transport.requests[0])
	}
	for i, request := range transport.requests[1:] {
		if request.method != http.MethodGet || request.path != "/v1/projects/"+clientTestProjectID+"/operations/op-1" || request.idempotencyKey != "" {
			t.Fatalf("poll request %d = %#v", i, request)
		}
	}
	for _, request := range transport.requests {
		if request.authorization != "Bearer "+string(clientTestToken) || request.projectID != clientTestProjectID {
			t.Fatalf("request identity = %#v", request)
		}
	}
}

func TestExecutePostRejectsTerminalFailure(t *testing.T) {
	transport := &scriptedRoundTripper{responses: []scriptedResponse{{
		status: http.StatusOK,
		body:   operationJSON("op-failed", "failed", "operation_failed"),
	}}}
	record := execute(context.Background(), &http.Client{Transport: transport, Timeout: time.Second},
		postConfig(), mustBaseURL(t), clientTestToken, []byte(`{"kind":"issue.create","payload":{}}`), "sample", 0)
	if record.Passed || !strings.Contains(record.Error, `failed with code "operation_failed"`) {
		t.Fatalf("record = %#v", record)
	}
}

func TestExecutePostRejectsPolledTerminalFailure(t *testing.T) {
	transport := &scriptedRoundTripper{responses: []scriptedResponse{
		{status: http.StatusAccepted, body: operationJSON("op-failed", "running", "")},
		{status: http.StatusOK, body: operationJSON("op-failed", "failed", "operation_failed")},
	}}
	record := execute(context.Background(), &http.Client{Transport: transport, Timeout: time.Second},
		postConfig(), mustBaseURL(t), clientTestToken, []byte(`{"kind":"issue.create","payload":{}}`), "sample", 0)
	if record.Passed || record.HTTPStatus != http.StatusOK || !strings.Contains(record.Error, `failed with code "operation_failed"`) {
		t.Fatalf("record = %#v", record)
	}
}

func TestExecutePostRejectsArbitrary2xxAndChangedPollIdentity(t *testing.T) {
	t.Run("arbitrary 2xx", func(t *testing.T) {
		transport := &scriptedRoundTripper{responses: []scriptedResponse{{
			status: http.StatusCreated,
			body:   operationJSON("op-created", "succeeded", ""),
		}}}
		record := execute(context.Background(), &http.Client{Transport: transport, Timeout: time.Second},
			postConfig(), mustBaseURL(t), clientTestToken, []byte(`{"kind":"issue.create","payload":{}}`), "sample", 0)
		if record.Passed || !strings.Contains(record.Error, "unexpected submit HTTP status 201") {
			t.Fatalf("record = %#v", record)
		}
	})

	t.Run("poll identity", func(t *testing.T) {
		transport := &scriptedRoundTripper{responses: []scriptedResponse{
			{status: http.StatusAccepted, body: operationJSON("op-1", "accepted", "")},
			{status: http.StatusOK, body: operationJSON("op-2", "succeeded", "")},
		}}
		record := execute(context.Background(), &http.Client{Transport: transport, Timeout: time.Second},
			postConfig(), mustBaseURL(t), clientTestToken, []byte(`{"kind":"issue.create","payload":{}}`), "sample", 0)
		if record.Passed || !strings.Contains(record.Error, "operation identity changed") {
			t.Fatalf("record = %#v", record)
		}
	})
}

func postConfig() config {
	return config{
		ProjectID: clientTestProjectID, Method: http.MethodPost,
		Resource: "operations?wait_ms=1", KeyTemplate: "client-key-{{ordinal}}",
		Timeout: time.Second, IncludeResponse: true,
	}
}

func mustBaseURL(t *testing.T) *url.URL {
	t.Helper()
	base, err := url.Parse("http://127.0.0.1:7707")
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func operationJSON(id, status, code string) string {
	if code == "" {
		return fmt.Sprintf(`{"id":%q,"project_id":%q,"status":%q}`, id, clientTestProjectID, status)
	}
	return fmt.Sprintf(`{"id":%q,"project_id":%q,"status":%q,"error_code":%q}`, id, clientTestProjectID, status, code)
}
