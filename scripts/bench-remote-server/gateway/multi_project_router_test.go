package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMultiProjectRouterDispatchesByProjectID(t *testing.T) {
	router := newMultiProjectRouter(nil)
	var hitA, hitB int
	router.Register("proj-a", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitA++
		w.WriteHeader(http.StatusOK)
	}))
	router.Register("proj-b", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitB++
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/proj-a/terminal-operations", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || hitA != 1 || hitB != 0 {
		t.Fatalf("proj-a: code=%d hitA=%d hitB=%d", rec.Code, hitA, hitB)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/proj-b/health", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || hitB != 1 {
		t.Fatalf("proj-b: code=%d hitB=%d", rec.Code, hitB)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/missing/health", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing project code=%d", rec.Code)
	}
}
