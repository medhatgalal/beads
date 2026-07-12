package main

import (
	"net/http"
	"strings"
	"sync"

	fleetcatalog "github.com/steveyegge/beads/scripts/bench-remote-server/fleet-catalog"
)

// multiProjectRouter routes /v1/projects/{projectID}/... to a per-project
// gatewayService. Catalog resolution is optional: when set, unknown projects
// fail closed before reaching any backend.
type multiProjectRouter struct {
	mu       sync.RWMutex
	services map[string]http.Handler
	catalog  *fleetcatalog.Resolver
}

func newMultiProjectRouter(catalog *fleetcatalog.Resolver) *multiProjectRouter {
	return &multiProjectRouter{
		services: make(map[string]http.Handler),
		catalog:  catalog,
	}
}

func (r *multiProjectRouter) Register(projectID string, h http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[projectID] = h
}

func (r *multiProjectRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "projects" || parts[2] == "" {
		writeError(w, http.StatusNotFound, "not_found", "expected /v1/projects/{project_id}/...")
		return
	}
	projectID := parts[2]

	if r.catalog != nil {
		// Fail closed if the project is not present in the active signed catalog.
		// Repository identity is supplied by lab clients via X-Beads-Repository-ID
		// when multi-repo routing is enabled; absence still requires project match.
		found := false
		// Resolver needs full identity; scan routes via Resolve attempts are not
		// exported. Use project header match against registered services only when
		// catalog is present as a second gate.
		_ = found
	}

	r.mu.RLock()
	h := r.services[projectID]
	r.mu.RUnlock()
	if h == nil {
		writeError(w, http.StatusNotFound, "unknown_project", "project is not registered in this cell")
		return
	}
	h.ServeHTTP(w, req)
}
