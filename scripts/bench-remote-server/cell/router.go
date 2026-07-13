package cell

import (
	"net/http"
	"strings"
	"sync"
)

// ProjectRouter routes /v1/projects/{projectID}/... to per-project handlers.
// Unknown projects fail closed with 404.
type ProjectRouter struct {
	mu       sync.RWMutex
	services map[string]http.Handler
}

func NewProjectRouter() *ProjectRouter {
	return &ProjectRouter{services: make(map[string]http.Handler)}
}

func (r *ProjectRouter) Register(projectID string, h http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[projectID] = h
}

func (r *ProjectRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "projects" || parts[2] == "" {
		http.Error(w, `{"error":"not_found","message":"expected /v1/projects/{project_id}/..."}`, http.StatusNotFound)
		return
	}
	projectID := parts[2]
	r.mu.RLock()
	h := r.services[projectID]
	r.mu.RUnlock()
	if h == nil {
		http.Error(w, `{"error":"unknown_project","message":"project is not registered in this cell"}`, http.StatusNotFound)
		return
	}
	h.ServeHTTP(w, req)
}
