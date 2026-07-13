package cell_test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/steveyegge/beads/scripts/bench-remote-server/cell"
	fleetcatalog "github.com/steveyegge/beads/scripts/bench-remote-server/fleet-catalog"
	"github.com/steveyegge/beads/scripts/bench-remote-server/fleetlazy"
)

type h1FakePool struct {
	open atomic.Int32
}

func (p *h1FakePool) Close() error {
	p.open.Store(0)
	return nil
}

func (p *h1FakePool) OpenConnections() int {
	return int(p.open.Load())
}

// TestH1_ThreeRepoCellComposition proves the integrated cell composition gates
// that do not require a live Dolt process:
//  1. signed catalog routes three synthetic repositories and rejects stale epochs;
//  2. multi-project HTTP router dispatches by project id and fails closed;
//  3. lazy pool budget 2 caps resident pools across three databases and can
//     hibernate to zero cold connections.
//
// Terminal-v2 exactly-once against real Dolt is H1b (see h1_dolt_test.go).
func TestH1_ThreeRepoCellComposition(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	routes := make([]fleetcatalog.Route, 0, 3)
	projectIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		projectIDs[i] = uuid.NewString()
		routes = append(routes, fleetcatalog.Route{
			RepositoryID:  fmt.Sprintf("repo-%d", i),
			TeamID:        "team-a",
			CellID:        "cell-1",
			Database:      fmt.Sprintf("beads_perf_lab_cell_%d", i),
			ProjectID:     projectIDs[i],
			DatabaseEpoch: uuid.NewString(),
			EndpointID:    "loopback-1",
		})
	}
	catalog, err := fleetcatalog.Sign(fleetcatalog.Catalog{
		SchemaVersion:  1,
		CatalogVersion: 1,
		KeyEpoch:       1,
		IssuedAt:       now.Add(-time.Minute),
		ExpiresAt:      now.Add(10 * time.Minute),
		Routes:         routes,
	}, priv)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := fleetcatalog.NewResolver(catalog, fleetcatalog.VerificationKeyring{1: pub}, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	for _, route := range routes {
		got, err := resolver.Resolve(fleetcatalog.RequestIdentity{
			RepositoryID:  route.RepositoryID,
			TeamID:        route.TeamID,
			ProjectID:     route.ProjectID,
			DatabaseEpoch: route.DatabaseEpoch,
		})
		if err != nil {
			t.Fatalf("resolve %s: %v", route.RepositoryID, err)
		}
		if got.Database != route.Database {
			t.Fatalf("database mismatch for %s", route.RepositoryID)
		}
	}
	if _, err := resolver.Resolve(fleetcatalog.RequestIdentity{
		RepositoryID:  routes[0].RepositoryID,
		TeamID:        routes[0].TeamID,
		ProjectID:     routes[0].ProjectID,
		DatabaseEpoch: "stale-epoch",
	}); err == nil {
		t.Fatal("expected stale database_epoch to fail closed")
	}

	router := cell.NewProjectRouter()
	var hits [3]atomic.Int64
	for i, pid := range projectIDs {
		i, pid := i, pid
		router.Register(pid, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			w.WriteHeader(http.StatusOK)
		}))
	}
	for i, pid := range projectIDs {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+pid+"/terminal-operations", nil)
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || hits[i].Load() != 1 {
			t.Fatalf("project %d: code=%d hits=%d", i, rec.Code, hits[i].Load())
		}
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/"+uuid.NewString()+"/health", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown project code=%d", rec.Code)
	}

	manager, err := fleetlazy.NewLazyPoolManager(2, func(database string) (fleetlazy.ManagedPool, error) {
		p := &h1FakePool{}
		p.open.Store(1)
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := fmt.Sprintf("beads_perf_lab_cell_%d", i)
			lease, err := manager.Acquire(context.Background(), db)
			if err != nil {
				errCh <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
			lease.Release()
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent acquire: %v", err)
	}
	m := manager.Metrics()
	if m.Budget != 2 {
		t.Fatalf("budget=%d", m.Budget)
	}
	if m.MaxResidentPools > 2 {
		t.Fatalf("max resident pools %d exceeds budget 2", m.MaxResidentPools)
	}
	if err := manager.HibernateIdle(); err != nil {
		t.Fatal(err)
	}
	m = manager.Metrics()
	if m.ResidentPools != 0 {
		t.Fatalf("after hibernate resident=%d want 0", m.ResidentPools)
	}
}
