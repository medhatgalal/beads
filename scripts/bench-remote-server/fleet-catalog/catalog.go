package fleetcatalog

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	errInvalidCatalog    = errors.New("invalid signed fleet catalog")
	errIdentityMismatch  = errors.New("request identity does not match the catalog route")
	errStaleCatalog      = errors.New("catalog version is not newer than the active version")
	errCatalogExpired    = errors.New("active fleet catalog is expired or not yet valid")
	errKeyEpochDowngrade = errors.New("catalog key epoch would move backwards")
	logicalIDPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)
)

const (
	maximumCatalogLifetime = 15 * time.Minute
	maximumClockSkew       = time.Minute
)

type VerificationKeyring map[uint64]ed25519.PublicKey

type Route struct {
	RepositoryID  string `json:"repository_id"`
	TeamID        string `json:"team_id"`
	CellID        string `json:"cell_id"`
	Database      string `json:"database"`
	ProjectID     string `json:"project_id"`
	DatabaseEpoch string `json:"database_epoch"`
	EndpointID    string `json:"endpoint_id"`
	Dedicated     bool   `json:"dedicated,omitempty"`
}

type Catalog struct {
	SchemaVersion  int       `json:"schema_version"`
	CatalogVersion uint64    `json:"catalog_version"`
	KeyEpoch       uint64    `json:"key_epoch"`
	IssuedAt       time.Time `json:"issued_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Routes         []Route   `json:"routes"`
	Signature      string    `json:"signature"`
}

type RequestIdentity struct {
	RepositoryID  string
	TeamID        string
	ProjectID     string
	DatabaseEpoch string
}

type Resolver struct {
	mu      sync.RWMutex
	catalog Catalog
	routes  map[string]Route
	clock   func() time.Time
}

// Sign is a control-plane operation. Cells receive only the corresponding
// public verification key, so compromising a cell does not grant route-forging
// capability.
func Sign(catalog Catalog, privateKey ed25519.PrivateKey) (Catalog, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Catalog{}, fmt.Errorf("catalog Ed25519 private key has the wrong size")
	}
	catalog.Signature = ""
	canonical, err := canonicalCatalog(catalog)
	if err != nil {
		return Catalog{}, err
	}
	catalog.Signature = hex.EncodeToString(ed25519.Sign(privateKey, canonical))
	return catalog, nil
}

func Verify(catalog Catalog, keyring VerificationKeyring, now time.Time) error {
	key := keyring[catalog.KeyEpoch]
	if len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: unknown or malformed public key epoch", errInvalidCatalog)
	}
	provided, err := hex.DecodeString(catalog.Signature)
	if err != nil || len(provided) != ed25519.SignatureSize {
		return fmt.Errorf("%w: malformed signature", errInvalidCatalog)
	}
	unsigned := catalog
	unsigned.Signature = ""
	canonical, err := canonicalCatalog(unsigned)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, canonical, provided) {
		return fmt.Errorf("%w: signature mismatch", errInvalidCatalog)
	}
	return validateCatalog(unsigned, now.UTC())
}

func NewResolver(catalog Catalog, keyring VerificationKeyring, clock func() time.Time) (*Resolver, error) {
	if clock == nil {
		return nil, fmt.Errorf("resolver clock is required")
	}
	if err := Verify(catalog, keyring, clock()); err != nil {
		return nil, err
	}
	return &Resolver{catalog: catalog, routes: indexRoutes(catalog.Routes), clock: clock}, nil
}

func (r *Resolver) Resolve(identity RequestIdentity) (Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.clock().UTC()
	if now.Before(r.catalog.IssuedAt.Add(-maximumClockSkew)) || !now.Before(r.catalog.ExpiresAt) {
		return Route{}, errCatalogExpired
	}
	route, ok := r.routes[identity.RepositoryID]
	if !ok || route.TeamID != identity.TeamID || route.ProjectID != identity.ProjectID ||
		route.DatabaseEpoch != identity.DatabaseEpoch {
		return Route{}, errIdentityMismatch
	}
	return route, nil
}

func (r *Resolver) Replace(catalog Catalog, keyring VerificationKeyring) error {
	if err := Verify(catalog, keyring, r.clock()); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if catalog.CatalogVersion <= r.catalog.CatalogVersion {
		return errStaleCatalog
	}
	if catalog.KeyEpoch < r.catalog.KeyEpoch {
		return errKeyEpochDowngrade
	}
	r.catalog = catalog
	r.routes = indexRoutes(catalog.Routes)
	return nil
}

func (r *Resolver) Version() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.catalog.CatalogVersion
}

func canonicalCatalog(catalog Catalog) ([]byte, error) {
	copyCatalog := catalog
	copyCatalog.Routes = append([]Route(nil), catalog.Routes...)
	sort.Slice(copyCatalog.Routes, func(i, j int) bool {
		return copyCatalog.Routes[i].RepositoryID < copyCatalog.Routes[j].RepositoryID
	})
	body, err := json.Marshal(copyCatalog)
	if err != nil {
		return nil, fmt.Errorf("canonicalize catalog: %w", err)
	}
	return body, nil
}

func validateCatalog(catalog Catalog, now time.Time) error {
	if catalog.SchemaVersion != 1 || catalog.CatalogVersion == 0 || catalog.KeyEpoch == 0 ||
		catalog.IssuedAt.IsZero() || catalog.ExpiresAt.IsZero() || !catalog.ExpiresAt.After(catalog.IssuedAt) ||
		catalog.ExpiresAt.Sub(catalog.IssuedAt) > maximumCatalogLifetime ||
		now.Before(catalog.IssuedAt.Add(-maximumClockSkew)) || !now.Before(catalog.ExpiresAt) ||
		len(catalog.Routes) == 0 || len(catalog.Routes) > 10000 {
		return fmt.Errorf("%w: invalid header or lifetime", errInvalidCatalog)
	}
	repositories := map[string]struct{}{}
	databases := map[string]struct{}{}
	projects := map[string]struct{}{}
	cellTeams := map[string]string{}
	cellRepositories := map[string]string{}
	cellDedicated := map[string]bool{}
	endpointRoutes := map[string]Route{}
	for _, route := range catalog.Routes {
		if !logicalIDPattern.MatchString(route.RepositoryID) || !logicalIDPattern.MatchString(route.TeamID) ||
			!logicalIDPattern.MatchString(route.CellID) || !logicalIDPattern.MatchString(route.EndpointID) ||
			!validDatabase(route.Database) {
			return fmt.Errorf("%w: invalid logical route identity", errInvalidCatalog)
		}
		if _, err := uuid.Parse(route.ProjectID); err != nil {
			return fmt.Errorf("%w: invalid project UUID", errInvalidCatalog)
		}
		if _, err := uuid.Parse(route.DatabaseEpoch); err != nil {
			return fmt.Errorf("%w: invalid database epoch", errInvalidCatalog)
		}
		if _, exists := repositories[route.RepositoryID]; exists {
			return fmt.Errorf("%w: duplicate repository", errInvalidCatalog)
		}
		if _, exists := databases[route.Database]; exists {
			return fmt.Errorf("%w: duplicate database", errInvalidCatalog)
		}
		if _, exists := projects[route.ProjectID]; exists {
			return fmt.Errorf("%w: duplicate project", errInvalidCatalog)
		}
		repositories[route.RepositoryID] = struct{}{}
		databases[route.Database] = struct{}{}
		projects[route.ProjectID] = struct{}{}
		if team, exists := cellTeams[route.CellID]; exists && team != route.TeamID {
			return fmt.Errorf("%w: execution cell spans multiple teams", errInvalidCatalog)
		}
		cellTeams[route.CellID] = route.TeamID
		if previous, exists := endpointRoutes[route.EndpointID]; exists && (previous.Dedicated || route.Dedicated) {
			return fmt.Errorf("%w: dedicated endpoint is shared", errInvalidCatalog)
		}
		endpointRoutes[route.EndpointID] = route
		if previous, exists := cellRepositories[route.CellID]; exists && previous != route.RepositoryID &&
			(cellDedicated[route.CellID] || route.Dedicated) {
			return fmt.Errorf("%w: dedicated execution cell is shared", errInvalidCatalog)
		}
		if _, exists := cellRepositories[route.CellID]; !exists {
			cellRepositories[route.CellID] = route.RepositoryID
		}
		cellDedicated[route.CellID] = cellDedicated[route.CellID] || route.Dedicated
		if route.RepositoryID == "ae" && (!route.Dedicated || route.CellID != "ae-dedicated" || route.EndpointID != "ae-primary") {
			return fmt.Errorf("%w: ae must use its dedicated cell and endpoint", errInvalidCatalog)
		}
		if route.RepositoryID != "ae" && (route.CellID == "ae-dedicated" || route.EndpointID == "ae-primary") {
			return fmt.Errorf("%w: ae bulkhead identity is reserved", errInvalidCatalog)
		}
	}
	return nil
}

func validDatabase(database string) bool {
	return strings.HasPrefix(database, "beads_perf_lab_") && logicalIDPattern.MatchString(database)
}

func indexRoutes(routes []Route) map[string]Route {
	indexed := make(map[string]Route, len(routes))
	for _, route := range routes {
		indexed[route.RepositoryID] = route
	}
	return indexed
}
