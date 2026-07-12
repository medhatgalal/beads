package fleetcatalog

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testPrivateKey1 = ed25519.NewKeyFromSeed([]byte("11111111111111111111111111111111"))
	testPrivateKey2 = ed25519.NewKeyFromSeed([]byte("22222222222222222222222222222222"))
	testPublicKey1  = testPrivateKey1.Public().(ed25519.PublicKey)
	testPublicKey2  = testPrivateKey2.Public().(ed25519.PublicKey)
)

func fixedClock(now time.Time) func() time.Time { return func() time.Time { return now } }

func TestSignedCatalogEnforcesThreeHundredRepoTeamIsolation(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	catalog := fleetCatalog(now, 1, 1)
	signed, err := Sign(catalog, testPrivateKey1)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(signed, VerificationKeyring{1: testPublicKey1}, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range catalog.Routes {
		resolved, err := resolver.Resolve(RequestIdentity{
			RepositoryID: route.RepositoryID, TeamID: route.TeamID,
			ProjectID: route.ProjectID, DatabaseEpoch: route.DatabaseEpoch,
		})
		if err != nil || resolved != route {
			t.Fatalf("resolve %s route=%+v err=%v", route.RepositoryID, resolved, err)
		}
		wrongTeam := RequestIdentity{
			RepositoryID: route.RepositoryID, TeamID: route.TeamID + "-other",
			ProjectID: route.ProjectID, DatabaseEpoch: route.DatabaseEpoch,
		}
		if _, err := resolver.Resolve(wrongTeam); !errors.Is(err, errIdentityMismatch) {
			t.Fatalf("cross-team resolve %s error=%v", route.RepositoryID, err)
		}
	}
	ae, err := resolver.Resolve(identityFor(catalog.Routes[0]))
	if err != nil || ae.RepositoryID != "ae" || !ae.Dedicated || ae.CellID != "ae-dedicated" || ae.EndpointID != "ae-primary" {
		t.Fatalf("ae route=%+v err=%v", ae, err)
	}
}

func TestCatalogTamperStaleRestoreAndKeyEpochFailClosed(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	signed1, err := Sign(fleetCatalog(now, 1, 1), testPrivateKey1)
	if err != nil {
		t.Fatal(err)
	}
	keyring := VerificationKeyring{1: testPublicKey1, 2: testPublicKey2}
	clockNow := now
	resolver, err := NewResolver(signed1, keyring, func() time.Time { return clockNow })
	if err != nil {
		t.Fatal(err)
	}

	tampered := signed1
	tampered.Routes = append([]Route(nil), signed1.Routes...)
	tampered.Routes[1].TeamID = "attacker"
	if err := Verify(tampered, keyring, now); !errors.Is(err, errInvalidCatalog) {
		t.Fatalf("tampered verification error=%v", err)
	}
	if err := Verify(signed1, VerificationKeyring{2: testPublicKey2}, now); !errors.Is(err, errInvalidCatalog) {
		t.Fatalf("unknown key epoch error=%v", err)
	}
	if err := resolver.Replace(signed1, keyring); !errors.Is(err, errStaleCatalog) {
		t.Fatalf("stale replacement error=%v", err)
	}

	catalog2 := fleetCatalog(now.Add(time.Minute), 2, 2)
	catalog2.Routes[1].DatabaseEpoch = uuid.NewString()
	signed2, err := Sign(catalog2, testPrivateKey2)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity := identityFor(signed1.Routes[1])
	clockNow = now.Add(time.Minute)
	if err := resolver.Replace(signed2, keyring); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(oldIdentity); !errors.Is(err, errIdentityMismatch) {
		t.Fatalf("stale database epoch error=%v", err)
	}
	if _, err := resolver.Resolve(identityFor(catalog2.Routes[1])); err != nil {
		t.Fatalf("new database epoch did not resolve: %v", err)
	}
}

func TestCatalogConcurrentResolveAndAtomicReplacement(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	keyring := VerificationKeyring{1: testPublicKey1, 2: testPublicKey2}
	first, _ := Sign(fleetCatalog(now, 1, 1), testPrivateKey1)
	resolver, err := NewResolver(first, keyring, fixedClock(now))
	if err != nil {
		t.Fatal(err)
	}
	secondCatalog := fleetCatalog(now.Add(time.Minute), 2, 2)
	second, _ := Sign(secondCatalog, testPrivateKey2)

	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_, _ = resolver.Resolve(identityFor(first.Routes[(i%299)+1]))
			}
		}()
	}
	if err := resolver.Replace(second, keyring); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if resolver.Version() != 2 {
		t.Fatalf("catalog version=%d", resolver.Version())
	}
}

func TestCatalogRejectsNonDedicatedAEAndDuplicateDatabase(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	tests := []Catalog{fleetCatalog(now, 1, 1), fleetCatalog(now, 1, 1)}
	tests[0].Routes[0].Dedicated = false
	tests[1].Routes[2].Database = tests[1].Routes[1].Database
	for i, catalog := range tests {
		signed, err := Sign(catalog, testPrivateKey1)
		if err != nil {
			t.Fatal(err)
		}
		if err := Verify(signed, VerificationKeyring{1: testPublicKey1}, now); !errors.Is(err, errInvalidCatalog) {
			t.Errorf("case %d error=%v", i, err)
		}
	}
}

func TestResolverExpiresInPlaceAndRejectsKeyEpochDowngrade(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	clockNow := now
	keyring := VerificationKeyring{1: testPublicKey1, 2: testPublicKey2}
	activeCatalog := fleetCatalog(now, 2, 2)
	active, err := Sign(activeCatalog, testPrivateKey2)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(active, keyring, func() time.Time { return clockNow })
	if err != nil {
		t.Fatal(err)
	}

	downgradeCatalog := fleetCatalog(now.Add(time.Minute), 3, 1)
	downgrade, err := Sign(downgradeCatalog, testPrivateKey1)
	if err != nil {
		t.Fatal(err)
	}
	clockNow = now.Add(time.Minute)
	if err := resolver.Replace(downgrade, keyring); !errors.Is(err, errKeyEpochDowngrade) {
		t.Fatalf("key epoch downgrade error=%v", err)
	}
	clockNow = activeCatalog.ExpiresAt
	if _, err := resolver.Resolve(identityFor(activeCatalog.Routes[1])); !errors.Is(err, errCatalogExpired) {
		t.Fatalf("expired in-place resolver error=%v", err)
	}
}

func TestCatalogRejectsCrossTeamCellAndSharedDedicatedEndpoint(t *testing.T) {
	now := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	crossTeamCell := fleetCatalog(now, 1, 1)
	crossTeamCell.Routes[11].CellID = crossTeamCell.Routes[1].CellID

	sharedDedicated := fleetCatalog(now, 1, 1)
	sharedDedicated.Routes[1].Dedicated = true
	sharedDedicated.Routes[2].EndpointID = sharedDedicated.Routes[1].EndpointID

	tooLong := fleetCatalog(now, 1, 1)
	tooLong.ExpiresAt = tooLong.IssuedAt.Add(maximumCatalogLifetime + time.Second)
	for index, catalog := range []Catalog{crossTeamCell, sharedDedicated, tooLong} {
		signed, err := Sign(catalog, testPrivateKey1)
		if err != nil {
			t.Fatal(err)
		}
		if err := Verify(signed, VerificationKeyring{1: testPublicKey1}, now); !errors.Is(err, errInvalidCatalog) {
			t.Errorf("case %d error=%v", index, err)
		}
	}
}

func fleetCatalog(now time.Time, version, keyEpoch uint64) Catalog {
	routes := make([]Route, 0, 300)
	routes = append(routes, Route{
		RepositoryID: "ae", TeamID: "team-ae", CellID: "ae-dedicated",
		Database: "beads_perf_lab_ae", ProjectID: stableUUID("project-ae"),
		DatabaseEpoch: stableUUID(fmt.Sprintf("epoch-ae-%d", version)),
		EndpointID:    "ae-primary", Dedicated: true,
	})
	for index := 1; index < 300; index++ {
		repository := fmt.Sprintf("repo-%03d", index)
		team := fmt.Sprintf("team-%02d", (index-1)/10)
		routes = append(routes, Route{
			RepositoryID: repository, TeamID: team, CellID: team + "-cell",
			Database:      "beads_perf_lab_" + fmt.Sprintf("repo_%03d", index),
			ProjectID:     stableUUID("project-" + repository),
			DatabaseEpoch: stableUUID(fmt.Sprintf("epoch-%s-%d", repository, version)),
			EndpointID:    fmt.Sprintf("shared-primary-%d", (index-1)/100),
		})
	}
	return Catalog{
		SchemaVersion: 1, CatalogVersion: version, KeyEpoch: keyEpoch,
		IssuedAt: now, ExpiresAt: now.Add(15 * time.Minute), Routes: routes,
	}
}

func stableUUID(value string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(value)).String()
}

func identityFor(route Route) RequestIdentity {
	return RequestIdentity{
		RepositoryID: route.RepositoryID, TeamID: route.TeamID,
		ProjectID: route.ProjectID, DatabaseEpoch: route.DatabaseEpoch,
	}
}
