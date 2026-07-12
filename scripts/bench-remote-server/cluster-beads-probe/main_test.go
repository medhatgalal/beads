package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const probeTestProjectID = "11111111-1111-4111-8111-111111111111"

func TestCanonicalApprovedPathRejectsTraversalEscapeAndSymlinks(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "approved")
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	valid := filepath.Join(root, "nested", "evidence.json")
	got, err := canonicalApprovedPath(valid, root, true)
	if err != nil || got != valid {
		t.Fatalf("valid path = %q, %v", got, err)
	}
	tests := map[string]string{
		"relative":  "evidence.json",
		"traversal": root + string(filepath.Separator) + "nested" + string(filepath.Separator) + ".." + string(filepath.Separator) + "evidence.json",
		"escape":    filepath.Join(root, "..", "outside.json"),
		"root":      root,
	}
	for name, path := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := canonicalApprovedPath(path, root, true); err == nil {
				t.Fatal("unsafe path passed validation")
			}
		})
	}

	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(root, "linked")
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := canonicalApprovedPath(filepath.Join(linkedDirectory, "evidence.json"), root, true); err == nil {
		t.Fatal("symlink directory component passed validation")
	}
	target := filepath.Join(realDirectory, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkedFile := filepath.Join(root, "linked-file")
	if err := os.Symlink(target, linkedFile); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalApprovedPath(linkedFile, root, false); err == nil {
		t.Fatal("final symlink passed validation")
	}
}

func TestPasswordReaderIsBoundedNoFollowAndExact0600(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "runtime")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(root, "password")
	if err := os.WriteFile(good, []byte("  synthetic-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	password, err := readPasswordFile(good, root)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(password)
	if string(password) != "synthetic-secret" {
		t.Fatalf("password = %q", password)
	}

	wrongMode := filepath.Join(root, "wrong-mode")
	if err := os.WriteFile(wrongMode, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(wrongMode, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := readPasswordFile(wrongMode, root); err == nil || !strings.Contains(err.Error(), "exact mode 0600") {
		t.Fatalf("wrong mode error = %v", err)
	}

	oversized := filepath.Join(root, "oversized")
	if err := os.WriteFile(oversized, bytes.Repeat([]byte("x"), maxPasswordBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPasswordFile(oversized, root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized error = %v", err)
	}

	linked := filepath.Join(root, "linked-password")
	if err := os.Symlink(good, linked); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := readPasswordFile(linked, root); err == nil {
		t.Fatal("symlink password passed validation")
	}
}

func TestEvidenceWriterIsExclusiveAndRejectsSymlink(t *testing.T) {
	root := filepath.Join(resolvedTempDir(t), "evidence")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "result.json")
	if err := writeEvidenceFileUnderRoot(path, root, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("evidence mode = %o", info.Mode().Perm())
	}
	if err := writeEvidenceFileUnderRoot(path, root, []byte("replacement")); err == nil {
		t.Fatal("existing evidence was overwritten")
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(root, "linked")
	if err := os.Symlink(target, linked); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := writeEvidenceFileUnderRoot(linked, root, []byte("bad")); err == nil {
		t.Fatal("symlink evidence target passed validation")
	}
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func TestProbeEndpointAndProjectIdentityFailClosed(t *testing.T) {
	if err := validateProbeEndpoint(probeHost, probePort, probeDatabase); err != nil {
		t.Fatalf("valid endpoint: %v", err)
	}
	for name, test := range map[string]struct {
		host     string
		port     int
		database string
	}{
		"hostname":            {"localhost", probePort, probeDatabase},
		"external host":       {"192.0.2.1", probePort, probeDatabase},
		"production mysql":    {probeHost, 3306, probeDatabase},
		"production dolt":     {probeHost, 3307, probeDatabase},
		"other loopback port": {probeHost, 13401, probeDatabase},
		"production database": {probeHost, probePort, "production"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateProbeEndpoint(test.host, test.port, test.database); err == nil {
				t.Fatal("unsafe endpoint passed validation")
			}
		})
	}
	user, err := labidentity.SQLUser(probeTestProjectID)
	if err != nil || user != "11111111111141118111111111111111" {
		t.Fatalf("derived SQL user = %q, %v", user, err)
	}
	if result := run("", "not-a-project", probeTestProjectID, "unsupported"); result.Success || !strings.Contains(result.Error, "unsupported action") {
		t.Fatalf("validation order/result = %+v", result)
	}
	if result := run("", "not-a-project", probeTestProjectID, "create"); result.Success || !strings.Contains(result.Error, "canonical UUID") {
		t.Fatalf("invalid project identity result = %+v", result)
	}
	if result := run("", probeTestProjectID, "not-a-lab", "create"); result.Success || !strings.Contains(result.Error, "lab-id") {
		t.Fatalf("invalid lab identity result = %+v", result)
	}
}

func TestProbeAttestationRequiresExactSyntheticIdentity(t *testing.T) {
	const labID = "22222222-2222-4222-8222-222222222222"
	valid := probeLabAttestation{
		Version: 1, Environment: "synthetic", LabID: labID,
		DatabaseName: probeDatabase, ProjectID: probeTestProjectID,
	}
	if err := validateProbeAttestation(valid, probeTestProjectID, labID); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*probeLabAttestation){
		"version":     func(a *probeLabAttestation) { a.Version = 2 },
		"environment": func(a *probeLabAttestation) { a.Environment = "production" },
		"lab":         func(a *probeLabAttestation) { a.LabID = "33333333-3333-4333-8333-333333333333" },
		"database":    func(a *probeLabAttestation) { a.DatabaseName = "production" },
		"project":     func(a *probeLabAttestation) { a.ProjectID = "33333333-3333-4333-8333-333333333333" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			marker := valid
			mutate(&marker)
			if err := validateProbeAttestation(marker, probeTestProjectID, labID); err == nil {
				t.Fatal("mismatched attestation passed")
			}
		})
	}
	canonical := `{"version":1,"environment":"synthetic","lab_id":"22222222-2222-4222-8222-222222222222","database_name":"beads_perf_lab_cluster_gateway_ae","project_id":"11111111-1111-4111-8111-111111111111"}`
	if marker, err := decodeProbeAttestation(canonical); err != nil || marker != valid {
		t.Fatalf("decode exact marker = %+v, %v", marker, err)
	}
	if _, err := decodeProbeAttestation(strings.TrimSuffix(canonical, "}") + `,"extra":true}`); err == nil {
		t.Fatal("unknown attestation field passed strict decoding")
	}
}
