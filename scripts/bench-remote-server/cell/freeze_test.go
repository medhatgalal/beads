package cell_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/scripts/bench-remote-server/cell"
)

func TestWriteFreezeManifest(t *testing.T) {
	root := findRepoRoot(t)
	out := filepath.Join(t.TempDir(), "h1-freeze.json")
	err := cell.WriteFreezeManifest(root, []string{
		"scripts/bench-remote-server/cell",
		"scripts/bench-remote-server/fleet-catalog",
		"scripts/bench-remote-server/fleetlazy",
	}, out, "H1 three-repo cell composition", "PASS", []string{
		"composition gates only; not current-source 150ms latency",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var m cell.FreezeManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.GitSHA == "" || m.Result != "PASS" || len(m.PackageFileSHAs) == 0 {
		t.Fatalf("manifest incomplete: %+v", m)
	}
	if m.ProductionReady || m.TerminalVerdict != "GATEWAY_ARCHITECTURE_REQUIRED" {
		t.Fatalf("verdict fields wrong: %+v", m)
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
