package cell

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"
)

// FreezeManifest records source and binary hashes for a lab evidence run.
type FreezeManifest struct {
	SchemaVersion   int               `json:"schema_version"`
	GeneratedAt     string            `json:"generated_at"`
	GitSHA          string            `json:"git_sha"`
	Branch          string            `json:"branch,omitempty"`
	GoVersion       string            `json:"go_version,omitempty"`
	Hypothesis      string            `json:"hypothesis"`
	Result          string            `json:"result"`
	PackageFileSHAs map[string]string `json:"package_file_shas"`
	BinarySHAs      map[string]string `json:"binary_shas,omitempty"`
	Notes           []string          `json:"notes,omitempty"`
	TerminalVerdict string            `json:"terminal_verdict"`
	ProductionReady bool              `json:"production_ready"`
}

// WriteFreezeManifest walks relRoots under repoRoot and records file SHAs.
func WriteFreezeManifest(repoRoot string, relRoots []string, outPath string, hypothesis, result string, notes []string) error {
	gitSHA, _ := runGit(repoRoot, "rev-parse", "HEAD")
	branch, _ := runGit(repoRoot, "rev-parse", "--abbrev-ref", "HEAD")
	goVersion, _ := runCmd("go", "version")

	files := map[string]string{}
	for _, root := range relRoots {
		abs := filepath.Join(repoRoot, root)
		_ = filepath.Walk(abs, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			// Skip large / generated noise.
			switch filepath.Ext(path) {
			case ".go", ".md", ".yml", ".yaml", ".json", ".sh":
			default:
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			sum, err := sha256File(path)
			if err != nil {
				return err
			}
			files[rel] = sum
			return nil
		})
	}
	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(files))
	for _, k := range keys {
		ordered[k] = files[k]
	}

	m := FreezeManifest{
		SchemaVersion:   1,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		GitSHA:          gitSHA,
		Branch:          branch,
		GoVersion:       goVersion,
		Hypothesis:      hypothesis,
		Result:          result,
		PackageFileSHAs: ordered,
		Notes:           notes,
		TerminalVerdict: "GATEWAY_ARCHITECTURE_REQUIRED",
		ProductionReady: false,
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, append(raw, '\n'), 0o644)
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(trimNL(out)), nil
}

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return "", err
	}
	return string(trimNL(out)), nil
}

func trimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// HashBinary returns sha256 of a file for BINARY_MANIFEST entries.
func HashBinary(path string) (string, error) {
	sum, err := sha256File(path)
	if err != nil {
		return "", fmt.Errorf("hash binary %s: %w", path, err)
	}
	return sum, nil
}
