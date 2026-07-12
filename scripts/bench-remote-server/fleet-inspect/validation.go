package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

var (
	databasePattern    = regexp.MustCompile(`^beads_perf_lab_[a-z0-9_]{1,96}$`)
	projectPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	operationIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	runIDPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{7,63}$`)
	hexDigestPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func parseInventory(body []byte) (inventory, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var value inventory
	if err := dec.Decode(&value); err != nil {
		return inventory{}, fmt.Errorf("decode inventory: %w", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return inventory{}, fmt.Errorf("decode inventory: %w", err)
	}
	if err := validateInventory(value); err != nil {
		return inventory{}, err
	}
	return value, nil
}

func parseRunnerReport(body []byte) (runnerReport, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	var value runnerReport
	if err := dec.Decode(&value); err != nil {
		return runnerReport{}, fmt.Errorf("decode runner report: %w", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return runnerReport{}, fmt.Errorf("decode runner report: %w", err)
	}
	return value, nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("trailing JSON")
	}
	return err
}

func validateInventory(value inventory) error {
	if value.SchemaVersion != inventorySchemaVersion {
		return fmt.Errorf("inventory schema_version must be %d", inventorySchemaVersion)
	}
	if !projectPattern.MatchString(value.LabID) {
		return fmt.Errorf("inventory lab_id must be a UUID")
	}
	if len(value.Targets) < 2 || len(value.Targets) > 500 {
		return fmt.Errorf("inventory targets must contain between 2 and 500 databases")
	}
	seenDB := map[string]struct{}{}
	seenProject := map[string]struct{}{}
	seenQueue := map[string]struct{}{}
	for index, target := range value.Targets {
		if net.ParseIP(target.Host) == nil || !net.ParseIP(target.Host).IsLoopback() {
			return fmt.Errorf("target %d host must be a numeric loopback address", index)
		}
		if target.Port < 1 || target.Port > 65535 || target.Port == 3306 || target.Port == 3307 {
			return fmt.Errorf("target %d port must be isolated; 3306 and 3307 are forbidden", index)
		}
		if !databasePattern.MatchString(target.DatabaseID) {
			return fmt.Errorf("target %d database_id must match beads_perf_lab_*", index)
		}
		if _, err := labidentity.SQLUser(target.ProjectID); err != nil {
			return fmt.Errorf("target %d project_id must be a canonical UUID", index)
		}
		if !filepath.IsAbs(target.PasswordFile) || !filepath.IsAbs(target.QueuePath) {
			return fmt.Errorf("target %d password_file and queue_path must be absolute", index)
		}
		queue := filepath.Clean(target.QueuePath)
		if _, exists := seenDB[target.DatabaseID]; exists {
			return fmt.Errorf("duplicate database_id %q", target.DatabaseID)
		}
		if _, exists := seenProject[target.ProjectID]; exists {
			return fmt.Errorf("duplicate project_id %q", target.ProjectID)
		}
		if _, exists := seenQueue[queue]; exists {
			return fmt.Errorf("duplicate queue_path %q", queue)
		}
		seenDB[target.DatabaseID] = struct{}{}
		seenProject[target.ProjectID] = struct{}{}
		seenQueue[queue] = struct{}{}
	}
	return nil
}

func validateInventoryAgainstReport(inv inventory, value runnerReport) error {
	if value.SchemaVersion != reportSchemaVersion {
		return fmt.Errorf("runner report schema_version must be %d", reportSchemaVersion)
	}
	if value.Mode != "closed-loop" {
		return fmt.Errorf("runner report mode must be closed-loop")
	}
	switch value.Profile {
	case "busy", "coordinated-burst":
	default:
		return fmt.Errorf("runner report profile must be busy or coordinated-burst")
	}
	// Exact-effect inspection is meaningful only for a mixed workload. Requiring
	// one read, one point mutation, and one structural graph mutation prevents a
	// zero-work (or read-only) report from being relabeled as fleet evidence.
	if value.Metrics.Generated < 3 || value.Metrics.ReadCommands < 1 ||
		value.Metrics.PointWrites < 1 || value.Metrics.Graphs < 1 ||
		value.Metrics.Generated != value.Metrics.ReadCommands+value.Metrics.PointWrites+value.Metrics.Graphs {
		return fmt.Errorf("runner report does not contain the required mixed workload for profile %s", value.Profile)
	}
	if value.Config.LabID != inv.LabID {
		return fmt.Errorf("runner report lab_id does not match inventory")
	}
	if value.Config.RegisteredDatabases != len(inv.Targets) || value.Attestation.Required != len(inv.Targets) || value.Attestation.Verified != len(inv.Targets) {
		return fmt.Errorf("runner report does not prove attestation of every inventory database")
	}
	if value.Config.ActiveDatabases != len(value.Config.ActiveDatabaseIDs) || value.Config.ActiveDatabases < 2 {
		return fmt.Errorf("runner report active database inventory is inconsistent")
	}
	known := map[string]struct{}{}
	for _, target := range inv.Targets {
		known[target.DatabaseID] = struct{}{}
	}
	seen := map[string]struct{}{}
	for _, database := range value.Config.ActiveDatabaseIDs {
		if _, ok := known[database]; !ok {
			return fmt.Errorf("runner report active database %q is not in inventory", database)
		}
		if _, duplicate := seen[database]; duplicate {
			return fmt.Errorf("runner report repeats active database %q", database)
		}
		seen[database] = struct{}{}
	}
	return nil
}

func inventoryByDatabase(inv inventory) map[string]inventoryTarget {
	result := make(map[string]inventoryTarget, len(inv.Targets))
	for _, target := range inv.Targets {
		result[target.DatabaseID] = target
	}
	return result
}

func sortedTargetIDs(inv inventory) []string {
	ids := make([]string, 0, len(inv.Targets))
	for _, target := range inv.Targets {
		ids = append(ids, target.DatabaseID)
	}
	sort.Strings(ids)
	return ids
}

func readGuardedFile(path string, maxBytes int64, requirePrivate bool) ([]byte, error) {
	if err := validateRegularPath(path, requirePrivate); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		zero(body)
		return nil, fmt.Errorf("file exceeds %d bytes", maxBytes)
	}
	return body, nil
}

func readSecret(path string) ([]byte, error) {
	body, err := readGuardedFile(path, 64<<10, true)
	if err != nil {
		return nil, err
	}
	secret := append([]byte(nil), bytes.TrimSpace(body)...)
	zero(body)
	if len(secret) < 16 {
		zero(secret)
		return nil, fmt.Errorf("secret must contain at least 16 bytes")
	}
	return secret, nil
}

func validateRegularPath(path string, requirePrivate bool) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("path must be absolute")
	}
	if err := rejectSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path must be a regular non-symlink file")
	}
	if requirePrivate && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("file permissions must not grant group or other access")
	}
	return nil
}

func rejectSymlinkComponents(path string) error {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	rest := strings.TrimPrefix(clean, volume)
	current := volume + string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(rest, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink path component is forbidden")
		}
	}
	return nil
}

func validateQueueFiles(path string) error {
	if err := validateRegularPath(path, true); err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		sidecar := path + suffix
		if _, err := os.Lstat(sidecar); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := validateRegularPath(sidecar, true); err != nil {
			return fmt.Errorf("queue sidecar: %w", err)
		}
	}
	return nil
}

func zero(body []byte) {
	for index := range body {
		body[index] = 0
	}
}
