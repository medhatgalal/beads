package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func validTestConfig(t *testing.T) config {
	t.Helper()
	directory, err := os.MkdirTemp("/private/tmp", "beads-fleet-scale-guard-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return config{
		host: "127.0.0.1", port: 13360, databases: 100, prewarm: 2,
		operations: 320, repetitions: 3,
		outputRoot: directory, output: filepath.Join(directory, "results.json"),
		labID: "dc6020ef-b433-41b6-9426-cedd9bf40502", ackSynthetic: true,
	}
}

func TestValidateConfigRequiresIsolatedLoopbackAndExplicitAcknowledgement(t *testing.T) {
	if err := validateConfig(validTestConfig(t)); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []struct {
		name string
		edit func(*config)
		want string
	}{
		{name: "hostname", edit: func(c *config) { c.host = "localhost" }, want: "numeric loopback"},
		{name: "external", edit: func(c *config) { c.host = "192.0.2.1" }, want: "numeric loopback"},
		{name: "production sql port", edit: func(c *config) { c.port = 3306 }, want: "must not be 3306"},
		{name: "production dolt port", edit: func(c *config) { c.port = 3307 }, want: "3307"},
		{name: "missing acknowledgement", edit: func(c *config) { c.ackSynthetic = false }, want: "ack-create"},
		{name: "missing lab identity", edit: func(c *config) { c.labID = "" }, want: "lab-id"},
		{name: "nil lab identity", edit: func(c *config) { c.labID = "00000000-0000-0000-0000-000000000000" }, want: "lab-id"},
		{name: "noncanonical lab identity", edit: func(c *config) { c.labID = "DC6020EF-B433-41B6-9426-CEDD9BF40502" }, want: "lab-id"},
		{name: "output root escape", edit: func(c *config) { c.outputRoot = "/private/tmp/other" }, want: "output-root"},
		{name: "output root control character", edit: func(c *config) { c.outputRoot = "/private/tmp/beads-fleet-scale-bad\nroot" }, want: "output-root"},
		{name: "output escape", edit: func(c *config) { c.output = "/private/report.json" }, want: "below output-root"},
		{name: "output root itself", edit: func(c *config) { c.output = c.outputRoot }, want: "below output-root"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validTestConfig(t)
			test.edit(&cfg)
			err := validateConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyLabControlIdentityRequiresExactSyntheticSingleton(t *testing.T) {
	const labID = "dc6020ef-b433-41b6-9426-cedd9bf40502"
	tests := []struct {
		name        string
		rows        *sqlmock.Rows
		queryErr    error
		wantFailure bool
	}{
		{name: "exact", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "synthetic", labID)},
		{name: "absent", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(0, "", ""), wantFailure: true},
		{name: "multiple", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(2, "synthetic", labID), wantFailure: true},
		{name: "environment", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "production", labID), wantFailure: true},
		{name: "other lab", rows: sqlmock.NewRows([]string{"count", "environment", "lab_id"}).AddRow(1, "synthetic", "80d22c54-066c-4f89-b6e8-d94e19b4e902"), wantFailure: true},
		{name: "missing table", queryErr: errors.New("table not found"), wantFailure: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectation := mock.ExpectQuery(regexp.QuoteMeta(labControlIdentityQuery))
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(test.rows)
			}
			err = verifyLabControlIdentity(context.Background(), db, labID)
			if (err != nil) != test.wantFailure {
				t.Fatalf("verifyLabControlIdentity() error = %v, want failure %v", err, test.wantFailure)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFleetScaleOutputRejectsSymlinkComponentsAndFinalSymlink(t *testing.T) {
	directory, err := os.MkdirTemp("/private/tmp", "beads-fleet-scale-symlink-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	realDirectory := filepath.Join(directory, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(directory, "linked")
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Fatal(err)
	}
	if err := validateFleetScaleOutput(directory, filepath.Join(linkedDirectory, "results.json")); err == nil {
		t.Fatal("symlink output parent was accepted")
	}
	target := filepath.Join(realDirectory, "target.json")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkedFile := filepath.Join(realDirectory, "linked.json")
	if err := os.Symlink(target, linkedFile); err != nil {
		t.Fatal(err)
	}
	if err := validateFleetScaleOutput(directory, linkedFile); err == nil {
		t.Fatal("symlink output file was accepted")
	}
}

func TestFleetScaleOutputRejectsSymlinkRoot(t *testing.T) {
	realRoot, err := os.MkdirTemp("/private/tmp", "beads-fleet-scale-real-root-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(realRoot) })
	linkedRoot := realRoot + "-link"
	if err := os.Symlink(realRoot, linkedRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(linkedRoot) })
	if err := validateFleetScaleOutput(linkedRoot, filepath.Join(linkedRoot, "results.json")); err == nil {
		t.Fatal("symlink output root was accepted")
	}
}

func TestWriteFleetScaleOutputCreatesPrivateRegularFile(t *testing.T) {
	cfg := validTestConfig(t)
	if err := writeFleetScaleOutput(cfg.outputRoot, cfg.output, []byte("{\"passed\":true}\n")); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(cfg.output)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "{\"passed\":true}\n" {
		t.Fatalf("output body = %q", body)
	}
	info, err := os.Stat(cfg.output)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %v", info.Mode())
	}
}

func TestWriteFleetScaleOutputDoesNotTruncateExistingHardlinkTarget(t *testing.T) {
	cfg := validTestConfig(t)
	outside, err := os.CreateTemp("/private/tmp", "fleet-scale-hardlink-target-")
	if err != nil {
		t.Fatal(err)
	}
	outsidePath := outside.Name()
	t.Cleanup(func() { _ = os.Remove(outsidePath) })
	if _, err := outside.WriteString("do-not-truncate"); err != nil {
		t.Fatal(err)
	}
	if err := outside.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outsidePath, cfg.output); err != nil {
		t.Fatal(err)
	}
	if err := writeFleetScaleOutput(cfg.outputRoot, cfg.output, []byte("new evidence\n")); err != nil {
		t.Fatal(err)
	}
	outsideBody, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(outsideBody) != "do-not-truncate" {
		t.Fatalf("external hardlink target was changed: %q", outsideBody)
	}
	outputBody, err := os.ReadFile(cfg.output)
	if err != nil {
		t.Fatal(err)
	}
	if string(outputBody) != "new evidence\n" {
		t.Fatalf("published evidence = %q", outputBody)
	}
}

func TestFleetDatabaseNamesAreSynthetic(t *testing.T) {
	for _, index := range []int{0, 99, 499} {
		if got := fleetDatabaseName(index); !strings.HasPrefix(got, "beads_perf_lab_scale_") {
			t.Fatalf("database %d = %q", index, got)
		}
	}
}
