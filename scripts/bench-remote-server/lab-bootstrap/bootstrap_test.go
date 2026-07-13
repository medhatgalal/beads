package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const (
	testProjectID = "11111111-1111-4111-8111-111111111111"
	testLabID     = "22222222-2222-4222-8222-222222222222"
	testLabUser   = "11111111111141118111111111111111"
)

func validConfig() bootstrapConfig {
	return bootstrapConfig{
		Host: "127.0.0.1", Port: 13360,
		Database:  "beads_perf_lab_bootstrap_test",
		ProjectID: testProjectID, LabID: testLabID,
		PasswordFile: "/tmp/password", Timeout: 30 * time.Second,
	}
}

func TestValidateConfigAcceptsNumericLoopback(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "127.12.34.56", "::1"} {
		cfg := validConfig()
		cfg.Host = host
		if err := validateConfig(cfg); err != nil {
			t.Fatalf("host %q: %v", host, err)
		}
	}
}

func TestValidateConfigRejectsUnsafeInputs(t *testing.T) {
	tests := map[string]func(*bootstrapConfig){
		"hostname":                  func(c *bootstrapConfig) { c.Host = "localhost" },
		"external host":             func(c *bootstrapConfig) { c.Host = "10.0.0.4" },
		"zero port":                 func(c *bootstrapConfig) { c.Port = 0 },
		"large port":                func(c *bootstrapConfig) { c.Port = 65536 },
		"production SQL port":       func(c *bootstrapConfig) { c.Port = 3306 },
		"production Dolt port":      func(c *bootstrapConfig) { c.Port = 3307 },
		"wrong prefix":              func(c *bootstrapConfig) { c.Database = "production" },
		"empty suffix":              func(c *bootstrapConfig) { c.Database = "beads_perf_lab_" },
		"SQL punctuation":           func(c *bootstrapConfig) { c.Database = "beads_perf_lab_x`;DROP" },
		"uppercase db":              func(c *bootstrapConfig) { c.Database = "beads_perf_lab_X" },
		"noncanonical project UUID": func(c *bootstrapConfig) { c.ProjectID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"nil project UUID":          func(c *bootstrapConfig) { c.ProjectID = "00000000-0000-0000-0000-000000000000" },
		"bad lab UUID":              func(c *bootstrapConfig) { c.LabID = "not-a-uuid" },
		"missing password":          func(c *bootstrapConfig) { c.PasswordFile = "" },
		"zero timeout":              func(c *bootstrapConfig) { c.Timeout = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := validateConfig(cfg); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestCanonicalAttestationIsExact(t *testing.T) {
	cfg := validConfig()
	_, body, err := canonicalAttestation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":1,"environment":"synthetic","lab_id":"22222222-2222-4222-8222-222222222222","database_name":"beads_perf_lab_bootstrap_test","project_id":"11111111-1111-4111-8111-111111111111"}`
	if string(body) != want {
		t.Fatalf("canonical attestation mismatch\n got: %s\nwant: %s", body, want)
	}
}

func TestLabUserForProjectIsStableAndProjectScoped(t *testing.T) {
	got, err := labidentity.SQLUser(testProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if got != testLabUser {
		t.Fatalf("derived user = %q, want %q", got, testLabUser)
	}
	other, err := labidentity.SQLUser("33333333-3333-4333-8333-333333333333")
	if err != nil {
		t.Fatal(err)
	}
	if other == testLabUser {
		t.Fatal("different projects shared a SQL principal")
	}
}

func TestVerifyDatabaseIdentityRejectsCrossProjectDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := validConfig()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT DATABASE()")).
		WillReturnRows(sqlmock.NewRows([]string{"database"}).AddRow(cfg.Database))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT value FROM metadata WHERE `key` = ?")).
		WithArgs("_project_id").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("33333333-3333-4333-8333-333333333333"))
	if err := verifyDatabaseIdentity(context.Background(), db, cfg); err == nil || !strings.Contains(err.Error(), "project identity mismatch") {
		t.Fatalf("cross-project identity error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadPasswordFileRequiresExact0600RegularFile(t *testing.T) {
	dir := t.TempDir()
	want := strings.Repeat("a1", 32)
	good := filepath.Join(dir, "password")
	if err := os.WriteFile(good, []byte(want+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := readPasswordFile(good)
	if err != nil {
		t.Fatal(err)
	}
	defer zero(secret)
	if string(secret) != want {
		t.Fatalf("got %q", secret)
	}

	permissive := filepath.Join(dir, "permissive")
	if err := os.WriteFile(permissive, []byte("test-secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(permissive, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readPasswordFile(permissive); err == nil {
		t.Fatal("expected permissive mode to fail")
	}

	symlink := filepath.Join(dir, "symlink")
	if err := os.Symlink(good, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readPasswordFile(symlink); err == nil {
		t.Fatal("expected symlink to fail")
	}
}

func TestValidateLabPasswordRejectsWrongLengthAndAlphabet(t *testing.T) {
	if err := validateLabPassword([]byte(strings.Repeat("a1", 32))); err != nil {
		t.Fatalf("valid password rejected: %v", err)
	}
	tests := map[string]string{
		"empty":      "",
		"too short":  strings.Repeat("a", 63),
		"too long":   strings.Repeat("a", 65),
		"uppercase":  strings.Repeat("A", 64),
		"non-hex":    strings.Repeat("g", 64),
		"quote":      strings.Repeat("a", 63) + "'",
		"semicolon":  strings.Repeat("a", 63) + ";",
		"whitespace": strings.Repeat("a", 63) + " ",
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateLabPassword([]byte(value)); err == nil {
				t.Fatal("expected password validation failure")
			}
		})
	}
}

type recordingQueryer struct {
	statements []string
	failAt     int
}

func (r *recordingQueryer) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	r.statements = append(r.statements, query)
	if r.failAt == len(r.statements) {
		return nil, errors.New("synthetic parser error: " + query)
	}
	return nil, nil
}

func (*recordingQueryer) QueryRowContext(context.Context, string, ...any) *sql.Row {
	panic("unexpected query")
}

func (*recordingQueryer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	panic("unexpected query")
}

func TestConfigureLabUserUsesValidatedLiteralAndRedactsDDLError(t *testing.T) {
	password := strings.Repeat("ab", 32)
	recorder := &recordingQueryer{}
	if err := configureLabUser(context.Background(), recorder, testLabUser, "beads_perf_lab_bootstrap_test", []byte(password)); err != nil {
		t.Fatal(err)
	}
	if len(recorder.statements) != 4 {
		t.Fatalf("statement count = %d, want 4", len(recorder.statements))
	}
	if !strings.Contains(recorder.statements[0], password) || !strings.Contains(recorder.statements[2], password) {
		t.Fatal("CREATE/ALTER did not contain the validated password literal")
	}
	for _, index := range []int{0, 2} {
		if strings.Contains(recorder.statements[index], "?") {
			t.Fatal("password-bearing DDL still contains a placeholder")
		}
	}

	failing := &recordingQueryer{failAt: 1}
	err := configureLabUser(context.Background(), failing, testLabUser, "beads_perf_lab_bootstrap_test", []byte(password))
	if err == nil {
		t.Fatal("expected synthetic parser failure")
	}
	if strings.Contains(err.Error(), password) {
		t.Fatal("returned error leaked password")
	}
}

func TestValidateGrantsAcceptsOnlyUsageAndNamedDatabase(t *testing.T) {
	grants := []string{
		"GRANT USAGE ON *.* TO `11111111111141118111111111111111`@`%`",
		"GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `beads_perf_lab_bootstrap_test`.* TO `11111111111141118111111111111111`@`%`",
	}
	if err := validateGrants(grants, "beads_perf_lab_bootstrap_test", testLabUser); err != nil {
		t.Fatal(err)
	}
}

func TestValidateGrantsRejectsLeakageAndExcessPrivilege(t *testing.T) {
	base := "GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `beads_perf_lab_bootstrap_test`.* TO `11111111111141118111111111111111`@`%`"
	tests := map[string][]string{
		"global FILE": {
			"GRANT FILE ON *.* TO `11111111111141118111111111111111`@`%`", base,
		},
		"global SELECT": {
			"GRANT SELECT ON *.* TO `11111111111141118111111111111111`@`%`", base,
		},
		"cross database": {
			base, "GRANT SELECT ON `beads_perf_lab_other`.* TO `11111111111141118111111111111111`@`%`",
		},
		"table scoped": {
			base, "GRANT SELECT ON `beads_perf_lab_bootstrap_test`.`metadata` TO `11111111111141118111111111111111`@`%`",
		},
		"CREATE privilege": {
			"GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE, CREATE ON `beads_perf_lab_bootstrap_test`.* TO `11111111111141118111111111111111`@`%`",
		},
		"grant option": {
			base + " WITH GRANT OPTION",
		},
		"role": {
			base, "GRANT `some_role`@`%` TO `11111111111141118111111111111111`@`%`",
		},
		"wrong account": {
			"GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `beads_perf_lab_bootstrap_test`.* TO `other_user`@`%`",
		},
		"missing EXECUTE": {
			"GRANT SELECT, INSERT, UPDATE, DELETE ON `beads_perf_lab_bootstrap_test`.* TO `11111111111141118111111111111111`@`%`",
		},
	}
	for name, grants := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateGrants(grants, "beads_perf_lab_bootstrap_test", testLabUser); err == nil {
				t.Fatal("expected grant validation failure")
			}
		})
	}
}

func TestBootstrapIntegration(t *testing.T) {
	if os.Getenv("BEADS_PERF_LAB_BOOTSTRAP_INTEGRATION") != "1" {
		t.Skip("set BEADS_PERF_LAB_BOOTSTRAP_INTEGRATION=1 with the lab variables to run")
	}
	cfg := bootstrapConfig{
		Host:         os.Getenv("BEADS_PERF_LAB_DOLT_HOST"),
		Database:     os.Getenv("BEADS_PERF_LAB_DATABASE"),
		ProjectID:    os.Getenv("BEADS_PERF_LAB_PROJECT_ID"),
		LabID:        os.Getenv("BEADS_PERF_LAB_ID"),
		PasswordFile: os.Getenv("BEADS_PERF_LAB_PASSWORD_FILE"),
		Timeout:      30 * time.Second,
	}
	if _, err := fmt.Sscanf(os.Getenv("BEADS_PERF_LAB_DOLT_PORT"), "%d", &cfg.Port); err != nil {
		t.Fatalf("parse BEADS_PERF_LAB_DOLT_PORT: %v", err)
	}
	result, err := runBootstrap(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.UserLoginTested || result.DatabaseName != cfg.Database {
		t.Fatalf("unexpected result: %+v", result)
	}
}
