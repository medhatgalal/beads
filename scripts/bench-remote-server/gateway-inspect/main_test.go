package main

import (
	"strings"
	"testing"
)

const inspectProjectID = "093828a8-233f-4fc9-a8b7-5b5f77a48c0b"

func TestInspectionSQLConfigBindsCompleteProjectPrincipal(t *testing.T) {
	cfg, err := inspectionSQLConfig(13360, "beads_perf_lab_inspect", inspectProjectID, "secret")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.ReplaceAll(inspectProjectID, "-", "")
	if cfg.User != want || len(cfg.User) != 32 {
		t.Fatalf("SQL user = %q, want %q", cfg.User, want)
	}

	other, err := inspectionSQLConfig(13360, "beads_perf_lab_inspect", "80d22c54-066c-4f89-b6e8-d94e19b4e902", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if other.User == cfg.User {
		t.Fatal("cross-project inspector reused a SQL principal")
	}
}

func TestInspectionProjectBindingRejectsCrossProjectDatabaseAndUser(t *testing.T) {
	user := strings.ReplaceAll(inspectProjectID, "-", "")
	if err := verifyInspectionProjectBinding(inspectProjectID, inspectProjectID, user); err != nil {
		t.Fatalf("exact binding rejected: %v", err)
	}
	otherProject := "80d22c54-066c-4f89-b6e8-d94e19b4e902"
	if err := verifyInspectionProjectBinding(inspectProjectID, otherProject, user); err == nil {
		t.Fatal("cross-project database marker was accepted")
	}
	if err := verifyInspectionProjectBinding(inspectProjectID, inspectProjectID, strings.ReplaceAll(otherProject, "-", "")); err == nil {
		t.Fatal("cross-project SQL principal was accepted")
	}
}

func TestInspectionSQLConfigRejectsUnsafeTargets(t *testing.T) {
	tests := []struct {
		name     string
		port     int
		database string
		project  string
	}{
		{name: "production SQL port", port: 3306, database: "beads_perf_lab_inspect", project: inspectProjectID},
		{name: "production Dolt port", port: 3307, database: "beads_perf_lab_inspect", project: inspectProjectID},
		{name: "database", port: 13360, database: "production", project: inspectProjectID},
		{name: "cross-project spelling", port: 13360, database: "beads_perf_lab_inspect", project: strings.ToUpper(inspectProjectID)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := inspectionSQLConfig(test.port, test.database, test.project, "secret"); err == nil {
				t.Fatal("unsafe inspection target was accepted")
			}
		})
	}
}
