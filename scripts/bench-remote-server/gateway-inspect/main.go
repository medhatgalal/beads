// gateway-inspect performs a fixed, read-only correctness inspection against
// the disposable gateway database. It accepts no arbitrary SQL.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const (
	receiptPrefix     = "_beads_perf_gateway_receipt_v1/"
	outboxPrefix      = "_beads_perf_gateway_outbox_v1/"
	terminalOutcomeV2 = "_beads_perf_gateway_terminal_outcome_v2/"
	terminalOutboxV2  = "_beads_perf_gateway_terminal_outbox_v2/"
)

type report struct {
	SchemaVersion        int       `json:"schema_version"`
	InspectedAt          time.Time `json:"inspected_at"`
	Database             string    `json:"database"`
	Port                 int       `json:"port"`
	ProjectID            string    `json:"project_id"`
	SQLUser              string    `json:"sql_user"`
	Head                 string    `json:"head"`
	OperationID          string    `json:"operation_id,omitempty"`
	IssueID              string    `json:"issue_id,omitempty"`
	TitlePrefix          string    `json:"title_prefix,omitempty"`
	IssueCount           int       `json:"issue_count"`
	DependencyCount      int       `json:"dependency_count"`
	DependencyScope      string    `json:"dependency_scope"`
	EventCount           int       `json:"event_count"`
	ReceiptCount         int       `json:"receipt_count"`
	OutboxMarkerCount    int       `json:"outbox_marker_count"`
	OperationCommitCount int       `json:"operation_commit_count"`
	DirtyTableCount      int       `json:"dirty_table_count"`
	Passed               bool      `json:"passed"`
}

var databaseNamePattern = regexp.MustCompile(`^beads_perf_lab_[a-z0-9][a-z0-9_]{0,48}$`)

func main() {
	var database, projectID, passwordFile, operationID, issueID, titlePrefix string
	var port, expectedIssues, expectedDependencies, expectedEvents int
	flag.IntVar(&port, "port", 13360, "isolated loopback Dolt SQL port (3306 and 3307 are forbidden)")
	flag.StringVar(&database, "database", "", "beads_perf_lab_ database")
	flag.StringVar(&projectID, "project-id", "", "expected database project UUID")
	flag.StringVar(&passwordFile, "password-file", "", "0600 Dolt password file")
	flag.StringVar(&operationID, "operation-id", "", "optional gateway operation UUID")
	flag.StringVar(&issueID, "issue-id", "", "optional exact synthetic issue ID")
	flag.StringVar(&titlePrefix, "title-prefix", "", "optional literal synthetic title prefix")
	flag.IntVar(&expectedIssues, "expected-issues", -1, "optional exact issue count")
	flag.IntVar(&expectedDependencies, "expected-dependencies", -1, "optional exact dependency count")
	flag.IntVar(&expectedEvents, "expected-events", -1, "optional exact event count")
	flag.Parse()
	if flag.NArg() != 0 || passwordFile == "" {
		fatal(fmt.Errorf("database, project-id, and password-file are required lab values"))
	}
	if _, err := inspectionSQLConfig(port, database, projectID, "placeholder"); err != nil {
		fatal(err)
	}
	if titlePrefix != "" && !strings.HasPrefix(titlePrefix, "[beads-perf-lab]") {
		fatal(fmt.Errorf("title-prefix must be synthetic"))
	}
	if len(issueID) > 128 || strings.ContainsAny(issueID, "\r\n\x00") {
		fatal(fmt.Errorf("issue-id is invalid"))
	}
	password, err := readSecret(passwordFile)
	if err != nil {
		fatal(err)
	}
	defer zero(password)
	result, err := inspect(context.Background(), port, database, projectID, string(password), operationID, issueID, titlePrefix,
		expectedIssues, expectedDependencies, expectedEvents)
	if err != nil {
		fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fatal(err)
	}
	if !result.Passed {
		os.Exit(1)
	}
}

func inspect(ctx context.Context, port int, database, expectedProjectID, password, operationID, issueID, titlePrefix string,
	expectedIssues, expectedDependencies, expectedEvents int) (report, error) {
	driverConfig, err := inspectionSQLConfig(port, database, expectedProjectID, password)
	if err != nil {
		return report{}, err
	}
	connector, err := mysql.NewConnector(driverConfig)
	if err != nil {
		return report{}, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return report{}, fmt.Errorf("connect: %w", err)
	}
	result := report{
		SchemaVersion: 1, InspectedAt: time.Now().UTC(), Database: database, Port: port,
		ProjectID: expectedProjectID, SQLUser: driverConfig.User, OperationID: operationID,
		IssueID: issueID, TitlePrefix: titlePrefix,
	}
	result.DependencyScope = "database-total"
	var actualProjectID string
	if err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key`='_project_id'").Scan(&actualProjectID); err != nil {
		return report{}, fmt.Errorf("project identity: %w", err)
	}
	if err := verifyInspectionProjectBinding(expectedProjectID, actualProjectID, driverConfig.User); err != nil {
		return report{}, err
	}
	if err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&result.Head); err != nil {
		return report{}, fmt.Errorf("head: %w", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&result.DirtyTableCount); err != nil {
		return report{}, fmt.Errorf("dirty tables: %w", err)
	}
	if issueID != "" {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues WHERE id=?", issueID).Scan(&result.IssueCount); err != nil {
			return report{}, fmt.Errorf("issues: %w", err)
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE issue_id=?", issueID).Scan(&result.EventCount); err != nil {
			return report{}, fmt.Errorf("events: %w", err)
		}
	} else if titlePrefix != "" {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues WHERE LEFT(title, CHAR_LENGTH(?))=?", titlePrefix, titlePrefix).Scan(&result.IssueCount); err != nil {
			return report{}, fmt.Errorf("issues: %w", err)
		}
		if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM events e JOIN issues i ON i.id=e.issue_id
WHERE LEFT(i.title, CHAR_LENGTH(?))=?`, titlePrefix, titlePrefix).Scan(&result.EventCount); err != nil {
			return report{}, fmt.Errorf("events: %w", err)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dependencies").Scan(&result.DependencyCount); err != nil {
		return report{}, fmt.Errorf("dependencies: %w", err)
	}
	if operationID != "" {
		if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM metadata
WHERE (`+"`key`"+` LIKE ? OR `+"`key`"+` LIKE ?) AND INSTR(value, ?) > 0`,
			receiptPrefix+"%", terminalOutcomeV2+"%", `"operation_id":"`+operationID+`"`).Scan(&result.ReceiptCount); err != nil {
			return report{}, fmt.Errorf("receipt: %w", err)
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM metadata WHERE `key` IN (?, ?)",
			outboxPrefix+operationID+"/0", terminalOutboxV2+operationID+"/0").Scan(&result.OutboxMarkerCount); err != nil {
			return report{}, fmt.Errorf("outbox marker: %w", err)
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_log WHERE message IN (?, ?)",
			"beads-perf-lab gateway "+operationID,
			"beads-perf-lab gateway terminal failure "+operationID).Scan(&result.OperationCommitCount); err != nil {
			return report{}, fmt.Errorf("operation commit: %w", err)
		}
	}
	result.Passed = result.DirtyTableCount == 0 && result.ProjectID == expectedProjectID
	if operationID != "" {
		result.Passed = result.Passed && result.ReceiptCount == 1 && result.OutboxMarkerCount == 1 && result.OperationCommitCount == 1
	}
	if expectedIssues >= 0 {
		result.Passed = result.Passed && result.IssueCount == expectedIssues
	}
	if expectedDependencies >= 0 {
		result.Passed = result.Passed && result.DependencyCount == expectedDependencies
	}
	if expectedEvents >= 0 {
		result.Passed = result.Passed && result.EventCount == expectedEvents
	}
	return result, nil
}

func verifyInspectionProjectBinding(expectedProjectID, actualProjectID, sqlUser string) error {
	expectedUser, err := labidentity.SQLUser(expectedProjectID)
	if err != nil || sqlUser != expectedUser || actualProjectID != expectedProjectID {
		return fmt.Errorf("project identity mismatch")
	}
	return nil
}

func inspectionSQLConfig(port int, database, projectID, password string) (*mysql.Config, error) {
	if net.ParseIP("127.0.0.1") == nil || port < 1 || port > 65535 || port == 3306 || port == 3307 {
		return nil, fmt.Errorf("loopback SQL port must be isolated and must not be 3306 or 3307")
	}
	if !databaseNamePattern.MatchString(database) {
		return nil, fmt.Errorf("database must be a bounded beads_perf_lab_ identifier")
	}
	user, err := labidentity.SQLUser(projectID)
	if err != nil {
		return nil, fmt.Errorf("project-id must be a canonical UUID")
	}
	return &mysql.Config{
		User: user, Passwd: password, Net: "tcp",
		Addr: net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port)), DBName: database,
		Timeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
		ParseTime: true, AllowNativePasswords: true, TLSConfig: "false",
	}, nil
}

func readSecret(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("password file must be a private regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	secret := append([]byte(nil), bytes.TrimSpace(body)...)
	zero(body)
	if len(secret) == 0 {
		return nil, fmt.Errorf("password is empty")
	}
	return secret, nil
}

func zero(body []byte) {
	for i := range body {
		body[i] = 0
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gateway-inspect:", err)
	os.Exit(2)
}
