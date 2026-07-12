// cluster-beads-probe diagnoses a fixed synthetic Beads mutation against the
// disposable loopback cluster. It accepts no SQL and never prints credentials.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

const (
	probeHost     = "127.0.0.1"
	probePort     = 13400
	probeDatabase = "beads_perf_lab_cluster_gateway_ae"
)

type report struct {
	Success    bool   `json:"success"`
	Stage      string `json:"stage"`
	ErrorClass string `json:"error_class,omitempty"`
	Error      string `json:"error,omitempty"`
	IssueID    string `json:"issue_id,omitempty"`
	WallMS     int64  `json:"wall_ms"`
	Warnings   int    `json:"warnings"`
}

func main() {
	var passwordFile, output, action, projectID, labID string
	flag.StringVar(&action, "action", "create", "fixed create or barrier diagnostic")
	flag.StringVar(&projectID, "project-id", "", "canonical synthetic database project UUID")
	flag.StringVar(&labID, "lab-id", "", "canonical synthetic cluster lab UUID")
	flag.StringVar(&passwordFile, "password-file", "", "0600 isolated project SQL-user password")
	flag.StringVar(&output, "output", "", "0600 JSON under the disposable evidence root")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "positional arguments are forbidden")
		os.Exit(2)
	}
	var outputPath string
	if output != "" {
		var err error
		outputPath, err = canonicalApprovedPath(output, probeEvidenceRoot, true)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid output path")
			os.Exit(2)
		}
	}
	started := time.Now()
	r := run(passwordFile, projectID, labID, action)
	r.WallMS = time.Since(started).Milliseconds()
	body, _ := json.MarshalIndent(r, "", "  ")
	body = append(body, '\n')
	if outputPath != "" {
		if err := writeEvidenceFile(outputPath, body); err != nil {
			fmt.Fprintln(os.Stderr, "write evidence failed")
			os.Exit(2)
		}
	} else {
		_, _ = os.Stdout.Write(body)
	}
	if !r.Success {
		os.Exit(1)
	}
}

func run(passwordFile, projectID, labID, action string) report {
	result := report{Stage: "validate"}
	if action != "create" && action != "barrier" {
		return failed(result, errors.New("unsupported action"))
	}
	if err := validateProbeEndpoint(probeHost, probePort, probeDatabase); err != nil {
		return failed(result, err)
	}
	user, err := labidentity.SQLUser(projectID)
	if err != nil {
		return failed(result, errors.New("project-id must be a canonical UUID"))
	}
	if !canonicalProbeUUID(labID) {
		return failed(result, errors.New("lab-id must be a canonical UUID"))
	}
	password, err := readPasswordFile(passwordFile, probePasswordRoot)
	if err != nil {
		return failed(result, err)
	}
	defer clear(password)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result.Stage = "provider"
	provider, err := uow.NewDirectDoltServerUOWProvider(ctx, uow.DirectDoltServerOptions{
		Host: probeHost, Port: probePort, Database: probeDatabase,
		User: user, Password: string(password), MaxOpenConns: 2, MaxIdleConns: 1,
	})
	if err != nil {
		return failed(result, err)
	}
	defer provider.Close(context.Background())
	result.Stage = "uow"
	uw, err := provider.NewUOW(ctx)
	if err != nil {
		return failed(result, err)
	}
	defer uw.Close(context.Background())
	result.Stage = "identity"
	if err := verifyProbeIdentity(ctx, uw, projectID, labID); err != nil {
		return failed(result, err)
	}
	if action == "barrier" {
		result.Stage = "barrier"
		if _, err := uw.RawSQLUseCase().Exec(ctx, "CALL DOLT_COMMIT('--allow-empty', '-m', ?)", "beads-perf-lab cluster raw-uow barrier diagnostic"); err != nil {
			return failed(result, err)
		}
		result.Stage = "warnings"
		warnings, err := uw.RawSQLUseCase().Query(ctx, "SHOW WARNINGS")
		if err != nil {
			return failed(result, err)
		}
		result.Warnings = len(warnings.Rows)
		result.Stage = "complete"
		result.Success = result.Warnings > 0
		if !result.Success {
			return failed(result, errors.New("replication warning was not visible on raw UOW"))
		}
		return result
	}
	result.Stage = "create"
	created, err := uw.IssueUseCase().CreateIssue(ctx, domain.CreateIssueParams{Issue: &types.Issue{
		Title: "[beads-perf-lab] cluster direct diagnostic", Description: "synthetic",
		IssueType: types.TypeTask, Status: types.StatusOpen, Priority: 2,
	}}, "cluster-diagnostic")
	if err != nil {
		return failed(result, err)
	}
	result.IssueID = created.Issue.ID
	result.Stage = "commit"
	if err := uw.Commit(ctx, "beads-perf-lab cluster direct diagnostic"); err != nil {
		return failed(result, err)
	}
	result.Stage = "complete"
	result.Success = true
	return result
}

func failed(result report, err error) report {
	result.ErrorClass = "other"
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		result.ErrorClass = fmt.Sprintf("mysql_%d", mysqlErr.Number)
	}
	detail := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(detail) > 300 {
		detail = detail[:300]
	}
	result.Error = detail
	return result
}
