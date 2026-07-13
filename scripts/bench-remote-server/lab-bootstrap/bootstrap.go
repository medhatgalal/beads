package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
	"golang.org/x/sys/unix"
)

const (
	labAttestationKey = "_beads_perf_lab_attestation_v1"
	labUserHost       = "%"
	maxPasswordBytes  = 4096
)

var (
	databaseNameRE = regexp.MustCompile(`^beads_perf_lab_[a-z0-9][a-z0-9_]{0,48}$`)
	grantLineRE    = regexp.MustCompile(`(?i)^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+(.+)$`)
	requiredGrants = []string{"DELETE", "EXECUTE", "INSERT", "SELECT", "UPDATE"}
)

type bootstrapConfig struct {
	Host         string
	Port         int
	Database     string
	ProjectID    string
	LabID        string
	PasswordFile string
	Timeout      time.Duration
}

type labAttestation struct {
	Version      int    `json:"version"`
	Environment  string `json:"environment"`
	LabID        string `json:"lab_id"`
	DatabaseName string `json:"database_name"`
	ProjectID    string `json:"project_id"`
}

type bootstrapResult struct {
	Status          string   `json:"status"`
	Environment     string   `json:"environment"`
	DatabaseName    string   `json:"database_name"`
	ProjectID       string   `json:"project_id"`
	LabID           string   `json:"lab_id"`
	Account         string   `json:"account"`
	Privileges      []string `json:"database_privileges"`
	AttestationKey  string   `json:"attestation_key"`
	UserLoginTested bool     `json:"user_login_tested"`
}

func validateConfig(cfg bootstrapConfig) error {
	ip := net.ParseIP(cfg.Host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("host must be a numeric loopback address")
	}
	if cfg.Port < 1 || cfg.Port > 65535 || cfg.Port == 3306 || cfg.Port == 3307 {
		return fmt.Errorf("port must be isolated and must not be 3306 or 3307")
	}
	if !databaseNameRE.MatchString(cfg.Database) {
		return fmt.Errorf("database must be a lowercase beads_perf_lab_ identifier no longer than 64 characters")
	}
	if _, err := labidentity.SQLUser(cfg.ProjectID); err != nil {
		return fmt.Errorf("project-id must be a canonical UUID")
	}
	if !canonicalUUID(cfg.LabID) {
		return fmt.Errorf("lab-id must be a canonical UUID")
	}
	if cfg.PasswordFile == "" {
		return fmt.Errorf("password-file is required")
	}
	if cfg.Timeout <= 0 || cfg.Timeout > 5*time.Minute {
		return fmt.Errorf("timeout must be greater than zero and no more than five minutes")
	}
	return nil
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func canonicalAttestation(cfg bootstrapConfig) (labAttestation, []byte, error) {
	value := labAttestation{
		Version:      1,
		Environment:  "synthetic",
		LabID:        cfg.LabID,
		DatabaseName: cfg.Database,
		ProjectID:    cfg.ProjectID,
	}
	body, err := json.Marshal(value)
	if err != nil {
		return labAttestation{}, nil, fmt.Errorf("encode attestation: %w", err)
	}
	return value, body, nil
}

// readPasswordFile opens the final path component with O_NOFOLLOW and checks
// the mode on the opened descriptor, avoiding a symlink-swap between checking
// and reading the secret.
func readPasswordFile(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open password file: %w", err)
	}
	file := os.NewFile(uintptr(fd), "password-file")
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open password file: invalid descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect password file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("password file must be a regular file with mode exactly 0600")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxPasswordBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read password file: %w", err)
	}
	if len(body) > maxPasswordBytes {
		zero(body)
		return nil, fmt.Errorf("password file exceeds %d bytes", maxPasswordBytes)
	}
	secret := append([]byte(nil), bytes.TrimSpace(body)...)
	zero(body)
	if err := validateLabPassword(secret); err != nil {
		zero(secret)
		return nil, err
	}
	return secret, nil
}

func validateLabPassword(secret []byte) error {
	if len(secret) != 64 {
		return fmt.Errorf("password must contain exactly 64 lowercase hexadecimal characters")
	}
	for _, value := range secret {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return fmt.Errorf("password must contain exactly 64 lowercase hexadecimal characters")
		}
	}
	return nil
}

func openDB(cfg bootstrapConfig, user string, password []byte) (*sql.DB, error) {
	driverCfg := mysql.NewConfig()
	driverCfg.User = user
	driverCfg.Passwd = string(password)
	driverCfg.Net = "tcp"
	driverCfg.Addr = net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	driverCfg.DBName = cfg.Database
	driverCfg.Timeout = cfg.Timeout
	driverCfg.ReadTimeout = cfg.Timeout
	driverCfg.WriteTimeout = cfg.Timeout
	driverCfg.MultiStatements = false
	connector, err := mysql.NewConnector(driverCfg)
	driverCfg.Passwd = ""
	if err != nil {
		return nil, fmt.Errorf("construct SQL connector: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(cfg.Timeout)
	return db, nil
}

func runBootstrap(ctx context.Context, cfg bootstrapConfig) (bootstrapResult, error) {
	if err := validateConfig(cfg); err != nil {
		return bootstrapResult{}, err
	}
	password, err := readPasswordFile(cfg.PasswordFile)
	if err != nil {
		return bootstrapResult{}, err
	}
	defer zero(password)
	attestation, attestationJSON, err := canonicalAttestation(cfg)
	if err != nil {
		return bootstrapResult{}, err
	}
	labUser, err := labidentity.SQLUser(cfg.ProjectID)
	if err != nil {
		return bootstrapResult{}, fmt.Errorf("derive project SQL user: %w", err)
	}

	rootDB, err := openDB(cfg, "root", nil)
	if err != nil {
		return bootstrapResult{}, err
	}
	defer rootDB.Close()
	if err := rootDB.PingContext(ctx); err != nil {
		return bootstrapResult{}, fmt.Errorf("connect to loopback Dolt as passwordless root: %w", err)
	}
	conn, err := rootDB.Conn(ctx)
	if err != nil {
		return bootstrapResult{}, fmt.Errorf("reserve bootstrap connection: %w", err)
	}
	defer conn.Close()

	if err := verifyDatabaseIdentity(ctx, conn, cfg); err != nil {
		return bootstrapResult{}, err
	}
	if err := upsertAttestation(ctx, conn, attestation, attestationJSON); err != nil {
		return bootstrapResult{}, err
	}
	if err := configureLabUser(ctx, conn, labUser, cfg.Database, password); err != nil {
		return bootstrapResult{}, err
	}
	grants, err := readGrants(ctx, conn, labUser)
	if err != nil {
		return bootstrapResult{}, err
	}
	if err := validateGrants(grants, cfg.Database, labUser); err != nil {
		return bootstrapResult{}, fmt.Errorf("postcondition failed: %w", err)
	}
	if err := verifyLabLogin(ctx, cfg, labUser, password); err != nil {
		return bootstrapResult{}, err
	}

	return bootstrapResult{
		Status:          "bootstrapped",
		Environment:     "synthetic",
		DatabaseName:    cfg.Database,
		ProjectID:       cfg.ProjectID,
		LabID:           cfg.LabID,
		Account:         labUser + "@" + labUserHost,
		Privileges:      append([]string(nil), requiredGrants...),
		AttestationKey:  labAttestationKey,
		UserLoginTested: true,
	}, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func verifyDatabaseIdentity(ctx context.Context, conn queryer, cfg bootstrapConfig) error {
	var actualDatabase string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&actualDatabase); err != nil {
		return fmt.Errorf("read selected database identity: %w", err)
	}
	if actualDatabase != cfg.Database {
		return fmt.Errorf("selected database identity mismatch")
	}
	var actualProject string
	if err := conn.QueryRowContext(ctx,
		"SELECT value FROM metadata WHERE `key` = ?", "_project_id").Scan(&actualProject); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("project identity marker is missing")
		}
		return fmt.Errorf("read project identity: %w", err)
	}
	if actualProject != cfg.ProjectID {
		return fmt.Errorf("project identity mismatch")
	}
	return nil
}

func upsertAttestation(ctx context.Context, conn queryer, expected labAttestation, canonical []byte) (err error) {
	if _, err = conn.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("begin attestation transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	var existing string
	scanErr := conn.QueryRowContext(ctx,
		"SELECT value FROM metadata WHERE `key` = ?", labAttestationKey).Scan(&existing)
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return fmt.Errorf("read existing lab attestation: %w", scanErr)
	}
	if scanErr == nil {
		var current labAttestation
		dec := json.NewDecoder(strings.NewReader(existing))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&current); err != nil {
			return fmt.Errorf("existing lab attestation is malformed")
		}
		var trailing any
		if err := dec.Decode(&trailing); err != io.EOF {
			return fmt.Errorf("existing lab attestation has trailing data")
		}
		if current != expected {
			return fmt.Errorf("existing lab attestation belongs to a different lab identity")
		}
	}

	if _, err = conn.ExecContext(ctx,
		"INSERT INTO metadata (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
		labAttestationKey, string(canonical)); err != nil {
		return fmt.Errorf("upsert lab attestation: %w", err)
	}
	if _, err = conn.ExecContext(ctx,
		"CALL DOLT_COMMIT('--allow-empty', '-m', ?)", "beads-perf-lab: attest synthetic database"); err != nil {
		return fmt.Errorf("commit lab attestation: %w", err)
	}
	committed = true
	return nil
}

func configureLabUser(ctx context.Context, conn queryer, user, database string, password []byte) error {
	if err := validateLabPassword(password); err != nil {
		return err
	}
	// Dolt 2.1.10 does not accept a bind placeholder in CREATE/ALTER USER.
	// The exact lowercase-hex validation above makes this literal incapable of
	// terminating the quoted value or adding SQL syntax.
	passwordLiteral := string(password)
	account := "'" + user + "'@'" + labUserHost + "'"
	// CREATE is idempotent. Each project has a distinct principal, so stripping
	// this account cannot revoke grants from repositories bootstrapped earlier.
	if _, err := conn.ExecContext(ctx,
		"CREATE USER IF NOT EXISTS "+account+" IDENTIFIED BY '"+passwordLiteral+"'"); err != nil {
		// Do not wrap the server error: a parser may echo the secret-bearing DDL.
		return fmt.Errorf("ensure isolated lab user failed")
	}
	if _, err := conn.ExecContext(ctx,
		"REVOKE ALL PRIVILEGES, GRANT OPTION FROM "+account); err != nil {
		return fmt.Errorf("clear prior lab-user privileges: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		"ALTER USER "+account+" IDENTIFIED BY '"+passwordLiteral+"'"); err != nil {
		// Do not wrap the server error: a parser may echo the secret-bearing DDL.
		return fmt.Errorf("rotate isolated lab-user credential failed")
	}
	grantSQL := "GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `" + database + "`.* TO " + account
	if _, err := conn.ExecContext(ctx, grantSQL); err != nil {
		return fmt.Errorf("grant database-scoped gateway privileges: %w", err)
	}
	return nil
}

func readGrants(ctx context.Context, conn queryer, user string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SHOW GRANTS FOR '"+user+"'@'%'")
	if err != nil {
		return nil, fmt.Errorf("inspect lab-user grants: %w", err)
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, fmt.Errorf("scan lab-user grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read lab-user grants: %w", err)
	}
	return grants, nil
}

func validateGrants(grants []string, database, user string) error {
	if len(grants) == 0 {
		return fmt.Errorf("SHOW GRANTS returned no rows")
	}
	found := make(map[string]bool, len(requiredGrants))
	allowed := make(map[string]bool, len(requiredGrants))
	for _, privilege := range requiredGrants {
		allowed[privilege] = true
	}
	for _, raw := range grants {
		line := strings.TrimSpace(raw)
		upper := strings.ToUpper(line)
		if strings.Contains(upper, "WITH GRANT OPTION") {
			return fmt.Errorf("grant option is forbidden")
		}
		matches := grantLineRE.FindStringSubmatch(line)
		if len(matches) != 4 {
			return fmt.Errorf("unexpected grant form")
		}
		if !grantTargetIsLabAccount(matches[3], user) {
			return fmt.Errorf("grant targets an unexpected account")
		}
		privileges := splitPrivileges(matches[1])
		scope := strings.ReplaceAll(strings.TrimSpace(matches[2]), "`", "")
		if scope == "*.*" {
			if len(privileges) != 1 || privileges[0] != "USAGE" {
				return fmt.Errorf("global privileges are forbidden")
			}
			continue
		}
		if scope != database+".*" {
			return fmt.Errorf("cross-database or non-database-scoped grant is forbidden")
		}
		for _, privilege := range privileges {
			if privilege == "FILE" {
				return fmt.Errorf("global FILE privilege is forbidden")
			}
			if !allowed[privilege] {
				return fmt.Errorf("unexpected database privilege %s", privilege)
			}
			found[privilege] = true
		}
	}
	var missing []string
	for _, privilege := range requiredGrants {
		if !found[privilege] {
			missing = append(missing, privilege)
		}
	}
	if len(missing) != 0 {
		return fmt.Errorf("required database privileges are missing: %s", strings.Join(missing, ","))
	}
	return nil
}

func grantTargetIsLabAccount(value, user string) bool {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return false
	}
	target := strings.NewReplacer("`", "", "'", "", `"`, "").Replace(fields[0])
	return strings.EqualFold(target, user+"@"+labUserHost)
}

func splitPrivileges(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		result = append(result, strings.ToUpper(strings.TrimSpace(part)))
	}
	sort.Strings(result)
	return result
}

func verifyLabLogin(ctx context.Context, cfg bootstrapConfig, user string, password []byte) error {
	db, err := openDB(cfg, user, password)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("verify isolated lab-user login: %w", err)
	}
	var database, project string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		return fmt.Errorf("verify lab-user database identity: %w", err)
	}
	if database != cfg.Database {
		return fmt.Errorf("lab-user database identity mismatch")
	}
	if err := db.QueryRowContext(ctx,
		"SELECT value FROM metadata WHERE `key` = ?", "_project_id").Scan(&project); err != nil {
		return fmt.Errorf("verify lab-user project identity: %w", err)
	}
	if project != cfg.ProjectID {
		return fmt.Errorf("lab-user project identity mismatch")
	}
	return nil
}

func zero(body []byte) {
	for i := range body {
		body[i] = 0
	}
}
