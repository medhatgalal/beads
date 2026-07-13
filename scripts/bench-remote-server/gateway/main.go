package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/steveyegge/beads/internal/lockfile"
	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/scripts/bench-remote-server/internal/labidentity"
)

type gatewayConfig struct {
	Listen                 string
	RuntimeRoot            string
	Host                   string
	Port                   int
	Database               string
	ProjectID              string
	LabID                  string
	Subject                string
	PasswordFile           string
	TokenFile              string
	IdempotencyKeyFile     string
	QueuePath              string
	MaxOpen                int
	MaxIdle                int
	ClusterAckGuard        bool
	ClusterRoleEpoch       int
	ClusterStandbys        string
	EnableLegacyOperations bool
}

func main() {
	var cfg gatewayConfig
	flag.StringVar(&cfg.Listen, "listen", "127.0.0.1:7707", "loopback HTTP listen address")
	flag.StringVar(&cfg.RuntimeRoot, "runtime-root", "/var/lib/beads-perf-lab", "isolated gateway runtime root")
	flag.StringVar(&cfg.Host, "dolt-host", "127.0.0.1", "loopback Dolt host")
	flag.IntVar(&cfg.Port, "dolt-port", 13360, "isolated Dolt port (3306 and 3307 are forbidden)")
	flag.StringVar(&cfg.Database, "database", "", "existing beads_perf_lab_ database")
	flag.StringVar(&cfg.ProjectID, "project-id", "", "required database project identity")
	flag.StringVar(&cfg.LabID, "lab-id", "", "required synthetic database attestation UUID")
	flag.StringVar(&cfg.Subject, "subject", "", "server-owned agent identity")
	flag.StringVar(&cfg.PasswordFile, "password-file", "", "0600 Dolt password file")
	flag.StringVar(&cfg.TokenFile, "token-file", "", "0600 gateway bearer token file")
	flag.StringVar(&cfg.IdempotencyKeyFile, "idempotency-key-file", "", "0600 stable idempotency HMAC key file")
	flag.StringVar(&cfg.QueuePath, "queue", "", "durable SQLite queue path")
	flag.IntVar(&cfg.MaxOpen, "max-open", 8, "maximum Dolt SQL connections")
	flag.IntVar(&cfg.MaxIdle, "max-idle", 8, "maximum idle Dolt SQL connections")
	flag.BoolVar(&cfg.ClusterAckGuard, "cluster-ack-guard", false, "require commit-specific Dolt cluster acknowledgement before terminal HTTP 200")
	flag.IntVar(&cfg.ClusterRoleEpoch, "cluster-role-epoch", 0, "expected positive Dolt cluster role epoch when cluster acknowledgement guard is enabled")
	flag.StringVar(&cfg.ClusterStandbys, "cluster-required-standbys", "", "comma-separated exact Dolt standby remote set when cluster acknowledgement guard is enabled")
	flag.BoolVar(&cfg.EnableLegacyOperations, "enable-legacy-operations", false, "lab-only compatibility lane; terminal operations are the safe default")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Printf("gateway stopped error_class=%s", safeLogError(err))
		os.Exit(1)
	}
}

func run(cfg gatewayConfig) error {
	if err := validateGatewayConfig(cfg); err != nil {
		return err
	}
	sqlUser, err := labidentity.SQLUser(cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("derive project SQL user: %w", err)
	}
	_ = syscall.Umask(0o077)
	for _, path := range []string{cfg.PasswordFile, cfg.TokenFile, cfg.IdempotencyKeyFile, cfg.QueuePath} {
		if err := verifyRuntimeLabPath(cfg.RuntimeRoot, path); err != nil {
			return err
		}
	}
	lock, err := acquireGatewayLock(cfg.QueuePath + ".lock")
	if err != nil {
		return err
	}
	defer func() {
		_ = lockfile.FlockUnlock(lock)
		_ = lock.Close()
	}()
	password, err := readSecretFile(cfg.PasswordFile, 1)
	if err != nil {
		return fmt.Errorf("password file: %w", err)
	}
	defer zero(password)
	token, err := readSecretFile(cfg.TokenFile, 32)
	if err != nil {
		return fmt.Errorf("token file: %w", err)
	}
	defer zero(token)
	idempotencyKey, err := readSecretFile(cfg.IdempotencyKeyFile, 32)
	if err != nil {
		return fmt.Errorf("idempotency key file: %w", err)
	}
	defer zero(idempotencyKey)
	if bytes.Equal(token, idempotencyKey) {
		return fmt.Errorf("bearer token and idempotency key must be distinct")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	provider, err := uow.NewDirectDoltServerUOWProvider(ctx, uow.DirectDoltServerOptions{
		Host: cfg.Host, Port: cfg.Port, Database: cfg.Database, User: sqlUser, AuthSecret: string(password),
		MaxOpenConns: cfg.MaxOpen, MaxIdleConns: cfg.MaxIdle,
		ConnMaxLifetime: 30 * time.Minute, ConnMaxIdleTime: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	clusterGuard, err := clusterAckGuardConfigFromGateway(cfg)
	if err != nil {
		_ = provider.Close(context.Background())
		return err
	}
	backend, err := newUOWBackend(ctx, provider, cfg.ProjectID, cfg.Database, cfg.LabID, clusterGuard)
	if err != nil {
		_ = provider.Close(context.Background())
		return err
	}
	queue, err := openOperationQueue(ctx, cfg.QueuePath, queueBinding{
		ProjectID: cfg.ProjectID, Database: cfg.Database,
		IdempotencyKeyID: hmacHex(idempotencyKey, "binding\x00idempotency-v1"),
		SubjectID:        hmacHex(idempotencyKey, "subject\x00"+cfg.Subject),
	})
	if err != nil {
		_ = backend.Close(context.Background())
		return err
	}
	service, err := newGatewayService(ctx, cfg.ProjectID, gatewayAuth{
		BearerToken: token, StableKey: idempotencyKey, Subject: cfg.Subject,
	}, queue, backend)
	if err != nil {
		_ = queue.Close()
		_ = backend.Close(context.Background())
		return err
	}
	service.allowLegacyOperations = cfg.EnableLegacyOperations

	httpServer := &http.Server{
		Addr: cfg.Listen, Handler: service,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 11 * time.Minute, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("gateway ready on %s for database %s", cfg.Listen, cfg.Database)
		errCh <- httpServer.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errCh:
	}
	service.beginDrain()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	cancel()
	var forceCloseErr error
	if shutdownErr != nil {
		forceCloseErr = httpServer.Close()
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	drainErr := service.waitHandlers(drainCtx)
	drainCancel()
	if drainErr != nil {
		// Never close dependencies or zero secrets underneath a still-running
		// handler. The process exits after this error and the OS reclaims them.
		return errors.Join(serveErr, shutdownErr, forceCloseErr, drainErr)
	}
	service.Close()
	queueErr := queue.Close()
	backendErr := backend.Close(shutdownCtx)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr, forceCloseErr, queueErr, backendErr)
}

func validateGatewayConfig(cfg gatewayConfig) error {
	if cfg.RuntimeRoot != "/var/lib/beads-perf-lab" && cfg.RuntimeRoot != "/private/tmp/beads-perf-lab-runtime" {
		return fmt.Errorf("runtime-root must be an approved isolated lab root")
	}
	listenHost, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil || net.ParseIP(listenHost) == nil || !net.ParseIP(listenHost).IsLoopback() {
		return fmt.Errorf("listen address must be a numeric loopback host and port")
	}
	hostIP := net.ParseIP(cfg.Host)
	if hostIP == nil || !hostIP.IsLoopback() {
		return fmt.Errorf("Dolt host must be a numeric loopback address")
	}
	if cfg.Port < 1 || cfg.Port > 65535 || cfg.Port == 3306 || cfg.Port == 3307 {
		return fmt.Errorf("Dolt port must be isolated and must not be 3306 or 3307")
	}
	if !strings.HasPrefix(cfg.Database, "beads_perf_lab_") {
		return fmt.Errorf("database must use the beads_perf_lab_ prefix")
	}
	if _, err := labidentity.SQLUser(cfg.ProjectID); err != nil {
		return fmt.Errorf("project-id must be a canonical UUID")
	}
	if _, err := uuid.Parse(cfg.LabID); err != nil {
		return fmt.Errorf("lab-id must be a UUID")
	}
	if cfg.Subject == "" || cfg.Subject != strings.TrimSpace(cfg.Subject) || len(cfg.Subject) > 200 || !utf8.ValidString(cfg.Subject) ||
		strings.ContainsAny(cfg.Subject, "\r\n\x00") {
		return fmt.Errorf("subject must be a non-empty server-owned identity")
	}
	if cfg.PasswordFile == "" || cfg.TokenFile == "" || cfg.IdempotencyKeyFile == "" || cfg.QueuePath == "" {
		return fmt.Errorf("password-file, token-file, idempotency-key-file, and queue are required")
	}
	if cfg.TokenFile == cfg.IdempotencyKeyFile {
		return fmt.Errorf("token-file and idempotency-key-file must be distinct")
	}
	for _, path := range []string{cfg.PasswordFile, cfg.TokenFile, cfg.IdempotencyKeyFile, cfg.QueuePath} {
		if !pathWithinLabRoot(cfg.RuntimeRoot, path) {
			return fmt.Errorf("runtime files must be under the configured lab root")
		}
	}
	if cfg.MaxOpen < 2 || cfg.MaxOpen > 16 || cfg.MaxIdle < 1 || cfg.MaxIdle > cfg.MaxOpen {
		return fmt.Errorf("connection pool bounds are invalid")
	}
	if _, err := clusterAckGuardConfigFromGateway(cfg); err != nil {
		return err
	}
	return nil
}

func verifyRuntimeLabPath(rootPath, path string) error {
	abs, err := filepath.Abs(path)
	if err != nil || !pathWithinLabRoot(rootPath, abs) {
		return fmt.Errorf("runtime path is outside the lab root")
	}
	root, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return fmt.Errorf("resolve lab root: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return fmt.Errorf("runtime parent must already exist: %w", err)
	}
	rel, err := filepath.Rel(root, parent)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("runtime parent resolves outside the lab root")
	}
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("runtime path must not be a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect runtime path: %w", err)
	}
	return nil
}

func readSecretFile(path string, minimum int) ([]byte, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("secret must be a regular file without group or world permissions")
	}
	body, err := os.ReadFile(abs)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(body)
	secret := append([]byte(nil), trimmed...)
	zero(body)
	if len(secret) < minimum || len(secret) > 4096 {
		zero(secret)
		return nil, fmt.Errorf("secret length is outside the accepted bounds")
	}
	return secret, nil
}

func zero(body []byte) {
	for i := range body {
		body[i] = 0
	}
}

func acquireGatewayLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open gateway lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure gateway lock: %w", err)
	}
	if err := lockfile.FlockExclusiveNonBlock(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("gateway singleton lock: %w", err)
	}
	return file, nil
}

func pathWithinLabRoot(rootPath, path string) bool {
	root, err := filepath.Abs(rootPath)
	if err != nil {
		return false
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == "." || rel == "" {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
