package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const defaultFleetScaleOutputRoot = "/private/tmp/beads-fleet-scale-20260712"

const labControlIdentityQuery = `
SELECT COUNT(*), COALESCE(MAX(environment), ''), COALESCE(MAX(lab_id), '')
FROM beads_perf_lab_control.lab_identity`

type config struct {
	host         string
	port         int
	databases    int
	prewarm      int
	operations   int
	repetitions  int
	output       string
	outputRoot   string
	labID        string
	ackSynthetic bool
}

type report struct {
	SchemaVersion int            `json:"schema_version"`
	StartedAt     time.Time      `json:"started_at"`
	FinishedAt    time.Time      `json:"finished_at"`
	DoltVersion   string         `json:"dolt_version"`
	Config        map[string]any `json:"config"`
	Experiments   []experiment   `json:"experiments"`
	Notes         []string       `json:"notes"`
}

type experiment struct {
	Name       string         `json:"name"`
	Trial      int            `json:"trial,omitempty"`
	Parameters map[string]any `json:"parameters,omitempty"`
	Metrics    map[string]any `json:"metrics"`
	Decision   string         `json:"decision"`
}

type latencySummary struct {
	Count int     `json:"count"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

type runStats struct {
	successes atomic.Int64
	retries   atomic.Int64
	failures  atomic.Int64
	mu        sync.Mutex
	latencies []time.Duration
	errors    map[string]int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.host, "host", "127.0.0.1", "Dolt loopback host")
	flag.IntVar(&cfg.port, "port", 13360, "Dolt loopback port")
	flag.IntVar(&cfg.databases, "databases", 100, "number of isolated fleet databases")
	flag.IntVar(&cfg.prewarm, "prewarm", 2, "idle connections to prewarm per database")
	flag.IntVar(&cfg.operations, "operations", 320, "operations per throughput cell")
	flag.IntVar(&cfg.repetitions, "repetitions", 3, "correctness repetitions")
	flag.StringVar(&cfg.outputRoot, "output-root", defaultFleetScaleOutputRoot, "existing non-symlink disposable evidence root")
	flag.StringVar(&cfg.output, "output", defaultFleetScaleOutputRoot+"/results.json", "structured result path below output-root")
	flag.StringVar(&cfg.labID, "lab-id", "", "canonical UUID from the pre-existing synthetic server control marker")
	flag.BoolVar(&cfg.ackSynthetic, "ack-create-synthetic-databases", false, "required acknowledgement that this command creates disposable synthetic databases")
	flag.Parse()

	if err := validateConfig(cfg); err != nil {
		fatalf("invalid experiment configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	r := report{
		SchemaVersion: 1,
		StartedAt:     time.Now().UTC(),
		Config: map[string]any{
			"host": cfg.host, "port": cfg.port, "databases": cfg.databases,
			"lab_id":               cfg.labID,
			"output_root":          cfg.outputRoot,
			"prewarm_per_database": cfg.prewarm, "operations_per_cell": cfg.operations,
			"correctness_repetitions": cfg.repetitions,
		},
		Notes: []string{
			"All databases are synthetic and use the beads_perf_lab_scale_ prefix.",
			"Raw Dolt probes isolate transaction semantics; they do not claim full Beads API parity.",
		},
	}

	admin := openDB(cfg, "")
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		fatalf("connect to isolated Dolt: %v", err)
	}
	if err := verifyLabControlIdentity(ctx, admin, cfg.labID); err != nil {
		fatalf("verify synthetic server identity before mutation: %v", err)
	}
	_ = admin.QueryRowContext(ctx, "SELECT DOLT_VERSION()").Scan(&r.DoltVersion)

	names, setupDuration, err := setupDatabases(ctx, cfg, admin)
	if err != nil {
		fatalf("setup databases: %v", err)
	}
	r.Experiments = append(r.Experiments, experiment{
		Name:       "fleet_database_setup",
		Parameters: map[string]any{"databases": len(names)},
		Metrics:    map[string]any{"wall_ms": ms(setupDuration)},
		Decision:   "measured_baseline",
	})

	pools, fleetMetrics, err := prewarmFleet(ctx, cfg, names)
	if err != nil {
		closePools(pools)
		fatalf("prewarm fleet: %v", err)
	}
	defer closePools(pools)
	fleetMetrics["server_processes"] = processCount(ctx, admin)
	r.Experiments = append(r.Experiments, experiment{
		Name:       "fleet_fixed_pool_multiplier",
		Parameters: map[string]any{"databases": len(names), "prewarm_each": cfg.prewarm, "max_open_each": 8},
		Metrics:    fleetMetrics,
		Decision:   "reject_unbounded_per_database_prewarm",
	})

	hot := openDB(cfg, names[0])
	hot.SetMaxOpenConns(128)
	hot.SetMaxIdleConns(8)
	defer hot.Close()

	for _, workers := range []int{1, 8, 32, 100} {
		stats := runCommitLoad(ctx, hot, nil, workers, cfg.operations, fmt.Sprintf("hot-%d", workers))
		r.Experiments = append(r.Experiments, throughputExperiment("hot_database_disjoint_commits", workers, 1, stats))
	}

	coldBaseline := runCommitLoad(ctx, pools[1], nil, 1, cfg.operations, "cold-isolated")
	coldBaselineSummary := statsLatency(coldBaseline)
	r.Experiments = append(r.Experiments,
		throughputExperiment("cold_database_isolated_commits", 1, 1, coldBaseline))

	noisyHot, noisyCold := runNoisyNeighbor(ctx, hot, pools[1], 100, cfg.operations*5, cfg.operations)
	noisyHotExperiment := throughputExperiment("noisy_neighbor_hot_side", 100, 1, noisyHot)
	noisyColdExperiment := throughputExperiment("noisy_neighbor_cold_side", 1, 1, noisyCold)
	noisyColdSummary := noisyColdExperiment.Metrics["latency"].(latencySummary)
	multiplier := 0.0
	if coldBaselineSummary.P95MS > 0 {
		multiplier = noisyColdSummary.P95MS / coldBaselineSummary.P95MS
	}
	noisyDecision := "cross_database_isolation_pass"
	if multiplier > 2 {
		noisyDecision = "cross_database_noisy_neighbor_observed"
	}
	r.Experiments = append(r.Experiments, experiment{
		Name: "hot_ae_cross_database_interference",
		Parameters: map[string]any{
			"hot_workers": 100, "hot_operations": cfg.operations * 5,
			"cold_workers": 1, "cold_operations": cfg.operations,
		},
		Metrics: map[string]any{
			"cold_isolated_p95_ms":  coldBaselineSummary.P95MS,
			"cold_under_hot_p95_ms": noisyColdSummary.P95MS,
			"cold_p95_multiplier":   multiplier,
			"hot_side":              noisyHotExperiment.Metrics,
			"cold_side":             noisyColdExperiment.Metrics,
		},
		Decision: noisyDecision,
	})

	multiWorkers := minInt(100, len(pools))
	multi := runCommitLoad(ctx, nil, pools[:multiWorkers], multiWorkers, cfg.operations, "fleet")
	r.Experiments = append(r.Experiments, throughputExperiment(
		"multi_database_disjoint_commits", multiWorkers, multiWorkers, multi,
	))

	for trial := 1; trial <= cfg.repetitions; trial++ {
		lost, err := runLostUpdate(ctx, hot, 16, trial)
		if err != nil {
			fatalf("lost update trial %d: %v", trial, err)
		}
		r.Experiments = append(r.Experiments, lost)

		cycle, err := runCycleWriteSkew(ctx, hot, trial)
		if err != nil {
			fatalf("cycle trial %d: %v", trial, err)
		}
		r.Experiments = append(r.Experiments, cycle)

		sameClaim, err := runClaimRace(ctx, hot, 16, true, trial)
		if err != nil {
			fatalf("same-actor claim trial %d: %v", trial, err)
		}
		r.Experiments = append(r.Experiments, sameClaim)

		distinctClaim, err := runClaimRace(ctx, hot, 16, false, trial)
		if err != nil {
			fatalf("distinct-actor claim trial %d: %v", trial, err)
		}
		r.Experiments = append(r.Experiments, distinctClaim)
	}

	r.FinishedAt = time.Now().UTC()
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatalf("marshal results: %v", err)
	}
	if err := writeFleetScaleOutput(cfg.outputRoot, cfg.output, append(body, '\n')); err != nil {
		fatalf("write results: %v", err)
	}
	fmt.Printf("wrote %s with %d experiments\n", cfg.output, len(r.Experiments))
}

func validateConfig(cfg config) error {
	ip := net.ParseIP(cfg.host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("host must be a numeric loopback address")
	}
	if cfg.port < 1 || cfg.port > 65535 || cfg.port == 3306 || cfg.port == 3307 {
		return fmt.Errorf("port must be isolated and must not be 3306 or 3307")
	}
	if !cfg.ackSynthetic {
		return fmt.Errorf("--ack-create-synthetic-databases is required")
	}
	parsedLabID, err := uuid.Parse(cfg.labID)
	if err != nil || parsedLabID == uuid.Nil || parsedLabID.String() != cfg.labID {
		return fmt.Errorf("--lab-id must be a non-nil canonical UUID")
	}
	if cfg.databases < 2 || cfg.databases > 500 || cfg.prewarm < 0 || cfg.prewarm > 8 ||
		cfg.operations < 100 || cfg.repetitions < 3 || cfg.repetitions > 10 {
		return fmt.Errorf("experiment bounds are invalid")
	}
	if err := validateFleetScaleOutput(cfg.outputRoot, cfg.output); err != nil {
		return err
	}
	return nil
}

func verifyLabControlIdentity(ctx context.Context, db *sql.DB, expectedLabID string) error {
	var count int
	var environment, actualLabID string
	if err := db.QueryRowContext(ctx, labControlIdentityQuery).Scan(&count, &environment, &actualLabID); err != nil {
		return fmt.Errorf("required beads_perf_lab_control identity marker is unavailable")
	}
	if count != 1 || environment != "synthetic" || actualLabID != expectedLabID {
		return fmt.Errorf("beads_perf_lab_control identity marker mismatch")
	}
	return nil
}

// openFleetScaleOutputParent walks from the fixed evidence root using openat
// and O_NOFOLLOW. Validation therefore happens against opened directory file
// descriptors rather than a path that can be redirected through a symlink.
func openFleetScaleOutputParent(root, path string) (int, string, error) {
	cleanRoot := filepath.Clean(root)
	rootName := filepath.Base(cleanRoot)
	if !filepath.IsAbs(cleanRoot) || filepath.Dir(cleanRoot) != "/private/tmp" || !validFleetScaleRootName(rootName) {
		return -1, "", fmt.Errorf("output-root must be a direct /private/tmp/beads-fleet-scale-* directory")
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return -1, "", fmt.Errorf("output must be an absolute file below output-root")
	}
	rel, err := filepath.Rel(cleanRoot, clean)
	if err != nil || rel == "." || rel == "" || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return -1, "", fmt.Errorf("output must be a file below output-root")
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts[len(parts)-1]) > 160 {
		return -1, "", fmt.Errorf("output file name is too long")
	}
	parentFD, err := unix.Open(cleanRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, "", fmt.Errorf("open fixed output root: %w", err)
	}
	for _, component := range parts[:len(parts)-1] {
		nextFD, openErr := unix.Openat(parentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(parentFD)
		if openErr != nil {
			return -1, "", fmt.Errorf("output parent must be an existing non-symlink directory: %w", openErr)
		}
		parentFD = nextFD
	}
	return parentFD, parts[len(parts)-1], nil
}

func validFleetScaleRootName(name string) bool {
	const prefix = "beads-fleet-scale-"
	if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
		return false
	}
	for _, value := range name[len(prefix):] {
		if (value < 'a' || value > 'z') && (value < 'A' || value > 'Z') &&
			(value < '0' || value > '9') && value != '-' && value != '_' && value != '.' {
			return false
		}
	}
	return true
}

func validateFleetScaleOutput(root, path string) error {
	parentFD, name, err := openFleetScaleOutputParent(root, path)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("inspect output file: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("output must be a regular non-symlink file")
	}
	return nil
}

func writeFleetScaleOutput(root, path string, body []byte) error {
	parentFD, name, err := openFleetScaleOutputParent(root, path)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("create output temporary name: %w", err)
	}
	temporary := "." + name + ".tmp-" + hex.EncodeToString(random[:])
	fd, err := unix.Openat(parentFD, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open output temporary file: %w", err)
	}
	temporaryExists := true
	defer func() {
		if temporaryExists {
			_ = unix.Unlinkat(parentFD, temporary, 0)
		}
	}()
	file := os.NewFile(uintptr(fd), temporary)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("open output file descriptor")
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if err := unix.Fchmod(fd, 0o600); err != nil {
		return fmt.Errorf("set output permissions: %w", err)
	}
	if _, err := file.Write(body); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync output: %w", err)
	}
	err = file.Close()
	closed = true
	if err != nil {
		return fmt.Errorf("close output temporary file: %w", err)
	}
	if err := unix.Renameat(parentFD, temporary, parentFD, name); err != nil {
		return fmt.Errorf("publish output file: %w", err)
	}
	temporaryExists = false
	if err := unix.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}

func fleetDatabaseName(index int) string {
	return fmt.Sprintf("beads_perf_lab_scale_%03d", index)
}

func openDB(cfg config, database string) *sql.DB {
	mysqlConfig := mysql.Config{
		User: "root", Net: "tcp", Addr: net.JoinHostPort(cfg.host, fmt.Sprintf("%d", cfg.port)),
		DBName: database, ParseTime: true, AllowNativePasswords: true, Timeout: 5 * time.Second,
		ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute,
	}
	dsn := mysqlConfig.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatalf("open database %q: %v", database, err)
	}
	return db
}

func setupDatabases(ctx context.Context, cfg config, admin *sql.DB) ([]string, time.Duration, error) {
	started := time.Now()
	names := make([]string, cfg.databases)
	for i := range names {
		names[i] = fleetDatabaseName(i)
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+names[i]+"`"); err != nil {
			return nil, 0, fmt.Errorf("create %s: %w", names[i], err)
		}
		db := openDB(cfg, names[i])
		db.SetMaxOpenConns(2)
		statements := []string{
			"CREATE TABLE IF NOT EXISTS commit_probe (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
			"CREATE TABLE IF NOT EXISTS counter_probe (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
			"CREATE TABLE IF NOT EXISTS dependency_probe (issue_id VARCHAR(64) NOT NULL, depends_on_id VARCHAR(64) NOT NULL, dep_type VARCHAR(32) NOT NULL, PRIMARY KEY(issue_id, depends_on_id, dep_type))",
			"CREATE TABLE IF NOT EXISTS claim_probe (id BIGINT PRIMARY KEY, assignee VARCHAR(255) NOT NULL DEFAULT '', status VARCHAR(32) NOT NULL, updated_at DATETIME, started_at DATETIME)",
			"CREATE TABLE IF NOT EXISTS claim_event_probe (event_id VARCHAR(128) PRIMARY KEY, issue_id BIGINT NOT NULL, actor VARCHAR(255) NOT NULL, event_type VARCHAR(32) NOT NULL)",
		}
		for _, statement := range statements {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				_ = db.Close()
				return nil, 0, fmt.Errorf("%s: %w", names[i], err)
			}
		}
		if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('--allow-empty', '-Am', ?)", "fleet scale setup"); err != nil {
			_ = db.Close()
			return nil, 0, fmt.Errorf("%s setup commit: %w", names[i], err)
		}
		if err := db.Close(); err != nil {
			return nil, 0, err
		}
	}
	return names, time.Since(started), nil
}

func prewarmFleet(ctx context.Context, cfg config, names []string) ([]*sql.DB, map[string]any, error) {
	started := time.Now()
	pools := make([]*sql.DB, len(names))
	latencies := make([]time.Duration, len(names))
	var wg sync.WaitGroup
	errCh := make(chan error, len(names))
	sem := make(chan struct{}, 32)
	for i, name := range names {
		i, name := i, name
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			db := openDB(cfg, name)
			db.SetMaxOpenConns(8)
			db.SetMaxIdleConns(cfg.prewarm)
			pools[i] = db
			begin := time.Now()
			conns := make([]*sql.Conn, 0, cfg.prewarm)
			for j := 0; j < cfg.prewarm; j++ {
				conn, err := db.Conn(ctx)
				if err != nil {
					errCh <- fmt.Errorf("%s conn %d: %w", name, j, err)
					return
				}
				conns = append(conns, conn)
			}
			for _, conn := range conns {
				if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
					errCh <- fmt.Errorf("%s ping: %w", name, err)
					return
				}
			}
			for _, conn := range conns {
				_ = conn.Close()
			}
			latencies[i] = time.Since(begin)
		}()
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return pools, nil, err
	}
	var open, idle int
	for _, db := range pools {
		stats := db.Stats()
		open += stats.OpenConnections
		idle += stats.Idle
	}
	return pools, map[string]any{
		"wall_ms":                       time.Since(started).Seconds() * 1000,
		"pool_open_connections":         open,
		"pool_idle_connections":         idle,
		"maximum_potential_connections": len(names) * 8,
		"per_database_warm_latency":     summarize(latencies),
	}, nil
}

func runCommitLoad(ctx context.Context, hot *sql.DB, fleet []*sql.DB, workers, total int, label string) *runStats {
	stats := &runStats{errors: map[string]int{}}
	var next atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				index := int(next.Add(1)) - 1
				if index >= total {
					return
				}
				db := hot
				if len(fleet) > 0 {
					db = fleet[worker%len(fleet)]
				}
				began := time.Now()
				retries, err := insertAndCommit(ctx, db, int64(index)+time.Now().UnixNano(), label)
				stats.retries.Add(int64(retries))
				if err != nil {
					stats.failures.Add(1)
					stats.mu.Lock()
					stats.errors[classifyError(err)]++
					stats.mu.Unlock()
					continue
				}
				stats.successes.Add(1)
				stats.mu.Lock()
				stats.latencies = append(stats.latencies, time.Since(began))
				stats.mu.Unlock()
			}
		}()
	}
	began := time.Now()
	close(start)
	wg.Wait()
	stats.mu.Lock()
	stats.errors["wall_ms"] = int(time.Since(began).Milliseconds())
	stats.mu.Unlock()
	return stats
}

func runNoisyNeighbor(ctx context.Context, hot, cold *sql.DB, hotWorkers, hotOperations, coldOperations int) (*runStats, *runStats) {
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var hotStats, coldStats *runStats
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-release
		hotStats = runCommitLoad(ctx, hot, nil, hotWorkers, hotOperations, "noisy-hot")
	}()
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-release
		coldStats = runCommitLoad(ctx, cold, nil, 1, coldOperations, "noisy-cold")
	}()
	<-ready
	<-ready
	close(release)
	wg.Wait()
	return hotStats, coldStats
}

func insertAndCommit(ctx context.Context, db *sql.DB, id int64, label string) (int, error) {
	for attempt := 0; attempt < 6; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return attempt, err
		}
		if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err == nil {
			_, err = conn.ExecContext(ctx, "INSERT INTO commit_probe(id, value) VALUES (?, ?)", id, id)
		}
		if err == nil {
			_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", "fleet scale "+label)
		}
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = conn.Close()
		if err == nil {
			return attempt, nil
		}
		if !serializationError(err) {
			return attempt, err
		}
		select {
		case <-ctx.Done():
			return attempt, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return 5, fmt.Errorf("serialization retry exhausted")
}

func throughputExperiment(name string, workers, databases int, stats *runStats) experiment {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	wallMS := stats.errors["wall_ms"]
	delete(stats.errors, "wall_ms")
	success := stats.successes.Load()
	throughput := 0.0
	if wallMS > 0 {
		throughput = float64(success) / (float64(wallMS) / 1000)
	}
	decision := "measure_throughput_knee"
	if stats.failures.Load() != 0 {
		decision = "correctness_or_capacity_failure"
	}
	return experiment{
		Name:       name,
		Parameters: map[string]any{"workers": workers, "databases": databases},
		Metrics: map[string]any{
			"successes": success, "failures": stats.failures.Load(), "retries": stats.retries.Load(),
			"wall_ms": wallMS, "throughput_per_second": throughput,
			"latency": summarize(stats.latencies), "error_classes": stats.errors,
		},
		Decision: decision,
	}
}

func statsLatency(stats *runStats) latencySummary {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	return summarize(stats.latencies)
}

func runLostUpdate(ctx context.Context, db *sql.DB, writers, trial int) (experiment, error) {
	if err := resetTables(ctx, db,
		"DELETE FROM counter_probe",
		"INSERT INTO counter_probe(id, value) VALUES (1, 0)",
	); err != nil {
		return experiment{}, err
	}
	type outcome struct {
		err error
	}
	ready := make(chan struct{}, writers)
	release := make(chan struct{})
	out := make(chan outcome, writers)
	for i := 0; i < writers; i++ {
		go func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				out <- outcome{err: err}
				return
			}
			defer conn.Close()
			if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
				out <- outcome{err: err}
				return
			}
			var value int64
			if err = conn.QueryRowContext(ctx, "SELECT value FROM counter_probe WHERE id=1").Scan(&value); err != nil {
				out <- outcome{err: err}
				return
			}
			ready <- struct{}{}
			<-release
			if _, err = conn.ExecContext(ctx, "UPDATE counter_probe SET value=? WHERE id=1", value+1); err == nil {
				_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", "lost update probe")
			}
			if err != nil {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
			out <- outcome{err: err}
		}()
	}
	for i := 0; i < writers; i++ {
		<-ready
	}
	close(release)
	successes, failures := 0, map[string]int{}
	for i := 0; i < writers; i++ {
		result := <-out
		if result.err == nil {
			successes++
		} else {
			failures[classifyError(result.err)]++
		}
	}
	var final int64
	if err := db.QueryRowContext(ctx, "SELECT value FROM counter_probe WHERE id=1").Scan(&final); err != nil {
		return experiment{}, err
	}
	decision := "pass"
	if final != int64(successes) {
		decision = "lost_update_observed"
	}
	return experiment{
		Name: "dolt_same_value_lost_update", Trial: trial,
		Parameters: map[string]any{"writers": writers},
		Metrics:    map[string]any{"successful_commits": successes, "errors": failures, "expected_counter": successes, "actual_counter": final},
		Decision:   decision,
	}, nil
}

func runCycleWriteSkew(ctx context.Context, db *sql.DB, trial int) (experiment, error) {
	if err := resetTables(ctx, db, "DELETE FROM dependency_probe"); err != nil {
		return experiment{}, err
	}
	type edge struct{ from, to string }
	edges := []edge{{"a", "b"}, {"b", "a"}}
	ready := make(chan struct{}, len(edges))
	release := make(chan struct{})
	out := make(chan error, len(edges))
	for _, edge := range edges {
		edge := edge
		go func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				out <- err
				return
			}
			defer conn.Close()
			if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
				out <- err
				return
			}
			var count int
			if err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dependency_probe").Scan(&count); err != nil {
				out <- err
				return
			}
			if count != 0 {
				out <- fmt.Errorf("prevalidation saw nonempty graph")
				return
			}
			ready <- struct{}{}
			<-release
			if _, err = conn.ExecContext(ctx,
				"INSERT INTO dependency_probe(issue_id, depends_on_id, dep_type) VALUES (?, ?, 'blocks')",
				edge.from, edge.to); err == nil {
				_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", "cycle write skew probe")
			}
			if err != nil {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
			out <- err
		}()
	}
	for range edges {
		<-ready
	}
	close(release)
	successes, failureClasses := 0, map[string]int{}
	for range edges {
		if err := <-out; err == nil {
			successes++
		} else {
			failureClasses[classifyError(err)]++
		}
	}
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dependency_probe").Scan(&rows); err != nil {
		return experiment{}, err
	}
	cycle := rows == 2
	decision := "acyclic"
	if cycle {
		decision = "combined_cycle_committed"
	}
	return experiment{
		Name: "concurrent_dependency_cycle_write_skew", Trial: trial,
		Parameters: map[string]any{"transactions": 2, "each_prevalidated_empty_graph": true},
		Metrics:    map[string]any{"successful_commits": successes, "errors": failureClasses, "edge_rows": rows, "cycle": cycle},
		Decision:   decision,
	}, nil
}

func runClaimRace(ctx context.Context, db *sql.DB, writers int, sameActor bool, trial int) (experiment, error) {
	if err := resetTables(ctx, db,
		"DELETE FROM claim_event_probe",
		"DELETE FROM claim_probe",
		"INSERT INTO claim_probe(id, assignee, status) VALUES (1, '', 'open')",
	); err != nil {
		return experiment{}, err
	}
	ready := make(chan struct{}, writers)
	release := make(chan struct{})
	out := make(chan error, writers)
	fixedTime := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			actor := "same-actor"
			if !sameActor {
				actor = fmt.Sprintf("actor-%02d", i)
			}
			conn, err := db.Conn(ctx)
			if err != nil {
				out <- err
				return
			}
			defer conn.Close()
			if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
				out <- err
				return
			}
			var status, assignee string
			if err = conn.QueryRowContext(ctx, "SELECT status, assignee FROM claim_probe WHERE id=1").Scan(&status, &assignee); err != nil {
				out <- err
				return
			}
			ready <- struct{}{}
			<-release
			var result sql.Result
			result, err = conn.ExecContext(ctx, `
UPDATE claim_probe SET assignee=?, status='in_progress', updated_at=?, started_at=?
WHERE id=1 AND status='open' AND (assignee='' OR assignee IS NULL OR assignee=?)`,
				actor, fixedTime, fixedTime, actor)
			if err == nil {
				var changed int64
				changed, err = result.RowsAffected()
				if err == nil && changed == 1 {
					_, err = conn.ExecContext(ctx,
						"INSERT INTO claim_event_probe(event_id, issue_id, actor, event_type) VALUES (?, 1, ?, 'claimed')",
						fmt.Sprintf("trial-%d-%s-%02d", trial, actor, i), actor)
				}
			}
			if err == nil {
				_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", "claim race probe")
			}
			if err != nil {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
			out <- err
		}()
	}
	for i := 0; i < writers; i++ {
		<-ready
	}
	close(release)
	successes, failures := 0, map[string]int{}
	for i := 0; i < writers; i++ {
		if err := <-out; err == nil {
			successes++
		} else {
			failures[classifyError(err)]++
		}
	}
	var assignee, status string
	if err := db.QueryRowContext(ctx, "SELECT assignee, status FROM claim_probe WHERE id=1").Scan(&assignee, &status); err != nil {
		return experiment{}, err
	}
	var events int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM claim_event_probe").Scan(&events); err != nil {
		return experiment{}, err
	}
	decision := "one_semantic_claim"
	if events != 1 {
		decision = "duplicate_or_missing_claim_events"
	}
	name := "concurrent_distinct_actor_claim"
	if sameActor {
		name = "concurrent_idempotent_same_actor_claim"
	}
	return experiment{
		Name: name, Trial: trial,
		Parameters: map[string]any{"writers": writers, "same_actor": sameActor, "fixed_second_precision_timestamp": true},
		Metrics: map[string]any{
			"successful_commits": successes, "errors": failures, "final_assignee": assignee,
			"final_status": status, "claim_events": events,
		},
		Decision: decision,
	}, nil
}

func resetTables(ctx context.Context, db *sql.DB, statements ...string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('--allow-empty', '-Am', ?)", "reset probe"); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return err
	}
	return nil
}

func processCount(ctx context.Context, db *sql.DB) int {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.processlist").Scan(&count); err != nil {
		return -1
	}
	return count
}

func closePools(pools []*sql.DB) {
	for _, db := range pools {
		if db != nil {
			_ = db.Close()
		}
	}
}

func summarize(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	copyValues := append([]time.Duration(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	return latencySummary{
		Count: len(copyValues),
		P50MS: ms(percentile(copyValues, 0.50)),
		P95MS: ms(percentile(copyValues, 0.95)),
		P99MS: ms(percentile(copyValues, 0.99)),
		MaxMS: ms(copyValues[len(copyValues)-1]),
	}
}

func percentile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func ms(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000
}

func serializationError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1205 || mysqlErr.Number == 1213)
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return fmt.Sprintf("mysql_%d", mysqlErr.Number)
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "context deadline"):
		return "context_deadline"
	case strings.Contains(text, "retry exhausted"):
		return "retry_exhausted"
	default:
		return "other"
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
