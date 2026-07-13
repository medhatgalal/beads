package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const syntheticAcknowledgement = "BEADS_FLEET_LAZY_SYNTHETIC_ONLY"

type lazyConfig struct {
	host       string
	port       int
	labID      string
	databaseN  int
	activeSets string
	budget     int
	serverCap  int
	workers    int
	queue      int
	operations int
	output     string
	execute    bool
}

type lazyReport struct {
	SchemaVersion int            `json:"schema_version"`
	StartedAt     time.Time      `json:"started_at"`
	FinishedAt    time.Time      `json:"finished_at"`
	DoltVersion   string         `json:"dolt_version"`
	Config        map[string]any `json:"config"`
	Cases         []lazyCase     `json:"cases"`
	Passed        bool           `json:"passed"`
	Notes         []string       `json:"notes"`
}

type lazyCase struct {
	ActiveDatabases     int            `json:"active_databases"`
	Operations          int            `json:"operations"`
	Workers             int            `json:"workers"`
	Successes           int64          `json:"successes"`
	Failures            int64          `json:"failures"`
	ErrorClasses        map[string]int `json:"error_classes"`
	Latency             latencySummary `json:"latency"`
	ThroughputPerSecond float64        `json:"throughput_per_second"`
	MaxAdmissionQueue   int            `json:"max_admission_queue"`
	ManagerBeforeSleep  managerMetrics `json:"manager_before_hibernate"`
	ManagerAfterSleep   managerMetrics `json:"manager_after_hibernate"`
	ServerProcessesPeak int            `json:"server_processes_peak"`
	ServerProcessesBase int            `json:"server_processes_baseline"`
	ServerProcessesIdle int            `json:"server_processes_after_hibernate"`
	ResourceSamples     int            `json:"resource_samples"`
	Passed              bool           `json:"passed"`
}

type latencySummary struct {
	Count int     `json:"count"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

type sqlManagedPool struct{ db *sql.DB }

func (p *sqlManagedPool) Close() error         { return p.db.Close() }
func (p *sqlManagedPool) OpenConnections() int { return p.db.Stats().OpenConnections }

const (
	fleetDatabasePrefix      = "beads_perf_lab_scale_"
	serverProcessDrainWindow = 2 * time.Second
	labControlIdentityQuery  = `
SELECT COUNT(*), COALESCE(MAX(environment), ''), COALESCE(MAX(lab_id), '')
FROM beads_perf_lab_control.lab_identity`
)

func fleetDatabaseName(index int) string {
	return fmt.Sprintf(fleetDatabasePrefix+"%03d", index)
}

func isFleetDatabase(database string) bool {
	if len(database) != len(fleetDatabasePrefix)+3 || !strings.HasPrefix(database, fleetDatabasePrefix) {
		return false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(database, fleetDatabasePrefix))
	return err == nil && index >= 0 && index < 500 && database == fleetDatabaseName(index)
}

func main() {
	var cfg lazyConfig
	flag.StringVar(&cfg.host, "host", "127.0.0.1", "numeric loopback Dolt host")
	flag.IntVar(&cfg.port, "port", 13360, "isolated Dolt SQL port; 3306 and 3307 are forbidden")
	flag.StringVar(&cfg.labID, "lab-id", "", "canonical UUID from the pre-existing synthetic server control marker")
	flag.IntVar(&cfg.databaseN, "databases", 100, "existing beads_perf_lab_scale_ database inventory")
	flag.StringVar(&cfg.activeSets, "active-sets", "5,30,100", "comma-separated active database counts")
	flag.IntVar(&cfg.budget, "connection-budget", 32, "hard resident-pool and connection budget")
	flag.IntVar(&cfg.serverCap, "server-connection-cap", 36, "hard Dolt max_connections ceiling, including control and close-drain headroom")
	flag.IntVar(&cfg.workers, "workers", 64, "bounded worker count")
	flag.IntVar(&cfg.queue, "queue-capacity", 256, "bounded admission queue")
	flag.IntVar(&cfg.operations, "operations", 640, "operations per active-set case")
	flag.StringVar(&cfg.output, "output", "/private/tmp/beads-fleet-scale-20260712/lazy-pools.json", "0600 JSON result")
	flag.BoolVar(&cfg.execute, "execute-synthetic", false, "required acknowledgement for synthetic writes")
	flag.Parse()

	if err := runMain(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fleet-lazy failed: %v\n", err)
		os.Exit(1)
	}
}

func runMain(cfg lazyConfig) error {
	activeSets, err := validateLazyConfig(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	admin, err := openSQLDatabase(cfg, "")
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("connect isolated Dolt: %w", err)
	}
	if err := verifyLabControlIdentity(ctx, admin, cfg.labID); err != nil {
		return fmt.Errorf("verify synthetic server identity before mutation: %w", err)
	}
	var serverMaxConnections int
	if err := admin.QueryRowContext(ctx, "SELECT @@max_connections").Scan(&serverMaxConnections); err != nil {
		return fmt.Errorf("read Dolt max_connections: %w", err)
	}
	if serverMaxConnections > cfg.serverCap {
		return fmt.Errorf("Dolt max_connections=%d exceeds approved hard cap %d", serverMaxConnections, cfg.serverCap)
	}

	report := lazyReport{
		SchemaVersion: 1, StartedAt: time.Now().UTC(), Passed: true,
		Config: map[string]any{
			"host": cfg.host, "port": cfg.port, "lab_id": cfg.labID, "registered_databases": cfg.databaseN,
			"active_sets": activeSets, "connection_budget": cfg.budget,
			"server_connection_cap": cfg.serverCap, "observed_server_max_connections": serverMaxConnections,
			"workers": cfg.workers, "queue_capacity": cfg.queue,
			"operations_per_case": cfg.operations,
		},
		Notes: []string{
			"Raw synthetic Dolt commit probes validate the lazy connection mechanism, not Beads API semantics.",
			"Each resident database pool is capped at one connection; a bounded worker pool is the per-cell admission ceiling.",
			"The fixed-pool comparison is recorded separately in results-100db-noisy.json.",
		},
	}
	_ = admin.QueryRowContext(ctx, "SELECT DOLT_VERSION()").Scan(&report.DoltVersion)

	for _, active := range activeSets {
		result, err := runLazyCase(ctx, cfg, admin, active)
		if err != nil {
			return fmt.Errorf("active set %d: %w", active, err)
		}
		report.Cases = append(report.Cases, result)
		if !result.Passed {
			report.Passed = false
		}
	}
	report.FinishedAt = time.Now().UTC()
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.output, append(body, '\n'), 0o600); err != nil {
		return err
	}
	fmt.Printf("wrote %s cases=%d passed=%t\n", cfg.output, len(report.Cases), report.Passed)
	if !report.Passed {
		return fmt.Errorf("one or more lazy-pool gates failed")
	}
	return nil
}

func validateLazyConfig(cfg lazyConfig) ([]int, error) {
	ip := net.ParseIP(cfg.host)
	if ip == nil || !ip.IsLoopback() || cfg.port < 1 || cfg.port > 65535 || cfg.port == 3306 || cfg.port == 3307 {
		return nil, fmt.Errorf("host must be numeric loopback and port must be isolated, not 3306 or 3307")
	}
	if !cfg.execute {
		return nil, fmt.Errorf("--execute-synthetic acknowledgement is required (%s)", syntheticAcknowledgement)
	}
	parsedLabID, err := uuid.Parse(cfg.labID)
	if err != nil || parsedLabID == uuid.Nil || parsedLabID.String() != cfg.labID {
		return nil, fmt.Errorf("--lab-id must be a non-nil canonical UUID")
	}
	if cfg.databaseN < 2 || cfg.databaseN > 500 || cfg.budget < 2 || cfg.budget > 252 ||
		cfg.serverCap < 6 || cfg.serverCap > 256 || cfg.budget+4 > cfg.serverCap ||
		cfg.workers < 1 || cfg.workers > 256 || cfg.queue < cfg.workers || cfg.queue > 4096 ||
		cfg.operations < 100 || cfg.operations > 100000 {
		return nil, fmt.Errorf("experiment bounds are invalid")
	}
	abs, err := filepath.Abs(cfg.output)
	if err != nil || !strings.HasPrefix(abs, "/private/tmp/beads-fleet-scale-") {
		return nil, fmt.Errorf("output must be under a disposable beads-fleet-scale directory")
	}
	parts := strings.Split(cfg.activeSets, ",")
	sets := make([]int, 0, len(parts))
	for _, raw := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || value < 1 || value > cfg.databaseN {
			return nil, fmt.Errorf("invalid active database count %q", raw)
		}
		sets = append(sets, value)
	}
	if len(sets) == 0 {
		return nil, fmt.Errorf("at least one active set is required")
	}
	return sets, nil
}

func openSQLDatabase(cfg lazyConfig, database string) (*sql.DB, error) {
	mysqlConfig := mysql.Config{
		User: "root", Net: "tcp", Addr: fmt.Sprintf("%s:%d", cfg.host, cfg.port),
		DBName: database, ParseTime: true, AllowNativePasswords: true,
		Timeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute,
	}
	db, err := sql.Open("mysql", mysqlConfig.FormatDSN())
	if err != nil {
		return nil, err
	}
	return db, nil
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

func runLazyCase(ctx context.Context, cfg lazyConfig, admin *sql.DB, active int) (lazyCase, error) {
	baselineProcesses, err := serverProcessCount(ctx, admin)
	if err != nil {
		return lazyCase{}, fmt.Errorf("capture server process baseline: %w", err)
	}
	factory := func(database string) (managedPool, error) {
		if !isFleetDatabase(database) {
			return nil, fmt.Errorf("database outside synthetic fleet namespace")
		}
		db, err := openSQLDatabase(cfg, database)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxIdleTime(30 * time.Second)
		return &sqlManagedPool{db: db}, nil
	}
	manager, err := newLazyPoolManager(cfg.budget, factory)
	if err != nil {
		return lazyCase{}, err
	}
	defer manager.Close()
	stopSamples := make(chan struct{})
	sampleDone := sampleFleetResources(ctx, admin, manager, stopSamples)

	type outcome struct {
		latency time.Duration
		err     error
	}
	jobs := make(chan int, cfg.queue)
	results := make(chan outcome, cfg.operations)
	workerN := min(cfg.workers, cfg.operations)
	var workers sync.WaitGroup
	for worker := 0; worker < workerN; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				began := time.Now()
				database := fleetDatabaseName(index % active)
				lease, err := manager.Acquire(ctx, database)
				if err == nil {
					pool, ok := lease.pool.(*sqlManagedPool)
					if !ok {
						err = fmt.Errorf("unexpected pool implementation")
					} else {
						err = syntheticCommit(ctx, pool.db, uniqueID.Add(1), active)
					}
					lease.Release()
				}
				results <- outcome{latency: time.Since(began), err: err}
			}
		}()
	}
	maxQueue := 0
	started := time.Now()
	for index := 0; index < cfg.operations; index++ {
		jobs <- index
		if len(jobs) > maxQueue {
			maxQueue = len(jobs)
		}
	}
	close(jobs)
	workers.Wait()
	close(stopSamples)
	samples := <-sampleDone
	if samples.err != nil {
		return lazyCase{}, samples.err
	}
	close(results)
	elapsed := time.Since(started)

	var successes, failures int64
	var latencies []time.Duration
	errorClasses := map[string]int{}
	for result := range results {
		if result.err != nil {
			failures++
			errorClasses[classifyLazyError(result.err)]++
			continue
		}
		successes++
		latencies = append(latencies, result.latency)
	}
	before := manager.Metrics()
	if err := manager.HibernateIdle(); err != nil {
		return lazyCase{}, err
	}
	after := manager.Metrics()
	idleProcesses, err := waitForServerProcessBaseline(ctx, admin, baselineProcesses, serverProcessDrainWindow)
	if err != nil {
		return lazyCase{}, err
	}
	passed := failures == 0 && successes == int64(cfg.operations) && before.MaxResidentPools <= cfg.budget &&
		before.MaxObservedOpenConnections <= cfg.budget && after.ResidentPools == 0 && after.OpenConnections == 0 &&
		maxQueue <= cfg.queue && samples.count > 0 && samples.peakProcesses <= cfg.serverCap && idleProcesses == baselineProcesses
	return lazyCase{
		ActiveDatabases: active, Operations: cfg.operations, Workers: workerN,
		Successes: successes, Failures: failures, ErrorClasses: errorClasses,
		Latency: summarizeLatencies(latencies), ThroughputPerSecond: float64(successes) / elapsed.Seconds(),
		MaxAdmissionQueue: maxQueue, ManagerBeforeSleep: before, ManagerAfterSleep: after,
		ServerProcessesPeak: samples.peakProcesses, ServerProcessesBase: baselineProcesses, ServerProcessesIdle: idleProcesses,
		ResourceSamples: samples.count, Passed: passed,
	}, nil
}

type resourceSampleSummary struct {
	peakProcesses int
	count         int
	err           error
}

// sampleFleetResources observes both manager and server state throughout the
// workload. The earlier one-shot process-list read happened after workers had
// stopped and could not substantiate a peak claim.
func sampleFleetResources(
	ctx context.Context,
	admin *sql.DB,
	manager *lazyPoolManager,
	stop <-chan struct{},
) <-chan resourceSampleSummary {
	done := make(chan resourceSampleSummary, 1)
	go func() {
		const interval = 5 * time.Millisecond
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		summary := resourceSampleSummary{}
		sample := func() bool {
			_ = manager.Metrics()
			processes, err := serverProcessCount(ctx, admin)
			if err != nil {
				summary.err = fmt.Errorf("sample server process count: %w", err)
				return false
			}
			summary.count++
			if processes > summary.peakProcesses {
				summary.peakProcesses = processes
			}
			return true
		}
		if !sample() {
			done <- summary
			return
		}
		for {
			select {
			case <-ctx.Done():
				done <- summary
				return
			case <-stop:
				_ = sample()
				done <- summary
				return
			case <-ticker.C:
				if !sample() {
					done <- summary
					return
				}
			}
		}
	}()
	return done
}

func syntheticCommit(ctx context.Context, db *sql.DB, id int64, active int) error {
	for attempt := 0; attempt < 6; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err == nil {
			_, err = conn.ExecContext(ctx, "INSERT INTO commit_probe(id, value) VALUES (?, ?)", id, id)
		}
		if err == nil {
			_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", fmt.Sprintf("fleet lazy active-%d", active))
		}
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = conn.Close()
		if err == nil {
			return nil
		}
		if !isSerializationError(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return fmt.Errorf("serialization retry exhausted")
}

func isSerializationError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1205 || mysqlErr.Number == 1213)
}

func classifyLazyError(err error) string {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return fmt.Sprintf("mysql_%d", mysqlErr.Number)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	return "other"
}

func serverProcessCount(ctx context.Context, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, "SHOW PROCESSLIST")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func waitForServerProcessBaseline(ctx context.Context, db *sql.DB, baseline int, window time.Duration) (int, error) {
	drainCtx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	last := -1
	for {
		count, err := serverProcessCount(drainCtx, db)
		if err != nil {
			return last, fmt.Errorf("read server processes while draining: %w", err)
		}
		last = count
		if count == baseline {
			return count, nil
		}
		select {
		case <-drainCtx.Done():
			return last, fmt.Errorf("server processes did not return to baseline %d (last %d): %w", baseline, last, drainCtx.Err())
		case <-ticker.C:
		}
	}
}

func summarizeLatencies(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return latencySummary{
		Count: len(values), P50MS: durationMS(percentile(values, .50)),
		P95MS: durationMS(percentile(values, .95)), P99MS: durationMS(percentile(values, .99)),
		MaxMS: durationMS(values[len(values)-1]),
	}
}

func percentile(values []time.Duration, q float64) time.Duration {
	index := int(math.Ceil(q*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func durationMS(value time.Duration) float64 {
	return math.Round(float64(value.Microseconds())/10) / 100
}

var uniqueID atomic.Int64

func init() { uniqueID.Store(time.Now().UnixNano()) }
