package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxResponseBytes     = 8 << 20
	metricReservoirLimit = 10_000
	operationProofLimit  = 100_000
	submitRetryLimit     = 3
)

type runtimeTarget struct {
	cfg    targetConfig
	base   *url.URL
	token  []byte
	client *http.Client
}

type job struct {
	ordinal    int64
	identity   string
	kind       string
	command    string
	resource   string
	originTeam string
	target     *runtimeTarget
	queued     time.Time
	done       chan jobResult
}

type jobResult struct {
	ordinal         int64
	identity        string
	kind            string
	command         string
	originTeam      string
	destinationTeam string
	databaseID      string
	queue           time.Duration
	service         time.Duration
	httpStatus      int
	passed          bool
	unclassified    bool
	errorClass      string
	operationID     string
}

type thinkObservation struct {
	key      string
	duration time.Duration
}

type latencySummary struct {
	Count int     `json:"count"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

type sampleRecord struct {
	Identity        string  `json:"identity"`
	Ordinal         int64   `json:"ordinal"`
	Kind            string  `json:"kind"`
	Command         string  `json:"command"`
	OperationID     string  `json:"operation_id"`
	OriginTeam      string  `json:"origin_team"`
	DestinationTeam string  `json:"destination_team"`
	DatabaseID      string  `json:"database_id"`
	QueueMS         float64 `json:"client_dispatch_queue_ms"`
	ServiceMS       float64 `json:"request_service_ms"`
	EndToEndMS      float64 `json:"end_to_end_ms"`
	HTTPStatus      int     `json:"http_status,omitempty"`
	Passed          bool    `json:"passed"`
	Unclassified    bool    `json:"unclassified"`
	ErrorClass      string  `json:"error_class,omitempty"`
}

type correctnessCounters struct {
	HTTPFailures          int64 `json:"http_failures"`
	InvalidJSON           int64 `json:"invalid_json"`
	TerminalFailures      int64 `json:"terminal_failures"`
	NonterminalTimeouts   int64 `json:"nonterminal_timeouts"`
	ProjectMismatches     int64 `json:"project_mismatches"`
	DuplicateOperationIDs int64 `json:"duplicate_operation_ids"`
	MissingOperationIDs   int64 `json:"missing_operation_ids"`
	DroppedAtAdmission    int64 `json:"dropped_at_client_admission"`
	UnclassifiedOutcomes  int64 `json:"unclassified_outcomes"`
}

type metricGroup struct {
	Commands     int64          `json:"commands"`
	Passed       int64          `json:"passed"`
	Failed       int64          `json:"failed"`
	Unclassified int64          `json:"unclassified"`
	Queue        latencySummary `json:"client_dispatch_queue"`
	Service      latencySummary `json:"request_service"`
	EndToEnd     latencySummary `json:"end_to_end"`
}

type runMetrics struct {
	Generated                    int64                  `json:"generated"`
	Completed                    int64                  `json:"completed"`
	Dropped                      int64                  `json:"dropped"`
	Unclassified                 int64                  `json:"unclassified"`
	AccountingBalanced           bool                   `json:"accounting_balanced"`
	CompletionsAtStop            int64                  `json:"classified_completions_at_stop"`
	UnclassifiedAtStop           int64                  `json:"unclassified_at_stop"`
	BacklogAtStop                int64                  `json:"backlog_at_stop"`
	StopAccountingValid          bool                   `json:"stop_accounting_valid"`
	Passed                       int64                  `json:"passed"`
	Failed                       int64                  `json:"failed"`
	ReadCommands                 int64                  `json:"read_commands"`
	PointWrites                  int64                  `json:"point_writes"`
	Graphs                       int64                  `json:"graphs"`
	HotAECommands                int64                  `json:"hot_ae_commands"`
	HotAEPointCommands           int64                  `json:"hot_ae_point_commands"`
	HotAEPointWrites             int64                  `json:"hot_ae_point_writes"`
	LoadWindowSeconds            float64                `json:"load_window_seconds"`
	DrainDurationMS              float64                `json:"drain_duration_ms"`
	OfferedPerSecond             float64                `json:"offered_per_second"`
	LoadWindowCompletedPerSecond float64                `json:"load_window_classified_completed_per_second"`
	TrueCompletionPerSecond      float64                `json:"true_classified_completion_per_second"`
	TrueResultPerSecond          float64                `json:"true_result_completion_per_second"`
	Think                        latencySummary         `json:"sampled_think_time"`
	Queue                        latencySummary         `json:"client_dispatch_queue"`
	Service                      latencySummary         `json:"request_service"`
	EndToEnd                     latencySummary         `json:"end_to_end"`
	GraphOnly                    latencySummary         `json:"graph_end_to_end"`
	ByCommand                    map[string]metricGroup `json:"by_command"`
	ByOriginTeam                 map[string]metricGroup `json:"by_origin_team"`
	ByDestinationTeam            map[string]metricGroup `json:"by_destination_team"`
	ByDatabase                   map[string]metricGroup `json:"by_database"`
	HTTPStatuses                 map[string]int64       `json:"http_statuses"`
	ErrorClasses                 map[string]int64       `json:"error_classes"`
}

type runtimeResource struct {
	Goroutines   int    `json:"goroutines"`
	HeapAlloc    uint64 `json:"heap_alloc_bytes"`
	TotalAlloc   uint64 `json:"total_alloc_bytes"`
	Sys          uint64 `json:"sys_bytes"`
	NumGC        uint32 `json:"num_gc"`
	PauseTotalNS uint64 `json:"pause_total_ns"`
}

type poolResource struct {
	TargetsSampled     int   `json:"targets_sampled"`
	SampleErrors       int   `json:"sample_errors"`
	MaxOpenConnections int64 `json:"max_open_connections"`
	OpenConnections    int64 `json:"open_connections"`
	InUse              int64 `json:"in_use"`
	Idle               int64 `json:"idle"`
	WaitCount          int64 `json:"wait_count"`
	WaitDurationNS     int64 `json:"wait_duration_ns"`
	SQLStatements      int64 `json:"sql_statement_count"`
	Transactions       int64 `json:"transaction_count"`
	CommitAttempts     int64 `json:"commit_attempt_count"`
	CommitSuccesses    int64 `json:"commit_success_count"`
	Rollbacks          int64 `json:"rollback_count"`
}

type resourceSnapshot struct {
	At      time.Time       `json:"at"`
	Runner  runtimeResource `json:"runner"`
	Gateway poolResource    `json:"gateway_pool_totals"`
}

type sanitizedConfig struct {
	RunID               string   `json:"run_id"`
	LabID               string   `json:"lab_id"`
	DurationSeconds     int      `json:"duration_seconds"`
	Seed                int64    `json:"seed"`
	RegisteredDatabases int      `json:"registered_databases"`
	ActiveDatabases     int      `json:"active_databases"`
	ActiveDatabaseIDs   []string `json:"active_database_ids"`
	MaxInflight         int      `json:"max_inflight"`
	QueueCapacity       int      `json:"queue_capacity"`
	SimulatedRTTMS      int      `json:"simulated_rtt_ms"`
	OpenLoopRate        int      `json:"open_loop_commands_per_second,omitempty"`
}

type boundedEvidence struct {
	ResultLatency    boundedStoreEvidence `json:"result_latency_reservoir"`
	ThinkTime        boundedStoreEvidence `json:"think_time_reservoir"`
	OperationIDProof boundedStoreEvidence `json:"operation_id_duplicate_proof"`
	SampleRecords    boundedStoreEvidence `json:"sample_records"`
}

type report struct {
	SchemaVersion             int                     `json:"schema_version"`
	StartedAt                 time.Time               `json:"started_at"`
	LoadStartedAt             time.Time               `json:"load_started_at"`
	LoadStoppedAt             time.Time               `json:"load_stopped_at"`
	FinishedAt                time.Time               `json:"finished_at"`
	Mode                      string                  `json:"mode"`
	CapacityInterpretation    string                  `json:"capacity_interpretation"`
	CapacityPromotionEligible bool                    `json:"capacity_promotion_eligible"`
	Profile                   string                  `json:"profile"`
	Config                    sanitizedConfig         `json:"config"`
	Model                     workloadModel           `json:"workload_model"`
	Attestation               attestationInventory    `json:"attestation_inventory"`
	Metrics                   runMetrics              `json:"metrics"`
	Correctness               correctnessCounters     `json:"correctness_counters"`
	BoundedEvidence           boundedEvidence         `json:"bounded_evidence"`
	Gates                     map[string]gateEvidence `json:"gates"`
	ResourceBefore            resourceSnapshot        `json:"resource_before"`
	ResourceAfter             resourceSnapshot        `json:"resource_after"`
	Samples                   []sampleRecord          `json:"samples"`
	Passed                    bool                    `json:"passed"`
	Notes                     []string                `json:"notes"`
}

type exactCount struct {
	commands, passed, failed, unclassified int64
}

type accumulatorSnapshot struct {
	generated, completed, dropped, unclassified int64
}

type accumulator struct {
	mu                sync.Mutex
	generated         int64
	completed         int64
	dropped           int64
	unclassified      int64
	passed            int64
	failed            int64
	reads             int64
	pointWrites       int64
	graphs            int64
	hotCommands       int64
	hotPointCommands  int64
	hotPointWrites    int64
	statuses          map[string]int64
	errors            map[string]int64
	byCommand         map[string]*exactCount
	byOriginTeam      map[string]*exactCount
	byDestinationTeam map[string]*exactCount
	byDatabase        map[string]*exactCount
	results           *deterministicReservoir[jobResult]
	think             *deterministicReservoir[thinkObservation]
	operationIDs      *boundedIDProof
	correctness       correctnessCounters
	sampleLimit       int
	hotDatabaseID     string
}

func newAccumulator(sampleLimit, metricLimit, proofLimit int, hotID string) *accumulator {
	return &accumulator{
		statuses: map[string]int64{}, errors: map[string]int64{},
		byCommand: map[string]*exactCount{}, byOriginTeam: map[string]*exactCount{},
		byDestinationTeam: map[string]*exactCount{}, byDatabase: map[string]*exactCount{},
		results:      newDeterministicReservoir[jobResult](metricLimit),
		think:        newDeterministicReservoir[thinkObservation](metricLimit),
		operationIDs: newBoundedIDProof(proofLimit), sampleLimit: sampleLimit,
		hotDatabaseID: hotID,
	}
}

func (a *accumulator) generatedOne() {
	a.mu.Lock()
	a.generated++
	a.mu.Unlock()
}

func (a *accumulator) cancelGenerated() {
	a.mu.Lock()
	a.generated--
	a.mu.Unlock()
}

func (a *accumulator) droppedOne() {
	a.mu.Lock()
	a.dropped++
	a.correctness.DroppedAtAdmission++
	a.mu.Unlock()
}

func (a *accumulator) recordThink(key string, value time.Duration) {
	a.mu.Lock()
	a.think.add(key, thinkObservation{key: key, duration: value})
	a.mu.Unlock()
}

func (a *accumulator) record(result jobResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if result.unclassified {
		a.unclassified++
		a.correctness.UnclassifiedOutcomes++
	} else {
		a.completed++
		if result.passed {
			a.passed++
		} else {
			a.failed++
		}
	}
	switch result.kind {
	case "read":
		a.reads++
	case "point_write":
		a.pointWrites++
	case "graph":
		a.graphs++
	}
	if result.databaseID == a.hotDatabaseID {
		a.hotCommands++
		if result.kind == "read" || result.kind == "point_write" {
			a.hotPointCommands++
		}
		if result.kind == "point_write" {
			a.hotPointWrites++
		}
	}
	status := "transport_error"
	if result.httpStatus > 0 {
		status = strconv.Itoa(result.httpStatus)
	}
	a.statuses[status]++
	if result.errorClass != "" {
		a.errors[result.errorClass]++
		switch result.errorClass {
		case "invalid_json":
			a.correctness.InvalidJSON++
		case "terminal_failed":
			a.correctness.TerminalFailures++
		case "nonterminal_timeout":
			a.correctness.NonterminalTimeouts++
		case "project_mismatch":
			a.correctness.ProjectMismatches++
		default:
			if !result.unclassified {
				a.correctness.HTTPFailures++
			}
		}
	}
	if result.operationID != "" && a.operationIDs.observe(result.operationID) {
		a.correctness.DuplicateOperationIDs++
	}
	if (result.kind == "point_write" || result.kind == "graph") && result.passed && result.operationID == "" {
		a.correctness.MissingOperationIDs++
	}
	bumpExact(a.byCommand, result.command, result)
	bumpExact(a.byOriginTeam, result.originTeam, result)
	bumpExact(a.byDestinationTeam, result.destinationTeam, result)
	bumpExact(a.byDatabase, result.databaseID, result)
	a.results.add(result.identity, result)
}

func bumpExact(groups map[string]*exactCount, key string, result jobResult) {
	group := groups[key]
	if group == nil {
		group = &exactCount{}
		groups[key] = group
	}
	group.commands++
	if result.unclassified {
		group.unclassified++
	} else if result.passed {
		group.passed++
	} else {
		group.failed++
	}
}

func (a *accumulator) snapshot() accumulatorSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return accumulatorSnapshot{
		generated: a.generated, completed: a.completed,
		dropped: a.dropped, unclassified: a.unclassified,
	}
}

func (a *accumulator) finish(
	loadElapsed, totalElapsed, drain time.Duration, atStop accumulatorSnapshot,
) (runMetrics, correctnessCounters, boundedEvidence, []sampleRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resultEntries := a.results.entries()
	thinkEntries := a.think.entries()
	results := make([]jobResult, 0, len(resultEntries))
	for _, entry := range resultEntries {
		results = append(results, entry.value)
	}
	var queue, service, endToEnd, graph []time.Duration
	for _, value := range results {
		queue = append(queue, value.queue)
		service = append(service, value.service)
		endToEnd = append(endToEnd, value.queue+value.service)
		if value.kind == "graph" {
			graph = append(graph, value.queue+value.service)
		}
	}
	think := make([]time.Duration, 0, len(thinkEntries))
	for _, entry := range thinkEntries {
		think = append(think, entry.value.duration)
	}
	backlog := atStop.generated - atStop.dropped - atStop.completed - atStop.unclassified
	metrics := runMetrics{
		Generated: a.generated, Completed: a.completed, Dropped: a.dropped,
		Unclassified:       a.unclassified,
		AccountingBalanced: a.generated == a.completed+a.dropped+a.unclassified,
		CompletionsAtStop:  atStop.completed, UnclassifiedAtStop: atStop.unclassified,
		BacklogAtStop: backlog, StopAccountingValid: backlog >= 0,
		Passed: a.passed, Failed: a.failed,
		ReadCommands: a.reads, PointWrites: a.pointWrites, Graphs: a.graphs,
		HotAECommands: a.hotCommands, HotAEPointCommands: a.hotPointCommands,
		HotAEPointWrites: a.hotPointWrites, LoadWindowSeconds: loadElapsed.Seconds(),
		DrainDurationMS: ms(drain), Think: summarizeDurations(think),
		Queue: summarizeDurations(queue), Service: summarizeDurations(service),
		EndToEnd: summarizeDurations(endToEnd), GraphOnly: summarizeDurations(graph),
		ByCommand:         buildGroups(a.byCommand, results, func(r jobResult) string { return r.command }),
		ByOriginTeam:      buildGroups(a.byOriginTeam, results, func(r jobResult) string { return r.originTeam }),
		ByDestinationTeam: buildGroups(a.byDestinationTeam, results, func(r jobResult) string { return r.destinationTeam }),
		ByDatabase:        buildGroups(a.byDatabase, results, func(r jobResult) string { return r.databaseID }),
		HTTPStatuses:      cloneCounts(a.statuses), ErrorClasses: cloneCounts(a.errors),
	}
	if loadElapsed > 0 {
		metrics.OfferedPerSecond = float64(metrics.Generated) / loadElapsed.Seconds()
		metrics.LoadWindowCompletedPerSecond = float64(atStop.completed) / loadElapsed.Seconds()
	}
	if totalElapsed > 0 {
		metrics.TrueCompletionPerSecond = float64(metrics.Completed) / totalElapsed.Seconds()
		metrics.TrueResultPerSecond = float64(metrics.Completed+metrics.Unclassified) / totalElapsed.Seconds()
	}
	sort.Slice(results, func(i, j int) bool { return results[i].identity < results[j].identity })
	limit := minInt(a.sampleLimit, len(results))
	samples := make([]sampleRecord, 0, limit)
	for _, value := range results[:limit] {
		samples = append(samples, sampleRecord{
			Identity: value.identity, Ordinal: value.ordinal, Kind: value.kind,
			Command: value.command, OperationID: value.operationID, OriginTeam: value.originTeam,
			DestinationTeam: value.destinationTeam, DatabaseID: value.databaseID,
			QueueMS: ms(value.queue), ServiceMS: ms(value.service),
			EndToEndMS: ms(value.queue + value.service), HTTPStatus: value.httpStatus,
			Passed: value.passed, Unclassified: value.unclassified,
			ErrorClass: value.errorClass,
		})
	}
	resultEvidence := a.results.evidence()
	evidence := boundedEvidence{
		ResultLatency: resultEvidence, ThinkTime: a.think.evidence(),
		OperationIDProof: a.operationIDs.evidence(),
		SampleRecords: boundedStoreEvidence{
			Seen: resultEvidence.Seen, Retained: len(samples), Limit: a.sampleLimit,
			Truncated: resultEvidence.Truncated || int64(len(samples)) != resultEvidence.Seen,
		},
	}
	return metrics, a.correctness, evidence, samples
}

type groupBuilder struct {
	exact               exactCount
	queue, service, e2e []time.Duration
}

func buildGroups(
	exact map[string]*exactCount, results []jobResult, key func(jobResult) string,
) map[string]metricGroup {
	builders := make(map[string]*groupBuilder, len(exact))
	for name, counts := range exact {
		builders[name] = &groupBuilder{exact: *counts}
	}
	for _, result := range results {
		builder := builders[key(result)]
		if builder == nil {
			builder = &groupBuilder{}
			builders[key(result)] = builder
		}
		builder.queue = append(builder.queue, result.queue)
		builder.service = append(builder.service, result.service)
		builder.e2e = append(builder.e2e, result.queue+result.service)
	}
	groups := make(map[string]metricGroup, len(builders))
	for name, builder := range builders {
		groups[name] = metricGroup{
			Commands: builder.exact.commands, Passed: builder.exact.passed,
			Failed: builder.exact.failed, Unclassified: builder.exact.unclassified,
			Queue: summarizeDurations(builder.queue), Service: summarizeDurations(builder.service),
			EndToEnd: summarizeDurations(builder.e2e),
		}
	}
	return groups
}

func cloneCounts(input map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func run(ctx context.Context, cfg config) (report, error) {
	startedAt := time.Now().UTC()
	profile := frozenProfiles[cfg.Profile]
	allTargets, err := loadTargets(cfg.Targets, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
	if err != nil {
		return report{}, err
	}
	defer closeTargets(allTargets)
	attestation, err := attestTargets(ctx, allTargets)
	if err != nil {
		return report{}, err
	}
	activeCfg := selectActiveTargets(cfg.Targets, cfg.ActiveDatabaseCount, cfg.Seed)
	targets, err := selectRuntimeTargets(allTargets, activeCfg)
	if err != nil {
		return report{}, err
	}
	hot := findHot(targets)
	activeIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		activeIDs = append(activeIDs, target.cfg.DatabaseID)
	}
	schedule, durationSufficient := absoluteGraphSchedule(profile, time.Duration(cfg.DurationSeconds)*time.Second)
	result := report{
		SchemaVersion: 2, StartedAt: startedAt, Mode: cfg.Mode,
		CapacityInterpretation: capacityInterpretation(cfg.Mode),
		Profile:                cfg.Profile, Model: modelFor(profile), Attestation: attestation,
		Config: sanitizedConfig{
			RunID: cfg.RunID, LabID: cfg.Targets[0].LabID,
			DurationSeconds: cfg.DurationSeconds, Seed: cfg.Seed,
			RegisteredDatabases: len(cfg.Targets), ActiveDatabases: len(targets),
			ActiveDatabaseIDs: activeIDs, MaxInflight: cfg.MaxInflight,
			QueueCapacity: cfg.QueueCapacity, SimulatedRTTMS: cfg.SimulatedRTTMS,
			OpenLoopRate: cfg.OpenLoopRate,
		},
		Notes: []string{
			"Every registered target was token-HMAC-attested as synthetic before load; database_id is the actual beads_perf_lab_* name.",
			"All registered databases are cold-sampled, but workload traffic is sent only to the deterministic active subset.",
			"client_dispatch_queue excludes gateway-internal queue time; request_service includes HTTP, gateway queue/execution, and polling.",
			"Latency percentiles are deterministic bounded samples and become unknown promotion evidence when truncated.",
			"Closed-loop actors start at deterministic uniform phases across the frozen model cycle; the required load-fidelity gate rejects startup or finite-window under-driving outside a 10% band.",
			"backlog_at_stop is the undrained deadline snapshot and is never clamped; final throughput includes the separately reported drain duration.",
			"Independent cross-database effect, isolation, interrupted-operation reconciliation, and recovery inspection remain required external evidence.",
			"Open-loop overload is a non-capacity failure envelope and can never promote capacity.",
		},
	}
	result.ResourceBefore = collectResources(ctx, allTargets)
	acc := newAccumulator(cfg.SampleLimit, metricReservoirLimit, operationProofLimit, hot.cfg.DatabaseID)
	jobs := make(chan job, cfg.QueueCapacity)
	var workers sync.WaitGroup
	for worker := 0; worker < cfg.MaxInflight; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range jobs {
				started := time.Now()
				out := executeJob(ctx, cfg, item)
				out.queue = started.Sub(item.queued)
				out.service = time.Since(started)
				acc.record(out)
				if item.done != nil {
					item.done <- out
				}
			}
		}()
	}

	loadCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.DurationSeconds)*time.Second)
	defer cancel()
	type stopObservation struct {
		at       time.Time
		snapshot accumulatorSnapshot
	}
	stopped := make(chan stopObservation, 1)
	go func() {
		<-loadCtx.Done()
		stopped <- stopObservation{at: time.Now(), snapshot: acc.snapshot()}
	}()
	var ordinal atomic.Int64
	var producers sync.WaitGroup
	loadStarted := time.Now()
	result.LoadStartedAt = loadStarted.UTC()
	if cfg.Mode == modeClosedLoop {
		startClosedLoop(loadCtx, cfg, profile, targets, hot, jobs, acc, &ordinal, &producers)
	} else {
		startOpenLoop(loadCtx, cfg, profile, targets, hot, jobs, acc, &ordinal, &producers)
	}
	startGraphs(loadCtx, cfg, profile, targets, hot, schedule, loadStarted, jobs, acc, &ordinal, &producers)
	producers.Wait()
	stop := <-stopped
	loadStopped := stop.at
	result.LoadStoppedAt = loadStopped.UTC()
	close(jobs)
	workers.Wait()
	finished := time.Now()
	result.FinishedAt = finished.UTC()
	result.ResourceAfter = collectResources(ctx, allTargets)
	result.Metrics, result.Correctness, result.BoundedEvidence, result.Samples = acc.finish(
		loadStopped.Sub(loadStarted), finished.Sub(loadStarted), finished.Sub(loadStopped), stop.snapshot,
	)
	result.Gates = evaluateGates(cfg, durationSufficient, result)
	result.Passed = allRequiredGatesPass(result.Gates)
	result.CapacityPromotionEligible = cfg.Mode == modeClosedLoop && result.Passed
	return result, nil
}

func startClosedLoop(
	ctx context.Context, cfg config, profile workloadProfile, targets []*runtimeTarget, hot *runtimeTarget,
	jobs chan<- job, acc *accumulator, ordinal *atomic.Int64, producers *sync.WaitGroup,
) {
	byTeam, teams, nonHot := targetGroups(targets, hot)
	for actor := 0; actor < actorCount(profile); actor++ {
		actor := actor
		producers.Add(1)
		go func() {
			defer producers.Done()
			timingRNG := rand.New(rand.NewSource(deriveSeed(cfg.Seed, "actor-timing", actor))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
			jobRNG := rand.New(rand.NewSource(deriveSeed(cfg.Seed, "actor-jobs", actor)))      //nolint:gosec // G404: lab workload RNG is not security-sensitive
			originTeam := teams[actor%len(teams)]
			if !sleepContext(ctx, steadyStateInitialPhase(timingRNG, profile)) {
				return
			}
			for cycle := 0; ; cycle++ {
				commandCount := sampleCommandCount(timingRNG, profile.CommandsPerCycle)
				for command := 0; command < commandCount; command++ {
					identity := actorOperationIdentity(actor, cycle, command)
					item := makePointJob(profile, jobRNG, originTeam, byTeam[originTeam], nonHot, hot, identity)
					item.ordinal = ordinal.Add(1) - 1
					item.done = make(chan jobResult, 1)
					acc.generatedOne()
					select {
					case jobs <- item:
					case <-ctx.Done():
						acc.cancelGenerated()
						return
					}
					select {
					case <-item.done:
					case <-ctx.Done():
						return
					}
				}
				think := sampleThink(timingRNG, time.Duration(profile.ThinkTimeSeconds*float64(time.Second)))
				acc.recordThink(fmt.Sprintf("actor-%06d-cycle-%09d", actor, cycle), think)
				if !sleepContext(ctx, think) {
					return
				}
			}
		}()
	}
}

func startOpenLoop(
	ctx context.Context, cfg config, profile workloadProfile, targets []*runtimeTarget, hot *runtimeTarget,
	jobs chan<- job, acc *accumulator, ordinal *atomic.Int64, producers *sync.WaitGroup,
) {
	byTeam, teams, nonHot := targetGroups(targets, hot)
	producers.Add(1)
	go func() {
		defer producers.Done()
		rng := rand.New(rand.NewSource(deriveSeed(cfg.Seed, "open-loop", 0))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
		interval := time.Second / time.Duration(cfg.OpenLoopRate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for index := 0; ; index++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				originTeam := teams[index%len(teams)]
				item := makePointJob(profile, rng, originTeam, byTeam[originTeam], nonHot, hot, openOperationIdentity(index))
				item.ordinal = ordinal.Add(1) - 1
				acc.generatedOne()
				select {
				case jobs <- item:
				default:
					acc.droppedOne()
				}
			}
		}
	}()
}

func startGraphs(
	ctx context.Context, cfg config, profile workloadProfile, targets []*runtimeTarget, hot *runtimeTarget,
	schedule []time.Duration, loadStarted time.Time, jobs chan<- job, acc *accumulator,
	ordinal *atomic.Int64, producers *sync.WaitGroup,
) {
	if len(schedule) == 0 {
		return
	}
	_, teams, nonHot := targetGroups(targets, hot)
	producers.Add(1)
	go func() {
		defer producers.Done()
		rng := rand.New(rand.NewSource(deriveSeed(cfg.Seed, "graphs", 0))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
		for index, offset := range schedule {
			if !sleepUntilContext(ctx, loadStarted.Add(offset)) {
				return
			}
			originTeam := teams[index%len(teams)]
			target := chooseTarget(rng, profile.HotAEShare, nonHot, nonHot, hot)
			item := job{
				identity: graphOperationIdentity(index), kind: "graph", command: "graph",
				originTeam: originTeam, target: target, queued: time.Now(),
				ordinal: ordinal.Add(1) - 1,
			}
			acc.generatedOne()
			select {
			case jobs <- item:
			case <-ctx.Done():
				acc.cancelGenerated()
				return
			}
		}
	}()
}

func absoluteGraphSchedule(profile workloadProfile, duration time.Duration) ([]time.Duration, bool) {
	if duration <= 0 {
		return nil, false
	}
	if profile.CoordinatedGraphCount > 0 {
		window := time.Duration(profile.CoordinatedGraphWindowS) * time.Second
		interval := window / time.Duration(profile.CoordinatedGraphCount)
		offsets := make([]time.Duration, 0, profile.CoordinatedGraphCount)
		for index := 0; index < profile.CoordinatedGraphCount; index++ {
			offset := time.Duration(index) * interval
			if offset >= duration {
				break
			}
			offsets = append(offsets, offset)
		}
		return offsets, duration >= window
	}
	if profile.GraphRatePerHour <= 0 {
		return nil, false
	}
	interval := time.Duration(float64(time.Hour) / profile.GraphRatePerHour)
	offsets := make([]time.Duration, 0, int(math.Ceil(duration.Seconds()/interval.Seconds())))
	for offset := time.Duration(0); offset < duration; offset += interval {
		offsets = append(offsets, offset)
	}
	return offsets, duration >= interval
}

func targetGroups(targets []*runtimeTarget, hot *runtimeTarget) (map[string][]*runtimeTarget, []string, []*runtimeTarget) {
	byTeam := map[string][]*runtimeTarget{}
	var nonHot []*runtimeTarget
	for _, target := range targets {
		if target == hot {
			continue
		}
		byTeam[target.cfg.TeamID] = append(byTeam[target.cfg.TeamID], target)
		nonHot = append(nonHot, target)
	}
	teams := make([]string, 0, len(byTeam))
	for team := range byTeam {
		teams = append(teams, team)
	}
	sort.Strings(teams)
	if len(teams) == 0 {
		teams = []string{hot.cfg.TeamID}
	}
	return byTeam, teams, nonHot
}

func makePointJob(
	profile workloadProfile, rng *rand.Rand, originTeam string,
	teamTargets, nonHot []*runtimeTarget, hot *runtimeTarget, identity string,
) job {
	target := chooseTarget(rng, profile.HotAEShare, teamTargets, nonHot, hot)
	item := job{
		identity: identity, kind: "point_write", command: "create",
		originTeam: originTeam, target: target, queued: time.Now(),
	}
	if rng.Float64() >= profile.ReadFraction {
		return item
	}
	item.kind = "read"
	reads := []struct{ command, resource string }{
		{"ping", "ping"}, {"list", "issues?limit=50"}, {"ready", "ready?limit=50"},
	}
	if target.cfg.FixtureIssueID != "" {
		reads = append(reads, struct{ command, resource string }{
			"show", "issues/" + url.PathEscape(target.cfg.FixtureIssueID),
		})
	}
	read := reads[rng.Intn(len(reads))]
	item.command, item.resource = read.command, read.resource
	return item
}

func chooseTarget(
	rng *rand.Rand, hotShare float64, preferredNonHot, allNonHot []*runtimeTarget, hot *runtimeTarget,
) *runtimeTarget {
	if rng.Float64() < hotShare {
		return hot
	}
	pool := preferredNonHot
	if len(pool) == 0 {
		pool = allNonHot
	}
	if len(pool) == 0 {
		return hot
	}
	return pool[rng.Intn(len(pool))]
}

func actorOperationIdentity(actor, cycle, command int) string {
	return fmt.Sprintf("actor-%06d-cycle-%09d-command-%03d", actor, cycle, command)
}

func openOperationIdentity(index int) string {
	return fmt.Sprintf("open-%012d", index)
}

func graphOperationIdentity(index int) string {
	return fmt.Sprintf("graph-%06d", index)
}

func loadTargets(configs []targetConfig, timeout time.Duration) ([]*runtimeTarget, error) {
	targets := make([]*runtimeTarget, 0, len(configs))
	for _, cfg := range configs {
		base, _ := url.Parse(cfg.BaseURL)
		token, err := readSecret(cfg.TokenFile)
		if err != nil {
			closeTargets(targets)
			return nil, fmt.Errorf("%s token: %w", cfg.DatabaseID, err)
		}
		targets = append(targets, &runtimeTarget{
			cfg: cfg, base: base, token: token, client: newHTTPClient(timeout),
		})
	}
	return targets, nil
}

func selectRuntimeTargets(all []*runtimeTarget, selected []targetConfig) ([]*runtimeTarget, error) {
	byID := make(map[string]*runtimeTarget, len(all))
	for _, target := range all {
		byID[target.cfg.DatabaseID] = target
	}
	result := make([]*runtimeTarget, 0, len(selected))
	for _, cfg := range selected {
		target := byID[cfg.DatabaseID]
		if target == nil {
			return nil, fmt.Errorf("selected target %s is absent from loaded inventory", cfg.DatabaseID)
		}
		result = append(result, target)
	}
	return result, nil
}

func closeTargets(targets []*runtimeTarget) {
	for _, target := range targets {
		target.client.CloseIdleConnections()
		for i := range target.token {
			target.token[i] = 0
		}
	}
}

func findHot(targets []*runtimeTarget) *runtimeTarget {
	for _, target := range targets {
		if target.cfg.HotAE {
			return target
		}
	}
	panic("validated active target set has no hot ae database")
}

func newHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: nil, MaxIdleConns: 512, MaxIdleConnsPerHost: 16,
		IdleConnTimeout: 30 * time.Second, DisableCompression: true,
	}
	return &http.Client{Transport: transport, Timeout: timeout}
}

func readSecret(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("token file must be a private regular non-symlink file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	body = bytes.TrimSpace(body)
	if len(body) < 32 || len(body) > 4096 {
		return nil, fmt.Errorf("token length outside safe bounds")
	}
	return body, nil
}

func executeJob(parent context.Context, cfg config, item job) jobResult {
	ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	result := jobResult{
		ordinal: item.ordinal, identity: item.identity, kind: item.kind,
		command: item.command, originTeam: item.originTeam,
		destinationTeam: item.target.cfg.TeamID, databaseID: item.target.cfg.DatabaseID,
	}
	switch item.kind {
	case "read":
		status, body, err := doHTTP(ctx, cfg, item.target, http.MethodGet, item.resource, nil, "")
		result.httpStatus = status
		if err != nil {
			result.errorClass, result.unclassified = classifyRequestError(err)
			return result
		}
		if status != http.StatusOK {
			result.errorClass = "http_" + strconv.Itoa(status)
			return result
		}
		if !json.Valid(body) {
			result.errorClass = "invalid_json"
			return result
		}
		if item.command == "ping" {
			var ping struct {
				ProjectID string `json:"project_id"`
			}
			if json.Unmarshal(body, &ping) != nil {
				result.errorClass = "invalid_json"
				return result
			}
			if ping.ProjectID != item.target.cfg.ProjectID {
				result.errorClass = "project_mismatch"
				return result
			}
		}
		result.passed = true
		return result
	case "point_write", "graph":
		state, status, err := submitAndPoll(ctx, cfg, item)
		result.httpStatus = status
		result.operationID = state.ID
		if err != nil {
			result.errorClass, result.unclassified = classifyRequestError(err)
			return result
		}
		if state.ProjectID != item.target.cfg.ProjectID {
			result.errorClass = "project_mismatch"
			return result
		}
		if state.Status != "succeeded" {
			if state.Status == "failed" {
				result.errorClass = "terminal_failed"
			} else {
				result.errorClass = "nonterminal_timeout"
				result.unclassified = true
			}
			return result
		}
		result.passed = true
		return result
	default:
		result.errorClass = "runner_unknown_kind"
		return result
	}
}

type operationState struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
}

type httpStatusError struct {
	phase  string
	status int
}

func (e httpStatusError) Error() string {
	return fmt.Sprintf("%s unexpected HTTP status %d", e.phase, e.status)
}

type unclassifiedOutcomeError struct {
	reason string
	cause  error
}

func (e unclassifiedOutcomeError) Error() string {
	if e.cause == nil {
		return e.reason
	}
	return e.reason + ": " + e.cause.Error()
}

func (e unclassifiedOutcomeError) Unwrap() error { return e.cause }

func submitAndPoll(ctx context.Context, cfg config, item job) (operationState, int, error) {
	body, err := operationBody(cfg.RunID, item)
	if err != nil {
		return operationState{}, 0, err
	}
	key := stableIdempotencyKey(cfg.RunID, item.identity)
	var status int
	var response []byte
	for attempt := 0; attempt < submitRetryLimit; attempt++ {
		status, response, err = doHTTP(ctx, cfg, item.target, http.MethodPost, "operations?wait_ms=30000", body, key)
		if err == nil {
			break
		}
		if ctx.Err() != nil || attempt == submitRetryLimit-1 {
			return operationState{}, status, unclassifiedOutcomeError{reason: "ambiguous_submit", cause: err}
		}
		if !sleepContext(ctx, time.Duration(attempt+1)*25*time.Millisecond) {
			return operationState{}, status, unclassifiedOutcomeError{reason: "ambiguous_submit", cause: ctx.Err()}
		}
	}
	if status != http.StatusOK && status != http.StatusAccepted {
		statusErr := httpStatusError{phase: "submit", status: status}
		if status >= 500 || status == http.StatusRequestTimeout {
			return operationState{}, status, unclassifiedOutcomeError{reason: "ambiguous_submit_status", cause: statusErr}
		}
		return operationState{}, status, statusErr
	}
	state, err := decodeOperation(response)
	if err != nil {
		return operationState{}, status, unclassifiedOutcomeError{reason: "ambiguous_submit_response", cause: err}
	}
	for state.Status != "succeeded" && state.Status != "failed" {
		if !sleepContext(ctx, 25*time.Millisecond) {
			return state, status, unclassifiedOutcomeError{reason: "nonterminal_timeout", cause: ctx.Err()}
		}
		resource := "operations/" + url.PathEscape(state.ID)
		pollStatus, pollBody, pollErr := doHTTP(ctx, cfg, item.target, http.MethodGet, resource, nil, "")
		status = pollStatus
		if pollErr != nil {
			if ctx.Err() != nil {
				return state, status, unclassifiedOutcomeError{reason: "nonterminal_timeout", cause: pollErr}
			}
			continue
		}
		if pollStatus != http.StatusOK {
			return state, status, unclassifiedOutcomeError{
				reason: "ambiguous_poll_status", cause: httpStatusError{phase: "poll", status: pollStatus},
			}
		}
		state, err = decodeOperation(pollBody)
		if err != nil {
			return state, status, unclassifiedOutcomeError{reason: "ambiguous_poll_response", cause: err}
		}
	}
	return state, status, nil
}

func stableIdempotencyKey(runID, identity string) string {
	return "realistic-" + runID + "-" + identity
}

func operationBody(runID string, item job) ([]byte, error) {
	switch item.kind {
	case "point_write":
		return json.Marshal(map[string]any{
			"kind": "issue.create",
			"payload": map[string]any{
				"title":       fmt.Sprintf("[beads-perf-lab] realistic %s %s", runID, item.identity),
				"description": "isolated realistic fleet synthetic point mutation",
				"type":        "task", "priority": 2,
			},
		})
	case "graph":
		nodes := make([]map[string]any, 100)
		for i := range nodes {
			nodes[i] = map[string]any{
				"key":   fmt.Sprintf("n%03d", i),
				"title": fmt.Sprintf("[beads-perf-lab] realistic %s %s node %03d", runID, item.identity, i),
				"type":  "task", "priority": 2,
			}
		}
		edges := make([]map[string]any, 0, 200)
		for distance := 1; len(edges) < 200; distance++ {
			for from := distance; from < len(nodes) && len(edges) < 200; from++ {
				edges = append(edges, map[string]any{
					"from_key": fmt.Sprintf("n%03d", from),
					"to_key":   fmt.Sprintf("n%03d", from-distance), "type": "blocks",
				})
			}
		}
		return json.Marshal(map[string]any{
			"kind": "graph.apply", "payload": map[string]any{"nodes": nodes, "edges": edges},
		})
	default:
		return nil, fmt.Errorf("unsupported operation body kind %q", item.kind)
	}
}

func decodeOperation(body []byte) (operationState, error) {
	if !json.Valid(body) {
		return operationState{}, fmt.Errorf("invalid json")
	}
	var state operationState
	if err := json.Unmarshal(body, &state); err != nil || state.ID == "" || state.ProjectID == "" || state.Status == "" {
		return operationState{}, fmt.Errorf("invalid operation response")
	}
	return state, nil
}

func doHTTP(
	ctx context.Context, cfg config, target *runtimeTarget, method, resource string, body []byte, key string,
) (int, []byte, error) {
	half := time.Duration(cfg.SimulatedRTTMS) * time.Millisecond / 2
	if !sleepContext(ctx, half) {
		return 0, nil, ctx.Err()
	}
	endpoint := strings.TrimSuffix(target.base.String(), "/") + "/v1/projects/" +
		url.PathEscape(target.cfg.ProjectID) + "/" + resource
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+string(target.token))
	request.Header.Set("X-Beads-Project-ID", target.cfg.ProjectID)
	request.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := target.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if readErr != nil {
		return response.StatusCode, nil, readErr
	}
	if len(responseBody) > maxResponseBytes {
		return response.StatusCode, nil, fmt.Errorf("response too large")
	}
	if !sleepContext(ctx, half) {
		return response.StatusCode, responseBody, ctx.Err()
	}
	return response.StatusCode, responseBody, nil
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sleepUntilContext(ctx context.Context, deadline time.Time) bool {
	return sleepContext(ctx, time.Until(deadline))
}

func classifyRequestError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var unclassified unclassifiedOutcomeError
	if errors.As(err, &unclassified) {
		if strings.Contains(unclassified.reason, "timeout") {
			return "nonterminal_timeout", true
		}
		return "unclassified_" + unclassified.reason, true
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
		return "http_" + strconv.Itoa(statusErr.status), false
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "invalid json"), strings.Contains(text, "invalid operation response"):
		return "invalid_json", false
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "request_timeout", false
	default:
		return "request_error", false
	}
}

func collectResources(ctx context.Context, targets []*runtimeTarget) resourceSnapshot {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	snapshot := resourceSnapshot{
		At: time.Now().UTC(),
		Runner: runtimeResource{
			Goroutines: runtime.NumGoroutine(), HeapAlloc: memory.HeapAlloc,
			TotalAlloc: memory.TotalAlloc, Sys: memory.Sys, NumGC: memory.NumGC,
			PauseTotalNS: memory.PauseTotalNs,
		},
	}
	for _, target := range targets {
		status, body, err := resourceRequest(ctx, target)
		snapshot.Gateway.TargetsSampled++
		if err != nil || status != http.StatusOK {
			snapshot.Gateway.SampleErrors++
			continue
		}
		var raw map[string]json.RawMessage
		if json.Unmarshal(body, &raw) != nil {
			snapshot.Gateway.SampleErrors++
			continue
		}
		snapshot.Gateway.MaxOpenConnections += jsonInt(raw, "max_open_connections")
		snapshot.Gateway.OpenConnections += jsonInt(raw, "open_connections")
		snapshot.Gateway.InUse += jsonInt(raw, "in_use")
		snapshot.Gateway.Idle += jsonInt(raw, "idle")
		snapshot.Gateway.WaitCount += jsonInt(raw, "wait_count")
		snapshot.Gateway.WaitDurationNS += jsonInt(raw, "wait_duration_ns")
		snapshot.Gateway.SQLStatements += jsonInt(raw, "sql_statement_count")
		snapshot.Gateway.Transactions += jsonInt(raw, "transaction_count")
		snapshot.Gateway.CommitAttempts += jsonInt(raw, "commit_attempt_count")
		snapshot.Gateway.CommitSuccesses += jsonInt(raw, "commit_success_count")
		snapshot.Gateway.Rollbacks += jsonInt(raw, "rollback_count")
	}
	return snapshot
}

func resourceRequest(ctx context.Context, target *runtimeTarget) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	endpoint := strings.TrimSuffix(target.base.String(), "/") + "/v1/projects/" +
		url.PathEscape(target.cfg.ProjectID) + "/pool"
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+string(target.token))
	request.Header.Set("X-Beads-Project-ID", target.cfg.ProjectID)
	response, err := target.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if len(body) > maxResponseBytes {
		return response.StatusCode, nil, fmt.Errorf("response too large")
	}
	return response.StatusCode, body, err
}

func jsonInt(values map[string]json.RawMessage, key string) int64 {
	var value int64
	_ = json.Unmarshal(values[key], &value)
	return value
}

func summarizeDurations(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return latencySummary{
		Count: len(sorted), P50MS: ms(nearestRank(sorted, .50)),
		P95MS: ms(nearestRank(sorted, .95)), P99MS: ms(nearestRank(sorted, .99)),
		MaxMS: ms(sorted[len(sorted)-1]),
	}
}

func nearestRank(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(values))*quantile)) - 1
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

func correctnessTotal(value correctnessCounters) int64 {
	return value.HTTPFailures + value.InvalidJSON + value.TerminalFailures +
		value.NonterminalTimeouts + value.ProjectMismatches +
		value.DuplicateOperationIDs + value.MissingOperationIDs +
		value.DroppedAtAdmission + value.UnclassifiedOutcomes
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
