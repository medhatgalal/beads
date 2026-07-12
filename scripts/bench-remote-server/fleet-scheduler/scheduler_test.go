package fleetscheduler

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestNoisyRepositoryCannotStarveTeamPeers(t *testing.T) {
	repositories := makeRepositories(10)
	config := testConfig("team-00", repositories)
	config.MaxJobsPerRepo = 600
	config.InteractiveReserveJobsPerRepo = 10
	scheduler := mustScheduler(t, config)
	for index := 0; index < 500; index++ {
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("noisy-%d", index), repositories[0], ClassBulk, 16, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	for repository := 1; repository < len(repositories); repository++ {
		for index := 0; index < 10; index++ {
			if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("quiet-%d-%d", repository, index), repositories[repository], ClassInteractive, 1, 256)); err != nil {
				t.Fatal(err)
			}
		}
	}

	firstService := map[string]int{}
	dequeued := 0
	for {
		job, ok := scheduler.Next()
		if !ok {
			break
		}
		dequeued++
		if _, exists := firstService[job.RepositoryID]; !exists {
			firstService[job.RepositoryID] = dequeued
		}
		if err := scheduler.Complete(job); err != nil {
			t.Fatal(err)
		}
	}
	if dequeued != 590 {
		t.Fatalf("dequeued=%d", dequeued)
	}
	for _, repository := range repositories[1:] {
		if position := firstService[repository]; position == 0 || position > 20 {
			t.Fatalf("repository %s first served at %d", repository, position)
		}
	}
	stats := scheduler.Stats()
	if stats.QueuedJobs != 0 || stats.QueuedBytes != 0 || stats.InFlight != 0 || stats.Completed != 590 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestAEClassWeightsPreserveInteractiveAndBulkProgress(t *testing.T) {
	config := testConfig("team-ae", []string{"ae"})
	config.MaxJobsPerRepo = 1000
	config.MaxBytesPerRepo = config.MaxBytes
	config.InteractiveReserveJobsPerRepo = 100
	config.InteractiveReserveBytesPerRepo = 1 << 20
	scheduler := mustScheduler(t, config)
	for index := 0; index < 500; index++ {
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("i-%d", index), "ae", ClassInteractive, 1, 64)); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 50; index++ {
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("b-%d", index), "ae", ClassBulk, 16, 1024)); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("r-%d", index), "ae", ClassReconcile, 4, 256)); err != nil {
			t.Fatal(err)
		}
	}
	firstBulk, firstReconcile := 0, 0
	interactiveInFirstHundred := 0
	for position := 1; position <= 600; position++ {
		job, ok := scheduler.Next()
		if !ok {
			t.Fatalf("queue ended at %d", position)
		}
		switch job.Class {
		case ClassBulk:
			if firstBulk == 0 {
				firstBulk = position
			}
		case ClassReconcile:
			if firstReconcile == 0 {
				firstReconcile = position
			}
		case ClassInteractive:
			if position <= 100 {
				interactiveInFirstHundred++
			}
		}
		if err := scheduler.Complete(job); err != nil {
			t.Fatal(err)
		}
	}
	if firstBulk == 0 || firstBulk > 20 || firstReconcile == 0 || firstReconcile > 20 || interactiveInFirstHundred < 50 {
		t.Fatalf("firstBulk=%d firstReconcile=%d interactiveFirst100=%d", firstBulk, firstReconcile, interactiveInFirstHundred)
	}
}

func TestQueueUsesActualSerializedBytesAndOverflowSafeLimits(t *testing.T) {
	config := testConfig("team-00", []string{"repo-000"})
	config.MaxJobs = 2
	config.MaxJobsPerRepo = 2
	config.MaxBytes = 512
	config.MaxBytesPerRepo = 512
	config.MaxJobBytes = 400
	config.InteractiveReserveJobs = 0
	config.InteractiveReserveBytes = 0
	config.InteractiveReserveJobsPerRepo = 0
	config.InteractiveReserveBytesPerRepo = 0
	scheduler := mustScheduler(t, config)
	if err := scheduler.Enqueue(testJob(config, "wrong", "repo-000", ClassInteractive, 1, 1, withTeam("team-01"))); !errors.Is(err, ErrWrongTeam) {
		t.Fatalf("wrong team error=%v", err)
	}
	foreign := testJob(config, "foreign", "repo-000", ClassInteractive, 1, 1)
	foreign.RepositoryID = "repo-999"
	if err := scheduler.Enqueue(foreign); !errors.Is(err, ErrUnknownRepository) {
		t.Fatalf("foreign repo error=%v", err)
	}
	if err := scheduler.Enqueue(testJob(config, "a", "repo-000", ClassWrite, 2, 120)); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(testJob(config, "a", "repo-000", ClassWrite, 2, 1)); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate job error=%v", err)
	}
	if err := scheduler.Enqueue(testJob(config, "b", "repo-000", ClassWrite, 2, 300)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("byte capacity error=%v", err)
	}
	tooLong := testJob(config, strings.Repeat("x", maximumJobIDBytes+1), "repo-000", ClassWrite, 1, 1)
	if err := scheduler.Enqueue(tooLong); err == nil {
		t.Fatal("unbounded job ID passed")
	}
	invalid := config
	invalid.MaxBytes = math.MaxInt64
	if _, err := New(invalid); err == nil {
		t.Fatal("overflow-prone configuration passed")
	}
	stats := scheduler.Stats()
	if stats.RejectedIdentity != 3 || stats.RejectedCapacity != 1 || stats.RejectedDuplicate != 1 || stats.MaxQueuedBytes <= 120 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestSchedulerCopiesPayloadAndReleasesDrainedBackingStore(t *testing.T) {
	config := testConfig("team-00", []string{"repo-000"})
	scheduler := mustScheduler(t, config)
	job := testJob(config, "copy", "repo-000", ClassWrite, 1, 4)
	job.Payload[0] = 7
	if err := scheduler.Enqueue(job); err != nil {
		t.Fatal(err)
	}
	job.Payload[0] = 99
	selected, ok := scheduler.Next()
	if !ok || selected.Payload[0] != 7 {
		t.Fatalf("queued payload aliased caller memory: %+v ok=%t", selected, ok)
	}
	if queue := scheduler.repos["repo-000"].jobs[ClassWrite]; queue != nil {
		t.Fatalf("drained queue retained backing storage: len=%d cap=%d", len(queue), cap(queue))
	}
	if err := scheduler.Complete(selected); err != nil {
		t.Fatal(err)
	}
}

func TestInteractiveReservationsProtectQueueAndExecutionSlots(t *testing.T) {
	config := testConfig("team-ae", []string{"ae"})
	config.MaxJobs = 4
	config.MaxJobsPerRepo = 4
	config.InteractiveReserveJobs = 1
	config.InteractiveReserveJobsPerRepo = 1
	config.MaxInFlight = 2
	config.MaxInFlightPerRepo = 2
	config.InteractiveInFlightReserve = 1
	scheduler := mustScheduler(t, config)
	for index := 0; index < 3; index++ {
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("bulk-%d", index), "ae", ClassBulk, 1, 64)); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue(testJob(config, "bulk-overflow", "ae", ClassBulk, 1, 64)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("bulk consumed interactive reservation: %v", err)
	}
	if err := scheduler.Enqueue(testJob(config, "interactive", "ae", ClassInteractive, 1, 64)); err != nil {
		t.Fatal(err)
	}
	first, ok := scheduler.Next()
	if !ok || first.Class != ClassInteractive {
		t.Fatalf("reserved interactive job was not selected first: %+v ok=%t", first, ok)
	}
	second, ok := scheduler.Next()
	if !ok || second.Class != ClassBulk {
		t.Fatalf("bulk did not use its execution slot: %+v ok=%t", second, ok)
	}
	if _, ok := scheduler.Next(); ok {
		t.Fatal("non-interactive work consumed the reserved in-flight slot")
	}
	if err := scheduler.Complete(first); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Complete(second); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogUpdateDropsQueuedAndFencesInFlightWork(t *testing.T) {
	config := testConfig("team-00", []string{"repo-000"})
	scheduler := mustScheduler(t, config)
	if err := scheduler.Enqueue(testJob(config, "running", "repo-000", ClassWrite, 1, 64)); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(testJob(config, "queued", "repo-000", ClassWrite, 1, 64)); err != nil {
		t.Fatal(err)
	}
	running, ok := scheduler.Next()
	if !ok {
		t.Fatal("expected in-flight job")
	}
	next := map[string]Fence{"repo-000": {
		ProjectID: config.Fences["repo-000"].ProjectID, DatabaseEpoch: stableUUID("restored-repo-000"),
		CatalogVersion: 2, KeyEpoch: 2,
	}}
	if err := scheduler.UpdateCatalog(next); err != nil {
		t.Fatal(err)
	}
	if _, ok := scheduler.Next(); ok {
		t.Fatal("stale queued work survived catalog replacement")
	}
	if queue := scheduler.repos["repo-000"].jobs[ClassWrite]; queue != nil {
		t.Fatalf("stale purge retained payload backing storage: len=%d cap=%d", len(queue), cap(queue))
	}
	if err := scheduler.Complete(running); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale in-flight completion error=%v", err)
	}
	stats := scheduler.Stats()
	if stats.StaleQueuedDropped != 1 || stats.StaleInFlight != 0 || stats.QueuedJobs != 0 || stats.InFlight != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	newConfig := config
	newConfig.Fences = next
	if err := scheduler.Enqueue(testJob(newConfig, "new", "repo-000", ClassWrite, 1, 64)); err != nil {
		t.Fatalf("new epoch job: %v", err)
	}
	downgrade := map[string]Fence{"repo-000": {
		ProjectID: next["repo-000"].ProjectID, DatabaseEpoch: stableUUID("downgrade"),
		CatalogVersion: 3, KeyEpoch: 1,
	}}
	if err := scheduler.UpdateCatalog(downgrade); err == nil {
		t.Fatal("key epoch downgrade passed")
	}
}

func TestDeficitIsBoundedAndResetWhenQueueDrains(t *testing.T) {
	config := testConfig("team-ae", []string{"ae"})
	scheduler := mustScheduler(t, config)
	for index := 0; index < 500; index++ {
		if err := scheduler.Enqueue(testJob(config, fmt.Sprintf("cheap-%d", index), "ae", ClassInteractive, 1, 1)); err != nil {
			t.Fatal(err)
		}
	}
	for {
		job, ok := scheduler.Next()
		if !ok {
			break
		}
		if err := scheduler.Complete(job); err != nil {
			t.Fatal(err)
		}
	}
	if scheduler.repos["ae"].deficit != 0 || scheduler.Stats().MaxObservedRepositoryDeficit > maximumCostUnits {
		t.Fatalf("deficit escaped bound: repo=%d stats=%+v", scheduler.repos["ae"].deficit, scheduler.Stats())
	}
}

func TestThirtyTeamCellsRemainBoundedUnderConcurrentChurn(t *testing.T) {
	const teamCount = 30
	schedulers := make([]*Scheduler, teamCount)
	configs := make([]Config, teamCount)
	for team := range schedulers {
		repositories := make([]string, 10)
		for index := range repositories {
			repositories[index] = fmt.Sprintf("repo-%03d", team*10+index)
		}
		config := testConfig(fmt.Sprintf("team-%02d", team), repositories)
		config.MaxJobs = 128
		config.MaxJobsPerRepo = 32
		configs[team] = config
		schedulers[team] = mustScheduler(t, config)
	}
	var wg sync.WaitGroup
	for team, scheduler := range schedulers {
		team, scheduler := team, scheduler
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < 10000; index++ {
				job := testJob(configs[team], fmt.Sprintf("%d-%d", team, index),
					fmt.Sprintf("repo-%03d", team*10+index%10), ClassWrite, 2, 256)
				if err := scheduler.Enqueue(job); err != nil {
					t.Errorf("team %d enqueue: %v", team, err)
					return
				}
				selected, ok := scheduler.Next()
				if !ok {
					t.Errorf("team %d did not dequeue", team)
					return
				}
				if err := scheduler.Complete(selected); err != nil {
					t.Errorf("team %d complete: %v", team, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for team, scheduler := range schedulers {
		stats := scheduler.Stats()
		if stats.QueuedJobs != 0 || stats.QueuedBytes != 0 || stats.InFlight != 0 ||
			stats.Accepted != 10000 || stats.Completed != 10000 || stats.MaxQueuedJobs > 128 {
			t.Fatalf("team %d stats=%+v", team, stats)
		}
	}
}

func TestConcurrentSameCellAccounting(t *testing.T) {
	repositories := makeRepositories(10)
	config := testConfig("team-00", repositories)
	config.MaxJobs = 10000
	config.MaxJobsPerRepo = 2000
	config.MaxInFlight = 1000
	config.MaxInFlightPerRepo = 1000
	scheduler := mustScheduler(t, config)
	const workers, operations = 16, 100
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < operations; index++ {
				job := testJob(config, fmt.Sprintf("%d-%d", worker, index), repositories[(worker+index)%len(repositories)], ClassWrite, 2, 64)
				if err := scheduler.Enqueue(job); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				if selected, ok := scheduler.Next(); ok {
					if err := scheduler.Complete(selected); err != nil {
						t.Errorf("complete: %v", err)
						return
					}
				} else {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	for {
		job, ok := scheduler.Next()
		if !ok {
			break
		}
		if err := scheduler.Complete(job); err != nil {
			t.Fatal(err)
		}
	}
	stats := scheduler.Stats()
	if stats.Accepted != workers*operations || stats.Completed != workers*operations ||
		stats.QueuedJobs != 0 || stats.InFlight != 0 || stats.MaxObservedInFlight > config.MaxInFlight {
		t.Fatalf("stats=%+v", stats)
	}
}

type jobOption func(*Job)

func withTeam(team string) jobOption { return func(job *Job) { job.TeamID = team } }

func testJob(config Config, id, repository string, class Class, cost, payloadBytes int, options ...jobOption) Job {
	fence := config.Fences[repository]
	job := Job{
		ID: id, TeamID: config.TeamID, RepositoryID: repository,
		ProjectID: fence.ProjectID, DatabaseEpoch: fence.DatabaseEpoch,
		CatalogVersion: fence.CatalogVersion, KeyEpoch: fence.KeyEpoch,
		Class: class, CostUnits: cost, Payload: make([]byte, payloadBytes),
	}
	for _, option := range options {
		option(&job)
	}
	return job
}

func testConfig(team string, repositories []string) Config {
	fences := make(map[string]Fence, len(repositories))
	for _, repository := range repositories {
		fences[repository] = Fence{
			ProjectID: stableUUID("project-" + repository), DatabaseEpoch: stableUUID("epoch-" + repository),
			CatalogVersion: 1, KeyEpoch: 1,
		}
	}
	return Config{
		TeamID: team, Repositories: repositories, Fences: fences, Quantum: 8,
		MaxJobs: 1000, MaxBytes: 8 << 20, MaxJobsPerRepo: 600, MaxBytesPerRepo: 4 << 20,
		MaxJobBytes:            64 << 10,
		InteractiveReserveJobs: 100, InteractiveReserveBytes: 1 << 20,
		InteractiveReserveJobsPerRepo: 10, InteractiveReserveBytesPerRepo: 64 << 10,
		MaxInFlight: 64, MaxInFlightPerRepo: 16, InteractiveInFlightReserve: 8,
		MaxInFlightBytes: 4 << 20, MaxInFlightBytesPerRepo: 1 << 20,
		InteractiveInFlightReserveBytes: 512 << 10,
	}
}

func makeRepositories(count int) []string {
	repositories := make([]string, count)
	for index := range repositories {
		repositories[index] = fmt.Sprintf("repo-%03d", index)
	}
	return repositories
}

func stableUUID(value string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(value)).String()
}

func mustScheduler(t *testing.T, config Config) *Scheduler {
	t.Helper()
	scheduler, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler
}
