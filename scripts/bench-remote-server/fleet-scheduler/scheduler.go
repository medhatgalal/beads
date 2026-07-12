package fleetscheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrWrongTeam         = errors.New("job does not belong to this team cell")
	ErrUnknownRepository = errors.New("repository is not cataloged in this team cell")
	ErrQueueFull         = errors.New("bounded scheduler queue is full")
	ErrStaleJob          = errors.New("job identity epochs are stale")
	ErrInvalidJob        = errors.New("job is malformed or exceeds its per-job bound")
	ErrDuplicateJob      = errors.New("job identity is already queued or in flight")
	ErrUnknownCompletion = errors.New("job is not in flight")
)

const (
	maximumRepositories          = 1000
	maximumConfiguredJobs        = 1_000_000
	maximumConfiguredBytes int64 = 1 << 40  // 1 TiB hard configuration ceiling.
	maximumJobBytes        int64 = 64 << 20 // 64 MiB exact serialized-job ceiling.
	maximumJobIDBytes            = 128
	maximumIdentityBytes         = 128
	maximumCostUnits             = 64
	maximumInFlight              = 10_000
)

type Class string

const (
	ClassInteractive Class = "interactive"
	ClassWrite       Class = "write"
	ClassBulk        Class = "bulk"
	ClassReconcile   Class = "reconcile"
)

var classPattern = []Class{
	ClassInteractive, ClassInteractive, ClassInteractive, ClassInteractive,
	ClassInteractive, ClassInteractive, ClassInteractive, ClassInteractive,
	ClassWrite, ClassWrite, ClassWrite, ClassWrite,
	ClassBulk, ClassReconcile,
}

type Fence struct {
	ProjectID      string
	DatabaseEpoch  string
	CatalogVersion uint64
	KeyEpoch       uint64
}

type Job struct {
	ID             string
	TeamID         string
	RepositoryID   string
	ProjectID      string
	DatabaseEpoch  string
	CatalogVersion uint64
	KeyEpoch       uint64
	Class          Class
	CostUnits      int
	Payload        []byte
}

type Config struct {
	TeamID          string
	Repositories    []string
	Fences          map[string]Fence
	Quantum         int
	MaxJobs         int
	MaxBytes        int64
	MaxJobsPerRepo  int
	MaxBytesPerRepo int64
	MaxJobBytes     int64

	InteractiveReserveJobs         int
	InteractiveReserveBytes        int64
	InteractiveReserveJobsPerRepo  int
	InteractiveReserveBytesPerRepo int64

	MaxInFlight                     int
	MaxInFlightPerRepo              int
	MaxInFlightBytes                int64
	MaxInFlightBytesPerRepo         int64
	InteractiveInFlightReserve      int
	InteractiveInFlightReserveBytes int64
}

type Stats struct {
	QueuedJobs                   int   `json:"queued_jobs"`
	QueuedBytes                  int64 `json:"queued_bytes"`
	QueuedNonInteractiveJobs     int   `json:"queued_non_interactive_jobs"`
	QueuedNonInteractiveBytes    int64 `json:"queued_non_interactive_bytes"`
	MaxQueuedJobs                int   `json:"max_queued_jobs"`
	MaxQueuedBytes               int64 `json:"max_queued_bytes"`
	InFlight                     int   `json:"in_flight"`
	InFlightBytes                int64 `json:"in_flight_bytes"`
	InFlightNonInteractive       int   `json:"in_flight_non_interactive"`
	InFlightNonInteractiveBytes  int64 `json:"in_flight_non_interactive_bytes"`
	MaxObservedInFlight          int   `json:"max_observed_in_flight"`
	MaxObservedInFlightBytes     int64 `json:"max_observed_in_flight_bytes"`
	Accepted                     int64 `json:"accepted"`
	Dequeued                     int64 `json:"dequeued"`
	Completed                    int64 `json:"completed"`
	RejectedCapacity             int64 `json:"rejected_capacity"`
	RejectedIdentity             int64 `json:"rejected_identity"`
	RejectedDuplicate            int64 `json:"rejected_duplicate"`
	StaleQueuedDropped           int64 `json:"stale_queued_dropped"`
	StaleInFlight                int   `json:"stale_in_flight"`
	MaxObservedRepositoryDeficit int   `json:"max_observed_repository_deficit"`
}

type queuedJob struct {
	job   Job
	bytes int64
}

type repoQueue struct {
	jobs                      map[Class][]queuedJob
	fence                     Fence
	queuedJobs                int
	queuedBytes               int64
	queuedNonInteractiveJobs  int
	queuedNonInteractiveBytes int64
	inFlight                  int
	inFlightBytes             int64
	deficit                   int
	classCursor               int
}

type inFlightJob struct {
	job   Job
	bytes int64
	stale bool
}

type Scheduler struct {
	mu             sync.Mutex
	config         Config
	order          []string
	repos          map[string]*repoQueue
	queuedIDs      map[string]struct{}
	inFlight       map[string]*inFlightJob
	cursor         int
	catalogVersion uint64
	keyEpoch       uint64
	stats          Stats
}

func New(config Config) (*Scheduler, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	order := append([]string(nil), config.Repositories...)
	sort.Strings(order)
	s := &Scheduler{
		config: config, order: order, repos: make(map[string]*repoQueue, len(order)),
		queuedIDs: make(map[string]struct{}), inFlight: make(map[string]*inFlightJob),
	}
	for _, repository := range order {
		fence := config.Fences[repository]
		if s.catalogVersion == 0 {
			s.catalogVersion, s.keyEpoch = fence.CatalogVersion, fence.KeyEpoch
		}
		s.repos[repository] = &repoQueue{jobs: map[Class][]queuedJob{}, fence: fence}
	}
	return s, nil
}

func validateConfig(config Config) error {
	if !validLogicalID(config.TeamID) || len(config.Repositories) == 0 || len(config.Repositories) > maximumRepositories ||
		len(config.Fences) != len(config.Repositories) || config.Quantum < 1 || config.Quantum > 256 ||
		config.MaxJobs < 1 || config.MaxJobs > maximumConfiguredJobs || config.MaxBytes < 1 ||
		config.MaxBytes > maximumConfiguredBytes || config.MaxJobsPerRepo < 1 ||
		config.MaxJobsPerRepo > config.MaxJobs || config.MaxBytesPerRepo < 1 ||
		config.MaxBytesPerRepo > config.MaxBytes || config.MaxJobBytes < 1 ||
		config.MaxJobBytes > maximumJobBytes || config.MaxJobBytes > config.MaxBytesPerRepo ||
		config.MaxInFlight < 1 || config.MaxInFlight > maximumInFlight ||
		config.MaxInFlightPerRepo < 1 || config.MaxInFlightPerRepo > config.MaxInFlight ||
		config.MaxInFlightBytes < 1 || config.MaxInFlightBytes > maximumConfiguredBytes ||
		config.MaxInFlightBytesPerRepo < 1 || config.MaxInFlightBytesPerRepo > config.MaxInFlightBytes ||
		config.MaxJobBytes > config.MaxInFlightBytesPerRepo ||
		config.InteractiveReserveJobs < 0 || config.InteractiveReserveJobs > config.MaxJobs ||
		config.InteractiveReserveBytes < 0 || config.InteractiveReserveBytes > config.MaxBytes ||
		config.InteractiveReserveJobsPerRepo < 0 || config.InteractiveReserveJobsPerRepo > config.MaxJobsPerRepo ||
		config.InteractiveReserveBytesPerRepo < 0 || config.InteractiveReserveBytesPerRepo > config.MaxBytesPerRepo ||
		config.InteractiveInFlightReserve < 0 || config.InteractiveInFlightReserve > config.MaxInFlight ||
		config.InteractiveInFlightReserveBytes < 0 ||
		config.InteractiveInFlightReserveBytes > config.MaxInFlightBytes {
		return fmt.Errorf("invalid bounded scheduler configuration")
	}
	seen := make(map[string]struct{}, len(config.Repositories))
	var catalogVersion, keyEpoch uint64
	for _, repository := range config.Repositories {
		if !validLogicalID(repository) {
			return fmt.Errorf("invalid repository identity")
		}
		if _, exists := seen[repository]; exists {
			return fmt.Errorf("duplicate repository identity")
		}
		seen[repository] = struct{}{}
		fence, exists := config.Fences[repository]
		if !exists || !validFence(fence) {
			return fmt.Errorf("missing or invalid repository fence")
		}
		if catalogVersion == 0 {
			catalogVersion, keyEpoch = fence.CatalogVersion, fence.KeyEpoch
		} else if fence.CatalogVersion != catalogVersion || fence.KeyEpoch != keyEpoch {
			return fmt.Errorf("repository fences must share catalog and key epochs")
		}
	}
	return nil
}

func (s *Scheduler) Enqueue(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.TeamID != s.config.TeamID {
		s.stats.RejectedIdentity++
		return ErrWrongTeam
	}
	repo := s.repos[job.RepositoryID]
	if repo == nil {
		s.stats.RejectedIdentity++
		return ErrUnknownRepository
	}
	bytes, err := s.validateAndSizeJobLocked(job, repo)
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			s.stats.RejectedCapacity++
		} else {
			s.stats.RejectedIdentity++
		}
		return err
	}
	key := jobKey(job)
	if _, exists := s.queuedIDs[key]; exists || s.inFlight[key] != nil {
		s.stats.RejectedDuplicate++
		return ErrDuplicateJob
	}
	if !fits(s.stats.QueuedJobs, 1, s.config.MaxJobs) || !fits64(s.stats.QueuedBytes, bytes, s.config.MaxBytes) ||
		!fits(repo.queuedJobs, 1, s.config.MaxJobsPerRepo) || !fits64(repo.queuedBytes, bytes, s.config.MaxBytesPerRepo) {
		s.stats.RejectedCapacity++
		return ErrQueueFull
	}
	latencySensitive := isLatencySensitive(job.Class)
	if !latencySensitive &&
		(!fits(s.stats.QueuedNonInteractiveJobs, 1, s.config.MaxJobs-s.config.InteractiveReserveJobs) ||
			!fits64(s.stats.QueuedNonInteractiveBytes, bytes, s.config.MaxBytes-s.config.InteractiveReserveBytes) ||
			!fits(repo.queuedNonInteractiveJobs, 1, s.config.MaxJobsPerRepo-s.config.InteractiveReserveJobsPerRepo) ||
			!fits64(repo.queuedNonInteractiveBytes, bytes, s.config.MaxBytesPerRepo-s.config.InteractiveReserveBytesPerRepo)) {
		s.stats.RejectedCapacity++
		return ErrQueueFull
	}

	job.Payload = append([]byte(nil), job.Payload...)
	repo.jobs[job.Class] = append(repo.jobs[job.Class], queuedJob{job: job, bytes: bytes})
	s.queuedIDs[key] = struct{}{}
	repo.queuedJobs++
	repo.queuedBytes += bytes
	s.stats.QueuedJobs++
	s.stats.QueuedBytes += bytes
	if !latencySensitive {
		repo.queuedNonInteractiveJobs++
		repo.queuedNonInteractiveBytes += bytes
		s.stats.QueuedNonInteractiveJobs++
		s.stats.QueuedNonInteractiveBytes += bytes
	}
	s.stats.Accepted++
	if s.stats.QueuedJobs > s.stats.MaxQueuedJobs {
		s.stats.MaxQueuedJobs = s.stats.QueuedJobs
	}
	if s.stats.QueuedBytes > s.stats.MaxQueuedBytes {
		s.stats.MaxQueuedBytes = s.stats.QueuedBytes
	}
	return nil
}

func (s *Scheduler) Next() (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stats.QueuedJobs == 0 || s.stats.InFlight >= s.config.MaxInFlight {
		return Job{}, false
	}
	maxRounds := len(s.order) * ((maximumCostUnits+s.config.Quantum-1)/s.config.Quantum + 2)
	for round := 0; round < maxRounds; round++ {
		repository := s.order[s.cursor]
		s.cursor = (s.cursor + 1) % len(s.order)
		repo := s.repos[repository]
		s.purgeStaleLocked(repo)
		if repo.queuedJobs == 0 || repo.inFlight >= s.config.MaxInFlightPerRepo {
			continue
		}
		allowNonInteractive := s.stats.InFlightNonInteractive < s.config.MaxInFlight-s.config.InteractiveInFlightReserve
		repo.deficit = min(maximumCostUnits, repo.deficit+s.config.Quantum)
		if repo.deficit > s.stats.MaxObservedRepositoryDeficit {
			s.stats.MaxObservedRepositoryDeficit = repo.deficit
		}
		candidate, class, ok := repo.peekWeighted(allowNonInteractive)
		if !ok || candidate.job.CostUnits > repo.deficit {
			continue
		}
		if !fits64(s.stats.InFlightBytes, candidate.bytes, s.config.MaxInFlightBytes) ||
			!fits64(repo.inFlightBytes, candidate.bytes, s.config.MaxInFlightBytesPerRepo) ||
			(!isLatencySensitive(candidate.job.Class) &&
				!fits64(s.stats.InFlightNonInteractiveBytes, candidate.bytes,
					s.config.MaxInFlightBytes-s.config.InteractiveInFlightReserveBytes)) {
			continue
		}
		queue := repo.jobs[class]
		queue[0] = queuedJob{}
		repo.jobs[class] = compactQueue(queue[1:])
		s.removeQueuedLocked(repo, candidate)
		repo.deficit -= candidate.job.CostUnits
		if repo.queuedJobs == 0 {
			repo.deficit = 0
			repo.classCursor = 0
		}
		key := jobKey(candidate.job)
		delete(s.queuedIDs, key)
		s.inFlight[key] = &inFlightJob{job: candidate.job, bytes: candidate.bytes}
		repo.inFlight++
		repo.inFlightBytes += candidate.bytes
		s.stats.InFlight++
		s.stats.InFlightBytes += candidate.bytes
		if !isLatencySensitive(candidate.job.Class) {
			s.stats.InFlightNonInteractive++
			s.stats.InFlightNonInteractiveBytes += candidate.bytes
		}
		if s.stats.InFlight > s.stats.MaxObservedInFlight {
			s.stats.MaxObservedInFlight = s.stats.InFlight
		}
		if s.stats.InFlightBytes > s.stats.MaxObservedInFlightBytes {
			s.stats.MaxObservedInFlightBytes = s.stats.InFlightBytes
		}
		s.stats.Dequeued++
		return candidate.job, true
	}
	return Job{}, false
}

func (s *Scheduler) Complete(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobKey(job)
	entry := s.inFlight[key]
	if entry == nil {
		return ErrUnknownCompletion
	}
	if entry.job.TeamID != job.TeamID || entry.job.RepositoryID != job.RepositoryID ||
		entry.job.ProjectID != job.ProjectID || entry.job.DatabaseEpoch != job.DatabaseEpoch ||
		entry.job.CatalogVersion != job.CatalogVersion || entry.job.KeyEpoch != job.KeyEpoch ||
		entry.job.Class != job.Class {
		return ErrUnknownCompletion
	}
	delete(s.inFlight, key)
	repo := s.repos[entry.job.RepositoryID]
	repo.inFlight--
	repo.inFlightBytes -= entry.bytes
	s.stats.InFlight--
	s.stats.InFlightBytes -= entry.bytes
	if !isLatencySensitive(entry.job.Class) {
		s.stats.InFlightNonInteractive--
		s.stats.InFlightNonInteractiveBytes -= entry.bytes
	}
	s.stats.Completed++
	if entry.stale || !repo.fence.matches(entry.job) {
		if entry.stale {
			s.stats.StaleInFlight--
		}
		return ErrStaleJob
	}
	return nil
}

// UpdateCatalog atomically replaces every repository fence. Queued work from
// an older catalog/key/database epoch is removed before it can be selected.
// Already-running work is marked stale; the executor must also check the same
// fence transactionally before applying a mutation.
func (s *Scheduler) UpdateCatalog(next map[string]Fence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(next) != len(s.repos) {
		return fmt.Errorf("replacement catalog must cover the complete team cell")
	}
	var catalogVersion, keyEpoch uint64
	for repository := range s.repos {
		fence, exists := next[repository]
		if !exists || !validFence(fence) {
			return fmt.Errorf("missing or invalid replacement fence")
		}
		if catalogVersion == 0 {
			catalogVersion, keyEpoch = fence.CatalogVersion, fence.KeyEpoch
		} else if fence.CatalogVersion != catalogVersion || fence.KeyEpoch != keyEpoch {
			return fmt.Errorf("replacement fences disagree on catalog or key epoch")
		}
	}
	if catalogVersion <= s.catalogVersion {
		return fmt.Errorf("replacement catalog is not newer")
	}
	if keyEpoch < s.keyEpoch {
		return fmt.Errorf("replacement key epoch would move backwards")
	}
	for repository, repo := range s.repos {
		repo.fence = next[repository]
		s.purgeStaleLocked(repo)
		repo.deficit = 0
		repo.classCursor = 0
	}
	for _, entry := range s.inFlight {
		if !s.repos[entry.job.RepositoryID].fence.matches(entry.job) && !entry.stale {
			entry.stale = true
			s.stats.StaleInFlight++
		}
	}
	s.catalogVersion, s.keyEpoch = catalogVersion, keyEpoch
	return nil
}

func (s *Scheduler) validateAndSizeJobLocked(job Job, repo *repoQueue) (int64, error) {
	if !validClass(job.Class) || len(job.ID) == 0 || len(job.ID) > maximumJobIDBytes ||
		job.CostUnits < 1 || job.CostUnits > maximumCostUnits || len(job.Payload) == 0 ||
		int64(len(job.Payload)) > s.config.MaxJobBytes {
		return 0, ErrInvalidJob
	}
	if !repo.fence.matches(job) {
		return 0, ErrStaleJob
	}
	bytes := int64(len(job.Payload) + len(job.ID) + len(job.TeamID) + len(job.RepositoryID) +
		len(job.ProjectID) + len(job.DatabaseEpoch) + len(job.Class))
	if bytes > s.config.MaxJobBytes {
		return 0, ErrQueueFull
	}
	return bytes, nil
}

func (s *Scheduler) purgeStaleLocked(repo *repoQueue) {
	for _, class := range []Class{ClassInteractive, ClassWrite, ClassBulk, ClassReconcile} {
		queue := repo.jobs[class]
		kept := queue[:0]
		for _, candidate := range queue {
			if repo.fence.matches(candidate.job) {
				kept = append(kept, candidate)
				continue
			}
			delete(s.queuedIDs, jobKey(candidate.job))
			s.removeQueuedLocked(repo, candidate)
			s.stats.StaleQueuedDropped++
		}
		repo.jobs[class] = kept
		if len(kept) == 0 {
			repo.jobs[class] = nil
		} else {
			clear(queue[len(kept):])
			repo.jobs[class] = compactQueue(kept)
		}
	}
	if repo.queuedJobs == 0 {
		repo.deficit = 0
		repo.classCursor = 0
	}
}

func (s *Scheduler) removeQueuedLocked(repo *repoQueue, candidate queuedJob) {
	repo.queuedJobs--
	repo.queuedBytes -= candidate.bytes
	s.stats.QueuedJobs--
	s.stats.QueuedBytes -= candidate.bytes
	if !isLatencySensitive(candidate.job.Class) {
		repo.queuedNonInteractiveJobs--
		repo.queuedNonInteractiveBytes -= candidate.bytes
		s.stats.QueuedNonInteractiveJobs--
		s.stats.QueuedNonInteractiveBytes -= candidate.bytes
	}
}

func (q *repoQueue) peekWeighted(allowNonInteractive bool) (queuedJob, Class, bool) {
	for attempt := 0; attempt < len(classPattern); attempt++ {
		class := classPattern[q.classCursor]
		q.classCursor = (q.classCursor + 1) % len(classPattern)
		if !allowNonInteractive && !isLatencySensitive(class) {
			continue
		}
		if queue := q.jobs[class]; len(queue) > 0 {
			return queue[0], class, true
		}
	}
	return queuedJob{}, "", false
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (f Fence) matches(job Job) bool {
	return f.ProjectID == job.ProjectID && f.DatabaseEpoch == job.DatabaseEpoch &&
		f.CatalogVersion == job.CatalogVersion && f.KeyEpoch == job.KeyEpoch
}

func validFence(fence Fence) bool {
	if fence.CatalogVersion == 0 || fence.KeyEpoch == 0 || len(fence.ProjectID) > maximumIdentityBytes ||
		len(fence.DatabaseEpoch) > maximumIdentityBytes {
		return false
	}
	_, projectErr := uuid.Parse(fence.ProjectID)
	_, epochErr := uuid.Parse(fence.DatabaseEpoch)
	return projectErr == nil && epochErr == nil
}

func validLogicalID(value string) bool {
	return len(value) > 0 && len(value) <= maximumIdentityBytes
}

func fits(current, delta, limit int) bool { //nolint:unparam // delta kept for symmetry with fits64
	return current >= 0 && delta >= 0 && current <= limit && delta <= limit-current
}

func fits64(current, delta, limit int64) bool {
	return current >= 0 && delta >= 0 && current <= limit && delta <= limit-current
}

func compactQueue(queue []queuedJob) []queuedJob {
	if len(queue) == 0 {
		return nil
	}
	if cap(queue) > 64 && len(queue)*4 < cap(queue) {
		compacted := make([]queuedJob, len(queue))
		copy(compacted, queue)
		return compacted
	}
	return queue
}

func jobKey(job Job) string { return job.RepositoryID + "\x00" + job.ID }

func isLatencySensitive(class Class) bool { return class == ClassInteractive || class == ClassWrite }

func validClass(class Class) bool {
	switch class {
	case ClassInteractive, ClassWrite, ClassBulk, ClassReconcile:
		return true
	default:
		return false
	}
}
