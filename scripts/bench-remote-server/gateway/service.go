package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

const (
	maxWorkRequests    = 56
	maxControlRequests = 8
)

type gatewayService struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	projectID             string
	token                 []byte
	stableKey             []byte
	subject               string
	subjectHash           string
	queue                 *operationQueue
	backend               operationBackend
	notify                chan struct{}
	stop                  chan struct{}
	done                  chan struct{}
	reconcileDone         chan struct{}
	stopOnce              sync.Once
	transitionRetries     atomic.Uint64
	requests              chan struct{}
	controlRequests       chan struct{}
	failpoint             string
	allowLegacyOperations bool
	handlerMu             sync.Mutex
	handlerWG             sync.WaitGroup
	draining              bool
}

type gatewayAuth struct {
	BearerToken []byte
	StableKey   []byte
	Subject     string
}

func newGatewayService(ctx context.Context, projectID string, auth gatewayAuth, queue *operationQueue, backend operationBackend) (*gatewayService, error) {
	if len(auth.BearerToken) < 32 || len(auth.StableKey) < 32 || string(auth.BearerToken) == string(auth.StableKey) {
		return nil, fmt.Errorf("gateway authentication secrets must be distinct and at least 32 bytes")
	}
	if auth.Subject == "" {
		return nil, fmt.Errorf("gateway subject is required")
	}
	if queue == nil || queue.binding.ProjectID != projectID {
		return nil, fmt.Errorf("gateway queue project binding mismatch")
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	s := &gatewayService{
		ctx:             workerCtx,
		cancel:          cancel,
		projectID:       projectID,
		token:           append([]byte(nil), auth.BearerToken...),
		stableKey:       append([]byte(nil), auth.StableKey...),
		subject:         auth.Subject,
		subjectHash:     hmacHex(auth.StableKey, "subject\x00"+auth.Subject),
		queue:           queue,
		backend:         backend,
		notify:          make(chan struct{}, 1),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		reconcileDone:   make(chan struct{}),
		requests:        make(chan struct{}, maxWorkRequests),
		controlRequests: make(chan struct{}, maxControlRequests),
		failpoint:       os.Getenv("BEADS_PERF_GATEWAY_FAILPOINT"),
	}
	if s.failpoint != "" && s.failpoint != "after_lease" && s.failpoint != "after_dolt_commit" {
		cancel()
		return nil, fmt.Errorf("unknown BEADS_PERF_GATEWAY_FAILPOINT")
	}
	if err := s.recover(ctx); err != nil {
		cancel()
		return nil, err
	}
	go s.worker()
	go s.reconciler()
	return s, nil
}

func (s *gatewayService) recover(ctx context.Context) error {
	if err := s.queue.markUnknownRunning(ctx); err != nil {
		return err
	}
	return s.reconcileUnknown(ctx)
}

func (s *gatewayService) reconcileUnknown(ctx context.Context) error {
	unknown, err := s.queue.unknown(ctx)
	if err != nil {
		return err
	}
	var reconcileErrors []error
	for _, op := range unknown {
		receipt, err := s.backend.LookupReceipt(ctx, op)
		if err != nil {
			if errors.Is(err, errReceiptIdentityMismatch) {
				if quarantineErr := s.queue.quarantineIdentityConflict(ctx, op); quarantineErr != nil &&
					!errors.Is(quarantineErr, errStaleQueueIdentity) {
					reconcileErrors = append(reconcileErrors,
						fmt.Errorf("quarantine operation %s: %w", op.ID, quarantineErr))
				}
				continue
			}
			reconcileErrors = append(reconcileErrors, fmt.Errorf("reconcile operation %s: %w", op.ID, err))
			continue
		}
		if receipt != nil {
			receipt.Reconciled = true
			if err := s.queue.markOutcome(ctx, op, receipt); err != nil {
				reconcileErrors = append(reconcileErrors, err)
			}
			continue
		}
		if err := s.queue.requeueReconciled(ctx, op); err != nil && !errors.Is(err, errStaleQueueIdentity) {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	return errors.Join(reconcileErrors...)
}

func (s *gatewayService) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *gatewayService) beginHandler() bool {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	if s.draining {
		return false
	}
	s.handlerWG.Add(1)
	return true
}

func (s *gatewayService) endHandler() { s.handlerWG.Done() }

func (s *gatewayService) beginDrain() {
	s.handlerMu.Lock()
	s.draining = true
	s.handlerMu.Unlock()
}

func (s *gatewayService) waitHandlers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.handlerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *gatewayService) Close() {
	s.stopOnce.Do(func() {
		s.beginDrain()
		s.cancel()
		close(s.stop)
		<-s.done
		<-s.reconcileDone
		zero(s.token)
		zero(s.stableKey)
	})
}

func (s *gatewayService) worker() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		op, ok, err := s.queue.leaseNext(ctx, 10*time.Minute)
		cancel()
		if err != nil {
			select {
			case <-s.stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		if !ok {
			select {
			case <-s.stop:
				return
			case <-s.notify:
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		if s.failpoint == "after_lease" {
			os.Exit(85)
		}
		s.execute(op)
	}
}

// reconciler runs independently of mutation traffic. Unknown outcomes must not
// wait for the main worker queue to become idle; a continuously busy database
// would otherwise leave an acknowledged operation unresolved indefinitely.
func (s *gatewayService) reconciler() {
	defer close(s.reconcileDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
			err := s.reconcileUnknown(ctx)
			cancel()
			if err != nil && s.ctx.Err() == nil {
				log.Printf("gateway unknown reconciliation deferred error_class=%s", safeLogError(err))
			}
		}
	}
}

func (s *gatewayService) execute(op operation) {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
	receipt, execErr := s.backend.Execute(ctx, op)
	cancel()
	if execErr == nil && receipt == nil {
		execErr = fmt.Errorf("backend returned no receipt")
	}
	if execErr == nil {
		if s.failpoint == "after_dolt_commit" {
			os.Exit(86)
		}
		if err := s.persistTransition(op.ID, "succeeded", func(ctx context.Context) error {
			return s.queue.markOutcome(ctx, op, receipt)
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=succeeded error_class=%s", op.ID, safeLogError(err))
		}
		return
	}

	reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer reconcileCancel()
	receipt, lookupErr := s.backend.LookupReceipt(reconcileCtx, op)
	if lookupErr != nil {
		if errors.Is(lookupErr, errReceiptIdentityMismatch) {
			if err := s.persistTransition(op.ID, "canonical_identity_conflict", func(ctx context.Context) error {
				return s.queue.quarantineIdentityConflict(ctx, op)
			}); err != nil {
				log.Printf("gateway queue transition failed operation=%s transition=canonical_identity_conflict error_class=%s", op.ID, safeLogError(err))
			}
			return
		}
		if err := s.persistTransition(op.ID, "unknown", func(ctx context.Context) error {
			return s.queue.markUnknown(ctx, op, safeOperationError(lookupErr))
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=unknown error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	if receipt != nil {
		receipt.Reconciled = true
		if err := s.persistTransition(op.ID, "reconciled", func(ctx context.Context) error {
			return s.queue.markOutcome(ctx, op, receipt)
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=reconciled error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	if errors.Is(execErr, context.Canceled) && s.ctx.Err() != nil {
		if err := s.persistTransition(op.ID, "shutdown_unknown", func(ctx context.Context) error {
			return s.queue.markUnknown(ctx, op, safeOperationError(execErr))
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=shutdown_unknown error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	knownPermanent := isPermanent(execErr)
	if isTerminalIdempotencyNamespace(op.KeyHash) && knownPermanent {
		failure, persistErr := s.backend.PersistPermanentFailure(
			reconcileCtx, op, "operation_failed", safeOperationError(execErr))
		if persistErr != nil {
			if err := s.persistTransition(op.ID, "terminal_failure_unknown", func(ctx context.Context) error {
				return s.queue.markUnknown(ctx, op, safeOperationError(persistErr))
			}); err != nil {
				log.Printf("gateway queue transition failed operation=%s transition=terminal_failure_unknown error_class=%s", op.ID, safeLogError(err))
			}
			return
		}
		if err := s.persistTransition(op.ID, "terminal_failed", func(ctx context.Context) error {
			return s.queue.markOutcome(ctx, op, failure)
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=terminal_failed error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	if !isTerminalIdempotencyNamespace(op.KeyHash) && (knownPermanent || !isTransient(execErr) || op.Attempts >= 3) {
		if err := s.persistTransition(op.ID, "failed", func(ctx context.Context) error {
			return s.queue.markFailed(ctx, op, "operation_failed", safeOperationError(execErr))
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=failed error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	if isTerminalIdempotencyNamespace(op.KeyHash) && (!isTransient(execErr) || op.Attempts >= 3) {
		// A retry budget is not a permanent domain outcome. Leave the operation
		// unacknowledged and reconcilable instead of manufacturing a queue-only
		// terminal failure that could incorrectly produce HTTP 200.
		if err := s.persistTransition(op.ID, "retry_exhausted", func(ctx context.Context) error {
			return s.queue.markRetryExhausted(ctx, op, safeOperationError(execErr))
		}); err != nil {
			log.Printf("gateway queue transition failed operation=%s transition=retry_exhausted_unknown error_class=%s", op.ID, safeLogError(err))
		}
		return
	}
	delay := time.Duration(op.Attempts*op.Attempts) * 250 * time.Millisecond
	if err := s.persistTransition(op.ID, "retry_wait", func(ctx context.Context) error {
		return s.queue.markRetry(ctx, op, safeOperationError(execErr), delay)
	}); err != nil {
		log.Printf("gateway queue transition failed operation=%s transition=retry_wait error_class=%s", op.ID, safeLogError(err))
	} else {
		s.signal()
	}
}

func (s *gatewayService) persistTransition(operationID, transition string, fn func(context.Context) error) error {
	delay := 25 * time.Millisecond
	for {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := fn(attemptCtx)
		cancel()
		if err == nil || errors.Is(err, errStaleQueueIdentity) {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return errors.Join(err, s.ctx.Err())
		case <-time.After(delay):
			s.transitionRetries.Add(1)
			if delay < 500*time.Millisecond {
				delay *= 2
			}
			log.Printf("gateway queue transition retry operation=%s transition=%s error_class=%s", operationID, transition, safeLogError(err))
		}
	}
}

func safeOperationError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "operation deadline exceeded"
	case errors.Is(err, context.Canceled):
		return "operation canceled"
	case isTransient(err):
		return "transient backend failure"
	case isPermanent(err):
		return "operation rejected"
	default:
		return "operation failed"
	}
}

func safeLogError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case isTransient(err):
		return "transient"
	case isPermanent(err):
		return "permanent"
	default:
		return "internal"
	}
}

func isTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1205 || mysqlErr.Number == 1213) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"connection reset", "invalid connection", "broken pipe", "deadlock",
		"lock wait", "timeout", "temporarily unavailable", "connection refused",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
