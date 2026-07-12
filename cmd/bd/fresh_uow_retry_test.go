package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
)

type proxiedRetryTestProvider struct {
	uows  []uow.UnitOfWork
	calls int
}

func (p *proxiedRetryTestProvider) NewUOW(context.Context) (uow.UnitOfWork, error) {
	if p.calls >= len(p.uows) {
		return nil, fmt.Errorf("unexpected NewUOW call %d", p.calls+1)
	}
	uw := p.uows[p.calls]
	p.calls++
	return uw, nil
}

func (*proxiedRetryTestProvider) Close(context.Context) error { return nil }

type proxiedRetryTestUOW struct {
	uow.UnitOfWork
	issueUC       domain.IssueUseCase
	commitErr     error
	commitCalls   int
	closeCalls    int
	commitMessage string
}

func (u *proxiedRetryTestUOW) IssueUseCase() domain.IssueUseCase { return u.issueUC }
func (u *proxiedRetryTestUOW) Commit(_ context.Context, message string) error {
	u.commitCalls++
	u.commitMessage = message
	return u.commitErr
}
func (u *proxiedRetryTestUOW) Close(context.Context) { u.closeCalls++ }

type proxiedRetryTestIssueUC struct {
	domain.IssueUseCase
	current         *types.Issue
	updated         *types.Issue
	ready           domain.ClaimReadyResult
	getCalls        int
	applyCalls      int
	claimReadyCalls int
	heartbeatCalls  int
}

func (u *proxiedRetryTestIssueUC) GetIssue(context.Context, string) (*types.Issue, error) {
	u.getCalls++
	if u.current == nil {
		return nil, errors.New("missing test issue")
	}
	return u.current, nil
}

func (u *proxiedRetryTestIssueUC) ApplyUpdate(context.Context, string, domain.UpdateSpec, string) (*types.Issue, error) {
	u.applyCalls++
	return u.updated, nil
}

func (u *proxiedRetryTestIssueUC) ClaimReadyIssue(context.Context, types.WorkFilter, string) (domain.ClaimReadyResult, error) {
	u.claimReadyCalls++
	return u.ready, nil
}

func (u *proxiedRetryTestIssueUC) HeartbeatIssue(context.Context, string, string) error {
	u.heartbeatCalls++
	return nil
}

func TestApplyUpdateProxiedOneRetriesWholeOperationOnFreshUOW(t *testing.T) {
	t.Chdir(t.TempDir())
	oldProvider, oldActor := uowProvider, actor
	t.Cleanup(func() { uowProvider, actor = oldProvider, oldActor })
	actor = "retry-agent"

	firstUC := &proxiedRetryTestIssueUC{
		current: &types.Issue{ID: "retry-1", Title: "first snapshot", Status: types.StatusOpen, IssueType: types.TypeTask},
		updated: &types.Issue{ID: "retry-1", Title: "failed attempt", Status: types.StatusOpen, IssueType: types.TypeTask},
	}
	secondUC := &proxiedRetryTestIssueUC{
		current: &types.Issue{ID: "retry-1", Title: "fresh snapshot", Status: types.StatusOpen, IssueType: types.TypeTask},
		updated: &types.Issue{ID: "retry-1", Title: "durable attempt", Status: types.StatusOpen, IssueType: types.TypeTask},
	}
	firstUOW := &proxiedRetryTestUOW{
		issueUC:   firstUC,
		commitErr: &mysql.MySQLError{Number: 1213, Message: "serialization conflict"},
	}
	secondUOW := &proxiedRetryTestUOW{issueUC: secondUC}
	provider := &proxiedRetryTestProvider{uows: []uow.UnitOfWork{firstUOW, secondUOW}}
	uowProvider = provider

	got, ok, claimLost, err := applyUpdateProxiedOne(context.Background(), "retry-1", &updateInput{
		fields: map[string]any{"title": "requested title"},
	})
	if err != nil {
		t.Fatalf("applyUpdateProxiedOne: %v", err)
	}
	if !ok || claimLost {
		t.Fatalf("result ok=%v claimLost=%v, want true/false", ok, claimLost)
	}
	if got != secondUC.updated {
		t.Fatalf("returned issue = %#v, want durable second-attempt issue %#v", got, secondUC.updated)
	}
	if provider.calls != 2 || firstUC.getCalls != 1 || firstUC.applyCalls != 1 || secondUC.getCalls != 1 || secondUC.applyCalls != 1 {
		t.Fatalf("fresh replay counts: provider=%d first(get=%d apply=%d) second(get=%d apply=%d)",
			provider.calls, firstUC.getCalls, firstUC.applyCalls, secondUC.getCalls, secondUC.applyCalls)
	}
	if firstUOW.closeCalls != 1 || secondUOW.closeCalls != 1 {
		t.Fatalf("UOW closes: first=%d second=%d, want 1/1", firstUOW.closeCalls, secondUOW.closeCalls)
	}
	for i, uw := range []*proxiedRetryTestUOW{firstUOW, secondUOW} {
		if uw.commitCalls != 1 || uw.commitMessage != "bd: update retry-1" {
			t.Errorf("UOW %d commit calls/message = %d/%q", i, uw.commitCalls, uw.commitMessage)
		}
	}
}

func TestRunReadyProxiedClaimRetriesWholeOperationOnFreshUOW(t *testing.T) {
	t.Chdir(t.TempDir())
	oldProvider, oldActor, oldReadonly := uowProvider, actor, readonlyMode
	t.Cleanup(func() { uowProvider, actor, readonlyMode = oldProvider, oldActor, oldReadonly })
	actor = "ready-retry-agent"
	readonlyMode = false

	firstUC := &proxiedRetryTestIssueUC{ready: domain.ClaimReadyResult{
		Issue:   &types.Issue{ID: "ready-failed", Title: "failed attempt", Status: types.StatusInProgress, IssueType: types.TypeTask},
		Claimed: true,
	}}
	secondUC := &proxiedRetryTestIssueUC{ready: domain.ClaimReadyResult{
		Issue:   &types.Issue{ID: "ready-durable", Title: "durable attempt", Status: types.StatusInProgress, IssueType: types.TypeTask},
		Claimed: true,
	}}
	firstUOW := &proxiedRetryTestUOW{
		issueUC:   firstUC,
		commitErr: &mysql.MySQLError{Number: 1213, Message: "serialization conflict"},
	}
	secondUOW := &proxiedRetryTestUOW{issueUC: secondUC}
	provider := &proxiedRetryTestProvider{uows: []uow.UnitOfWork{firstUOW, secondUOW}}
	uowProvider = provider

	out := captureStdout(t, func() error {
		return runReadyProxiedClaim(context.Background(), readyInput{claim: true})
	})
	if !strings.Contains(out, "ready-durable") || strings.Contains(out, "ready-failed") {
		t.Fatalf("output exposed failed attempt instead of durable retry: %q", out)
	}
	if provider.calls != 2 || firstUC.claimReadyCalls != 1 || secondUC.claimReadyCalls != 1 {
		t.Fatalf("fresh ready replay counts: provider=%d first=%d second=%d",
			provider.calls, firstUC.claimReadyCalls, secondUC.claimReadyCalls)
	}
	if firstUOW.closeCalls != 1 || secondUOW.closeCalls != 1 {
		t.Fatalf("UOW closes: first=%d second=%d, want 1/1", firstUOW.closeCalls, secondUOW.closeCalls)
	}
	wantMessages := []string{"bd: ready --claim ready-failed", "bd: ready --claim ready-durable"}
	for i, uw := range []*proxiedRetryTestUOW{firstUOW, secondUOW} {
		if uw.commitCalls != 1 || uw.commitMessage != wantMessages[i] {
			t.Errorf("UOW %d commit calls/message = %d/%q, want 1/%q", i, uw.commitCalls, uw.commitMessage, wantMessages[i])
		}
	}
}

func TestRunHeartbeatProxiedServerRetriesWholeOperationOnFreshUOW(t *testing.T) {
	t.Chdir(t.TempDir())
	oldProvider, oldActor, oldJSON := uowProvider, actor, jsonOutput
	t.Cleanup(func() { uowProvider, actor, jsonOutput = oldProvider, oldActor, oldJSON })
	actor = "heartbeat-retry-agent"
	jsonOutput = false

	firstUC := &proxiedRetryTestIssueUC{current: &types.Issue{
		ID: "heartbeat-failed", Title: "failed attempt", Status: types.StatusInProgress, Assignee: actor, IssueType: types.TypeTask,
	}}
	secondUC := &proxiedRetryTestIssueUC{current: &types.Issue{
		ID: "heartbeat-durable", Title: "durable attempt", Status: types.StatusInProgress, Assignee: actor, IssueType: types.TypeTask,
	}}
	firstUOW := &proxiedRetryTestUOW{
		issueUC:   firstUC,
		commitErr: &mysql.MySQLError{Number: 1213, Message: "serialization conflict"},
	}
	secondUOW := &proxiedRetryTestUOW{issueUC: secondUC}
	provider := &proxiedRetryTestProvider{uows: []uow.UnitOfWork{firstUOW, secondUOW}}
	uowProvider = provider

	out := captureStdout(t, func() error {
		return runHeartbeatProxiedServer(context.Background(), "heartbeat-alias")
	})
	if !strings.Contains(out, "heartbeat-durable") || strings.Contains(out, "heartbeat-failed") {
		t.Fatalf("output exposed failed attempt instead of durable retry: %q", out)
	}
	if provider.calls != 2 || firstUC.getCalls != 1 || secondUC.getCalls != 1 ||
		firstUC.heartbeatCalls != 1 || secondUC.heartbeatCalls != 1 {
		t.Fatalf("fresh heartbeat replay counts: provider=%d first(get=%d heartbeat=%d) second(get=%d heartbeat=%d)",
			provider.calls, firstUC.getCalls, firstUC.heartbeatCalls, secondUC.getCalls, secondUC.heartbeatCalls)
	}
	if firstUOW.closeCalls != 1 || secondUOW.closeCalls != 1 {
		t.Fatalf("UOW closes: first=%d second=%d, want 1/1", firstUOW.closeCalls, secondUOW.closeCalls)
	}
	for i, uw := range []*proxiedRetryTestUOW{firstUOW, secondUOW} {
		if uw.commitCalls != 1 || uw.commitMessage != "bd: heartbeat heartbeat-alias" {
			t.Errorf("UOW %d commit calls/message = %d/%q", i, uw.commitCalls, uw.commitMessage)
		}
	}
}
