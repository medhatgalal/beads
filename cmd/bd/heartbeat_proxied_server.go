package main

import (
	"context"
	"fmt"

	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/ui"
)

// runHeartbeatProxiedServer keeps the issue read and lease mutation inside the
// fresh-UOW retry callback. A failed Dolt commit therefore cannot expose output
// from a losing snapshot or retry the already-aborted transaction.
func runHeartbeatProxiedServer(ctx context.Context, id string) error {
	if uowProvider == nil {
		return HandleError("proxied-server UOW provider not initialized")
	}

	var durableIssue *types.Issue
	err := uow.RunWithFreshUOWRetries(ctx, uowProvider, fmt.Sprintf("bd: heartbeat %s", id), func(ctx context.Context, uw uow.UnitOfWork) error {
		durableIssue = nil
		issue, _, getErr := proxiedGetIssueOrWisp(ctx, uw, id)
		if getErr != nil {
			return getErr
		}
		if issue == nil {
			return fmt.Errorf("issue %s not found", id)
		}
		if heartbeatErr := uw.IssueUseCase().HeartbeatIssue(ctx, issue.ID, actor); heartbeatErr != nil {
			return heartbeatErr
		}
		durableIssue = issue
		return nil
	})
	if err != nil {
		return HandleErrorRespectJSON("heartbeat %s: %v", id, err)
	}

	SetLastTouchedID(durableIssue.ID)
	if jsonOutput {
		return outputJSON(map[string]string{
			"id":     durableIssue.ID,
			"status": "heartbeat",
			"owner":  actor,
		})
	}
	fmt.Printf("%s Heartbeat %s (lease refreshed)\n", ui.RenderPass("✓"), formatFeedbackID(durableIssue.ID, durableIssue.Title))
	return nil
}
