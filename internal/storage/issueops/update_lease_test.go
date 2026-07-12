package issueops

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func TestManageLeaseAndRowLockOnUpdate(t *testing.T) {
	now := time.Now().UTC()
	live := &types.Issue{
		Status:         types.StatusInProgress,
		Assignee:       "alice",
		LeaseExpiresAt: &now,
		HeartbeatAt:    &now,
	}
	tests := []struct {
		name      string
		oldIssue  *types.Issue
		updates   map[string]interface{}
		wantClear bool
		wantLease bool
	}{
		{name: "unrelated edit preserves lease", updates: map[string]interface{}{"title": "edited"}},
		{name: "same live claim refreshes lease", oldIssue: live, updates: map[string]interface{}{"assignee": "alice"}, wantLease: true},
		{name: "close clears lease", oldIssue: live, updates: map[string]interface{}{"status": string(types.StatusClosed)}, wantClear: true},
		{name: "unassign clears lease", oldIssue: live, updates: map[string]interface{}{"assignee": ""}, wantClear: true},
		{name: "transfer stamps fresh lease", oldIssue: live, updates: map[string]interface{}{"assignee": "bob"}, wantLease: true},
		{name: "generic claim state stamps lease", oldIssue: &types.Issue{Status: types.StatusOpen}, updates: map[string]interface{}{
			"status": string(types.StatusInProgress), "assignee": "alice",
		}, wantLease: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clauses, args := ManageLeaseAndRowLockOnUpdate(tt.oldIssue, tt.updates, nil, nil, context.Background())
			joined := strings.Join(clauses, ",")
			gotClear := strings.Contains(joined, "lease_expires_at = NULL") && strings.Contains(joined, "heartbeat_at = NULL")
			if gotClear != tt.wantClear {
				t.Fatalf("lease clear=%v, want %v; clauses=%v", gotClear, tt.wantClear, clauses)
			}
			if !strings.Contains(joined, "row_lock = ?") {
				t.Fatalf("row_lock rewrite missing: %v", clauses)
			}
			wantArgs := 1
			if tt.wantLease {
				wantArgs = 3
			}
			if len(args) != wantArgs || args[len(args)-1].(int64) == 0 {
				t.Fatalf("args=%v, want %d args ending in nonzero row_lock", args, wantArgs)
			}
			gotLease := strings.Contains(joined, "lease_expires_at = ?") && strings.Contains(joined, "heartbeat_at = ?")
			if gotLease != tt.wantLease {
				t.Fatalf("fresh lease=%v, want %v; clauses=%v", gotLease, tt.wantLease, clauses)
			}
		})
	}
}
