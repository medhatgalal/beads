package issueops

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/steveyegge/beads/internal/storage"
)

func TestHeartbeatIssueInTxRejectsWispWithoutMutation(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)

	const id = "bd-heartbeat-wisp"
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM wisps WHERE id = ? LIMIT 1")).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))

	err = HeartbeatIssueInTx(context.Background(), database, id, "alice")
	require.ErrorIs(t, err, storage.ErrNotClaimable)
	mock.ExpectClose()
	require.NoError(t, database.Close())
	require.NoError(t, mock.ExpectationsWereMet(), "wisp heartbeat issued an unexpected mutation")
}
