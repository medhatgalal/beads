package uow

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"
)

func TestDoltSQLProviderBeginTxDiscardsConnectionOnStartFailure(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = database.Close() })
	startErr := &mysql.MySQLError{Number: 1213, Message: "start transaction conflict"}
	mock.ExpectExec(regexp.QuoteMeta("START TRANSACTION;")).WillReturnError(startErr)
	provider := &doltSQLProvider{defaultBranch: defaultBranch, db: database}
	if tx, err := provider.BeginTx(context.Background()); tx != nil || err == nil {
		t.Fatalf("BeginTx tx=%#v err=%v", tx, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := database.Stats()
		if stats.InUse == 0 && stats.OpenConnections == 0 {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("failed START TRANSACTION leaked or pooled its pinned connection: %#v", database.Stats())
}
