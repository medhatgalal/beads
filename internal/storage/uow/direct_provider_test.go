package uow

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "modernc.org/sqlite"
)

func TestValidateDirectDoltServerOptions(t *testing.T) {
	valid := DirectDoltServerOptions{
		Host:            "127.0.0.1",
		Port:            3306,
		Database:        "beads_perf_lab_gateway",
		User:            "beads_lab",
		MaxOpenConns:    8,
		MaxIdleConns:    8,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 20 * time.Second,
	}
	tests := []struct {
		name string
		edit func(*DirectDoltServerOptions)
		want string
	}{
		{name: "host", edit: func(o *DirectDoltServerOptions) { o.Host = "" }, want: "host"},
		{name: "hostname", edit: func(o *DirectDoltServerOptions) { o.Host = "localhost" }, want: "numeric loopback"},
		{name: "external host", edit: func(o *DirectDoltServerOptions) { o.Host = "192.0.2.1" }, want: "numeric loopback"},
		{name: "port low", edit: func(o *DirectDoltServerOptions) { o.Port = 0 }, want: "port"},
		{name: "port high", edit: func(o *DirectDoltServerOptions) { o.Port = 65536 }, want: "port"},
		{name: "database", edit: func(o *DirectDoltServerOptions) { o.Database = "" }, want: "database"},
		{name: "user", edit: func(o *DirectDoltServerOptions) { o.User = "" }, want: "user"},
		{name: "max open", edit: func(o *DirectDoltServerOptions) { o.MaxOpenConns = 0 }, want: "max open"},
		{name: "max idle", edit: func(o *DirectDoltServerOptions) { o.MaxIdleConns = 9 }, want: "max idle"},
		{name: "negative lifetime", edit: func(o *DirectDoltServerOptions) { o.ConnMaxLifetime = -1 }, want: "lifetimes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid
			tt.edit(&got)
			err := validateDirectDoltServerOptions(got)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate error = %v, want substring %q", err, tt.want)
			}
		})
	}
	if err := validateDirectDoltServerOptions(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}
}

func TestDirectProviderSQLMetrics(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider := &DirectDoltServerProvider{
		inner:   &doltSQLProvider{defaultBranch: defaultBranch, db: db},
		maxOpen: 8,
	}

	mock.ExpectExec("START TRANSACTION").WillReturnResult(sqlmock.NewResult(0, 0))
	uw, err := provider.NewUOW(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("UPDATE metadata").WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := uw.RawSQLUseCase().Exec(ctx, "UPDATE metadata SET value='x'"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("CALL DOLT_COMMIT").WithArgs("metrics").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := uw.Commit(ctx, "metrics"); err != nil {
		t.Fatal(err)
	}
	uw.Close(ctx)

	mock.ExpectExec("START TRANSACTION").WillReturnResult(sqlmock.NewResult(0, 0))
	readUOW, err := provider.NewUOW(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
	readUOW.Close(ctx)

	metrics := provider.SQLMetrics()
	if metrics.StatementCount != 5 || metrics.ExecCount != 5 || metrics.TransactionCount != 2 ||
		metrics.CommitAttemptCount != 1 || metrics.CommitSuccessCount != 1 || metrics.RollbackCount != 1 {
		t.Fatalf("metrics = %#v", metrics)
	}
	if metrics.QueryCount != 0 || metrics.QueryRowCount != 0 {
		t.Fatalf("unexpected query metrics = %#v", metrics)
	}
	if metrics.CallDurationNS <= 0 || metrics.MaxCallDurationNS <= 0 || metrics.MaxCallDurationNS > metrics.CallDurationNS {
		t.Fatalf("invalid method-call durations = %#v", metrics)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDirectProviderConcurrentCloseAccess(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	provider := &DirectDoltServerProvider{
		inner:   &doltSQLProvider{defaultBranch: defaultBranch, db: db},
		maxOpen: 8,
	}

	start := make(chan struct{})
	closeErrors := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			uw, err := provider.NewUOW(ctx)
			if err == nil {
				uw.Close(ctx)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = provider.Stats()
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = provider.Prewarm(ctx, 1)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		closeErrors <- provider.Close(ctx)
	}()
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent provider shutdown deadlocked")
	}
	if err := <-closeErrors; err != nil {
		t.Fatalf("concurrent Close: %v", err)
	}

	if _, err := provider.NewUOW(ctx); err == nil {
		t.Fatal("NewUOW succeeded after Close")
	}
	if err := provider.Prewarm(ctx, 0); err == nil {
		t.Fatal("Prewarm succeeded after Close")
	}
	if got := provider.Stats(); got != (sql.DBStats{}) {
		t.Fatalf("Stats after Close = %#v", got)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
