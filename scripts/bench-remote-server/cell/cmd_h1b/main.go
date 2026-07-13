package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage/uow"
)

func main() {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13360)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		fmt.Fprintf(os.Stderr, "SHOW DATABASES: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("databases:")
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		fmt.Println(" ", n)
	}
	_ = rows.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("beads_perf_lab_cell_%d", i)
		p, err := uow.NewDirectDoltServerUOWProvider(ctx, uow.DirectDoltServerOptions{
			Host: "127.0.0.1", Port: 13360, Database: name, User: "root", AuthSecret: "",
			MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Hour, ConnMaxIdleTime: 5 * time.Minute,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "provider %s: %v\n", name, err)
			os.Exit(2)
		}
		uw, err := p.NewUOW(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewUOW %s: %v\n", name, err)
			_ = p.Close(ctx)
			os.Exit(3)
		}
		raw := uw.RawSQLUseCase()
		res, err := raw.Query(ctx, "SELECT DATABASE()")
		if err != nil {
			fmt.Fprintf(os.Stderr, "query %s: %v\n", name, err)
			uw.Close(ctx)
			_ = p.Close(ctx)
			os.Exit(4)
		}
		fmt.Printf("uow_ok db=%s result=%v\n", name, res.Rows)
		uw.Close(ctx)
		_ = p.Close(ctx)
	}
	fmt.Println("H1B_DIRECT_DOLT_UOW_PASS")
}
