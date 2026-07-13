package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dbName := "beads_perf_lab_cell_0"
	if len(os.Args) > 1 {
		dbName = os.Args[1]
	}
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13360)/"+dbName)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT `key`, `value` FROM metadata")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var k, v string
		_ = rows.Scan(&k, &v)
		fmt.Printf("%s=%s\n", k, v)
	}
}
