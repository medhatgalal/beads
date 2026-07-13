package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

func main() {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13360)/"+os.Args[1])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("REPLACE INTO metadata (`key`,`value`) VALUES (?,?)", os.Args[2], os.Args[3])
	if err != nil {
		panic(err)
	}
	fmt.Println("set", os.Args[2], os.Args[3])
}
