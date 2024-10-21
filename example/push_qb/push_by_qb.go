package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sqlbuilder"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	MAXALLOWEDPACKET uint64 = 16777216
)

func main() {
	fd, errF := os.Open("../dummy.csv")

	if errF != nil {
		panic(errF)
	}

	cr := csv.NewReader(fd)
	if _, err := cr.Read(); err != nil {
		panic(err)
	}

	records, errCr := cr.ReadAll()
	if errCr != nil {
		panic(errCr)
	}
	q := "dummy_2"
	queries, errQ := sqlbuilder.QueriesBuild(records, q, MAXALLOWEDPACKET)

	if errQ != nil {
		panic(errQ)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, errDb := sql.Open("mysql", "root:admin@tcp(127.0.0.1:3307)/test")
	if errDb != nil {
		panic(errDb)
	}
	defer db.Close()

	conn, errConn := db.Conn(ctx)
	if errConn != nil {
		panic(errConn)
	}
	defer conn.Close()

	//truncate dummy_2
	if _, err := conn.ExecContext(ctx, "TRUNCATE dummy_2"); err != nil {
		panic(err)
	}

	// start time
	start := time.Now()
	for _, q := range queries {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			log.Fatalf("%v\nlen: %d, want: %d\n", err, len(q), MAXALLOWEDPACKET)
		}
	}
	end := time.Now()
	fmt.Printf("elapsed time: %v\n", end.Sub(start))
}
