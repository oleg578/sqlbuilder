package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func saveData(d []string, pool *sql.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := pool.Conn(context.Background())
	if err != nil {
		return err
	}
	defer c.Close()
	_ , errS := c.ExecContext(ctx, "INSERT INTO `dummy_2` VALUES(?,?,?,?,?,?)", d[0],d[1],d[2],d[3],d[4],d[5])
	return errS
}

func worker(data <-chan []string, results chan<- error, pool *sql.DB) {
	for p := range data {
		results <- saveData(p, pool)
	}
}

func main() {
	fd, errF := os.Open("../dummy_s.csv")

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
	//multithread
	prodch := make(chan []string, len(records))
	results := make(chan error, len(records))
	//start workers
	for w := 1; w <= runtime.NumCPU(); w++ {
		go worker(prodch, results, db)
	}
	
	for _, p := range records {
		prodch <- p
	}
	close(prodch)
	//collect result
	for range records {
		err := <-results
		if err != nil {
			log.Println(err)
		}
	}
	end := time.Now()
	fmt.Printf("elapsed time: %v\n", end.Sub(start))
}
