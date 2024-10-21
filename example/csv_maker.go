package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
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

	rows, errRows := conn.QueryContext(ctx, "select id, product, IFNULL(description, 'null') as description, price,qty,date from dummy")

	if errRows != nil {
		panic(errRows)
	}

	cw := csv.NewWriter(os.Stdout)
	if err := cw.Write([]string{"id", "product", "description", "price", "qty", "date"}); err != nil {
		panic(err)
	}
	for rows.Next() {
		p := struct {
			ID          int64
			Product     string
			Description string
			Price       float64
			Quantity    int64
			Date        string
		}{}

		if err := rows.Scan(&p.ID, &p.Product, &p.Description, &p.Price, &p.Quantity, &p.Date); err != nil {
			panic(err)
		}
		if err := cw.Write([]string{fmt.Sprintf("%d", p.ID), p.Product, p.Description, fmt.Sprintf("%.2f", p.Price), fmt.Sprintf("%d", p.Quantity), p.Date}); err != nil {
			panic(err)
		}
	}

	if err := rows.Err(); err != nil {
		panic(err)
	}
	cw.Flush()

}
