package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"math/rand"
	"time"
)

const (
	host       = "database-2.cc8jabfxdsf0.ap-northeast-2.rds.amazonaws.com"
	port       = "5432"
	user       = "postgres"
	pass       = "12345678"
	postgresdb = "bg"
	table      = "distribution"

	//host       = "localhost"
	//port       = "5432"
	//user       = "postgres"
	//pass       = "0000"
	//postgresdb = "test"
	//table      = "distribution"
)

// заполняет таблицу
func genTableEntry(numberOfEntry int, conn *pgx.Conn) {

	for i := 0; i <= numberOfEntry; i++ {
		var randWasted bool
		rand.Seed(time.Now().UnixNano())
		randIndexWasted := rand.Intn(2)
		switch randIndexWasted {
		case 1:
			randWasted = true
		case 2:
			randWasted = false
		}
		var randReqID = rand.Intn(100)
		conn.Exec(context.Background(), "INSERT INTO "+table+" (request_id, wasted) values ($1,$2)", randReqID, randWasted)
	}
}

type dataSource struct {
	host string
	port string
	pass string
	user string
	db   string
}

type data struct {
	id         uint32
	uuid       string
	created_at time.Time
	updated_at time.Time
	request_id uint32
	wasted     bool
}

func db(dS dataSource) (*pgx.Conn, error) {
	dataSource := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", dS.user, dS.pass, dS.host, dS.port, dS.db)
	conn, err := pgx.Connect(context.Background(), dataSource)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func main() {

	dS := dataSource{
		host: host,
		port: port,
		pass: pass,
		user: user,
		db:   postgresdb,
	}

	// подключение к бд
	conn, err := db(dS)
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	// заполняет заблицу
	genTableEntry(20, conn)

	// запрос на выборку
	rows, err := conn.Query(context.Background(), "select id, uuid, created_at, updated_at, request_id, wasted"+
		" from distribution "+
		" where wasted=false "+
		" and created_at <= NOW() - INTERVAL '5 minutes'")
	if err != nil {
		panic(err)
	}

	// запись данных в слайс
	datas := []data{}
	for rows.Next() {
		var d data
		_ = rows.Scan(&d.id, &d.uuid, &d.created_at, &d.updated_at, &d.request_id, &d.wasted)
		datas = append(datas, d)
	}

	if len(datas) != 0 {
		for i := 0; i < len(datas); i++ {
			fmt.Println(datas[i])
		}
	} else {
		fmt.Println("Нет строк удовлетворяющих запросу")
	}
}
