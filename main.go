package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/modest-sql/data"
	"github.com/modest-sql/network"
	"github.com/modest-sql/parser"
	"github.com/modest-sql/transaction"
)

type config struct {
	DatabaseHost string
	DatabasePort string
	DatabaseRoot string
}

func loadConfig(path string) (c config) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	json.Unmarshal(raw, &c)
	return
}

func handleRequest(server *network.Server, request network.Request) {
	switch request.Response.Type {
	case network.KeepAlive:
		server.Send(request.SessionID, network.Response{Type: network.KeepAlive, Data: "Alive"})
	case network.NewDatabase:

	case network.LoadDatabase:

		databaseName = request.Response.Data
		database, err := data.LoadDatabase(databaseName)
		if err != nil {
			fmt.Print(err)
			return
		}
		go transaction.StartTransactionManager(database)

	case network.NewTable:
	case network.FindTable:
	case network.GetMetadata:
		type Database struct {
			DbNAme string        `json:"DB_Name"`
			Tables []*data.Table `json:"Tables"`
		}

		tables, err := database.AllTables()
		if err != nil {
			fmt.Println(err)
			return
		}
		datab := Database{databaseName, tables}
		c, err := json.Marshal(datab)
		if err != nil {
			fmt.Println("Error encoding metadata:", err)
		}
		server.Send(request.SessionID, network.Response{Type: network.GetMetadata, Data: string(c)})

	case network.Query:
		reader := bytes.NewReader([]byte(request.Response.Data))
		commands, err := parser.Parse(reader)

		transaction.AddTransactionToManager(transaction.NewTransaction(commands))

		if err != nil {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: "[{\"Error\":\"" + err.Error() + "\"}]"})
			return
		}

	case network.ShowTransaction:
	}

}

func handleIncoming(server *network.Server) {
	go func() {
		for {
			select {
			case IncomingRequest := <-server.RequestQueue:
				go handleRequest(server, IncomingRequest)
				//case TRChannel := <-transaction.TRChannel:
				//do something with the transaction result
			}
		}
	}()
}

var database *data.Database
var databaseName string

func main() {
	fmt.Println("Starting server")
	server := network.NewServer()
	handleIncoming(server)
	listener, err := net.Listen("tcp", ":3333")
	if err != nil {
		fmt.Println("Server Listener failed. Exiting.", err)
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection accepting failed.")
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fmt.Println("A new connection accepted.")
		server.Join(conn)
	}
}
