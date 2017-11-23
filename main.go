package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"

	"github.com/modest-sql/common"

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

type databaseMeta struct {
	DatabaseName string        `json:"DB_Name"`
	Tables       []*data.Table `json:"Tables"`
}

type database struct {
	databasePointer *data.Database
	databaseName    string
}

var databases sync.Map

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
		name := request.Response.Data
		db, err := data.NewDatabase(name)
		if err != nil {
			fmt.Print(err)
			return
		}
		databases.Store(request.SessionID, database{databasePointer: db, databaseName: name})
	case network.LoadDatabase:
		name := request.Response.Data
		db, err := data.LoadDatabase(name)
		if err != nil {
			fmt.Print(err)
			return
		}
		databases.Store(request.SessionID, database{databasePointer: db, databaseName: name})
	case network.NewTable:
	case network.FindTable:
	case network.GetMetadata:

		databasesFiles, err := listDatabases("./databases/")
		if err != nil {
			fmt.Println(err)
			return
		}

		databaseMetaArray := make([]databaseMeta, 0)
		for _, databaseFile := range databasesFiles {

			db, err := data.LoadDatabase(databaseFile.Name())
			if err != nil {
				fmt.Println("Error loading database ", databaseFile.Name(), err)
				return
			}
			tables, err := db.AllTables()
			if err != nil {
				fmt.Println("Error geting tables", databaseFile.Name(), err)
				return
			}
			databaseMetaArray = append(databaseMetaArray, databaseMeta{DatabaseName: databaseFile.Name(), Tables: tables})
		}

		databaseMetaArrayJSON, err := json.Marshal(databaseMetaArray)
		if err != nil {
			fmt.Println("Error encoding metadata:", err)
		}
		server.Send(request.SessionID, network.Response{Type: network.GetMetadata, Data: "{Databases:" + string(databaseMetaArrayJSON) + "}"})

	case network.Query:
		databaseTemp, ok := databases.Load(request.SessionID)
		if !ok {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: "No active databse selected."})
			return
		}
		reader := bytes.NewReader([]byte(request.Response.Data))
		commands, err := parser.Parse(reader)
		if err != nil {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
			return
		}

		commandsArray := make([]common.Command, 0)

		for _, command := range commands {
			var function func(interface{}, error)
			switch command.(type) {
			case *common.AlterTableCommand:
			case *common.CreateTableCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					table := result.(*data.Table)
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Table Created " + table.TableName})
				}
			case *common.DeleteCommand:
			case *common.DropTableCommand:
			case *common.InsertCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Data Inserted"})
				}
			case *common.UpdateTableCommand:
			case *common.SelectTableCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					resultset := result.(*data.ResultSet)
					resultJSON, _ := json.Marshal(resultset.Rows)
					server.Send(request.SessionID, network.Response{Type: network.Query, Data: string(resultJSON)})
				}
			}
			commandsArray = append(commandsArray, databaseTemp.(database).databasePointer.CommandFactory(command, function))

		}

		transaction.AddCommands(commandsArray)

	case network.ShowTransaction:
		transactions := transaction.GetTransactions()
		transactionsJSON, err := json.Marshal(transactions)
		if err != nil {
			fmt.Println(err)
		}
		server.Send(request.SessionID, network.Response{Type: network.ShowTransaction, Data: "{Transactions:" + string(transactionsJSON) + "}"})
	case network.Error:
	case network.SessionExited:
		_, ok := databases.Load(request.SessionID)
		if ok {
			databases.Delete(request.SessionID)
		}
	}

}

func init() {
	go transaction.StartTransactionManager()
}

func listDatabases(path string) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(path)
	return files, err
}

func main() {
	fmt.Println("Starting server")
	server := network.NewServer()

	go func() {
		for {
			select {
			case IncomingRequest := <-server.RequestQueue:
				go handleRequest(server, IncomingRequest)
			}
		}
	}()

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
