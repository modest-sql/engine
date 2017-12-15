package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/modest-sql/common"

	"github.com/modest-sql/data"
	"github.com/modest-sql/network"
	"github.com/modest-sql/parser"
	"github.com/modest-sql/transaction"
)

type config struct {
	Host        string
	Port        string
	Root        string
	MaxSessions int
	BlockSize   int64
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
var settings = loadConfig("settings.json")

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
		db, err := data.NewDatabase(filepath.Join(settings.Root, name), settings.BlockSize)
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
		fmt.Println("getting metadata")
		databasesFiles, err := listDatabases(settings.Root)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("printing databse")
		for _, databaseFile := range databasesFiles {
			fmt.Println(databaseFile.Name())
		}

		pathS, err := os.Getwd()
		if err != nil {
			panic(err)
		}

		databaseMetaArray := make([]databaseMeta, 0)
		for _, databaseFile := range databasesFiles {
			fmt.Println("sending:", filepath.Join(pathS+"/databases/", databaseFile.Name()))
			db, err := data.LoadDatabase(filepath.Join(settings.Root, databaseFile.Name()))
			if err != nil {
				fmt.Println("Error loading database ", databaseFile.Name(), err)
				return
			}
			tables := db.AllTables()
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

					resultJSON, _ := json.Marshal(result)
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
	case network.DropDb:
		name := request.Response.Data
		err := deleteDatabase(name, settings.Port)
		if err != nil {
			fmt.Print(err)
			return
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

func deleteDatabase(name string, path string) error {
	err := os.Remove(path + name)
	return err
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

	listener, err := net.Listen("tcp", settings.Host+":"+settings.Port)
	if err != nil {
		fmt.Println("Server Listener failed. Exiting.", err)
		os.Exit(1)
	}

	for {
		if settings.MaxSessions > server.GetSessionsAmount() {
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

}
