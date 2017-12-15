package main

import (
	"bytes"
	"encoding/json"
	"errors"
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

//DBManager implements simple CRUD functions to manage databases
type DBManager struct {
	databases sync.Map
	paired    sync.Map
}

type databaseMeta struct {
	DatabaseName string        `json:"DB_Name"`
	Tables       []*data.Table `json:"Tables"`
}

func (DBM *DBManager) getMetadata() (databaseMetaArray []databaseMeta) {
	DBM.databases.Range(func(ki, vi interface{}) bool {
		k, v := ki.(string), vi.(*data.Database)
		databaseMetaArray = append(databaseMetaArray, databaseMeta{DatabaseName: k, Tables: v.AllTables()})
		return true
	})
	return
}

//LoadAllDatabases loads all the existing databses files into memory
func (DBM *DBManager) loadAllDatabases(path string) (err error) {
	databasesFiles, err := listDatabases(path)
	if err != nil {
		return err
	}
	for _, databaseFile := range databasesFiles {
		db, err := data.LoadDatabase(filepath.Join(path, databaseFile.Name()))
		if err != nil {
			return err
		}
		DBM.databases.Store(databaseFile.Name(), db)
	}
	return nil
}

//CreateDatabase creates a new databse and pairs it to the session
func (DBM *DBManager) createDatabase(sessionID int64, name string, path string, blocksize int64) (err error) {
	db, err := data.NewDatabase(filepath.Join(path, name), blocksize)
	if err != nil {
		return err
	}
	DBM.databases.Store(name, db)
	return DBM.pair(sessionID, name)
}

//Pair pairs a session with a loaded database
func (DBM *DBManager) pair(sessionID int64, name string) (err error) {
	databasePointer, ok := DBM.databases.Load(name)
	if !ok {
		return errors.New("Error pairing, Database isn't loaded or doesnt exist")
	}
	DBM.paired.Store(sessionID, databasePointer)
	return nil
}

//Unpair deletes the relation between a session and a database
func (DBM *DBManager) unpair(sessionID int64) (err error) {
	_, ok := DBM.paired.Load(sessionID)
	if ok {
		DBM.paired.Delete(sessionID)
		return nil
	}
	return errors.New("Database specified wasn't found")
}

//GetPair gets the linked db pointer that was paired with id
func (DBM *DBManager) getPair(sessionID int64) (*data.Database, error) {
	dbpointer, ok := DBM.paired.Load(sessionID)
	if ok {
		return dbpointer.(*data.Database), nil
	}
	return nil, errors.New("No active database selected")
}

func listDatabases(path string) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(path)
	return files, err
}

func deleteDatabase(name string, path string) error {
	err := os.Remove(filepath.Join(path, name))
	return err
}

type config struct {
	Host        string
	Port        string
	Root        string
	MaxSessions int
	BlockSize   int64
}

var dbmanager DBManager
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
		err := dbmanager.createDatabase(request.SessionID, request.Response.Data, settings.Root, settings.BlockSize)
		if err != nil {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
			return
		}
	case network.LoadDatabase:
		err := dbmanager.pair(request.SessionID, request.Response.Data)
		if err != nil {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
			return
		}
	case network.NewTable:
	case network.FindTable:
	case network.GetMetadata:
		databaseMetaArray := dbmanager.getMetadata()
		databaseMetaArrayJSON, err := json.Marshal(databaseMetaArray)
		if err != nil {
			fmt.Println("Error encoding metadata:", err)
		}
		server.Send(request.SessionID, network.Response{Type: network.GetMetadata, Data: "{Databases:" + string(databaseMetaArrayJSON) + "}"})
	case network.Query:
		databaseTemp, err := dbmanager.getPair(request.SessionID)
		if err != nil {
			server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
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
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Table Created"})
				}
			case *common.DeleteCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Data Deleted"})
				}
			case *common.InsertCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Data Inserted"})
				}
			case *common.UpdateTableCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Data Updated"})
				}
			case *common.SelectTableCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}

					resultJSON, _ := json.Marshal(result)
					server.Send(request.SessionID, network.Response{Type: network.Query, Data: string(resultJSON)})
				}
			case *common.DropCommand:
				function = func(result interface{}, err error) {
					if err != nil {
						server.Send(request.SessionID, network.Response{Type: network.Error, Data: err.Error()})
						return
					}
					server.Send(request.SessionID, network.Response{Type: network.Notification, Data: "Table Dropped"})
				}
			}
			commandsArray = append(commandsArray, databaseTemp.CommandFactory(command, function))

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
		err := dbmanager.unpair(request.SessionID)
		if err != nil {
			fmt.Println(err)
			return
		}
	case network.DropDb:
		err := deleteDatabase(request.Response.Data, settings.Root)
		if err != nil {
			fmt.Print(err)
			return
		}
	}

}

func init() {
	go transaction.StartTransactionManager()
}

func main() {
	fmt.Println("Loading Databases")
	err := dbmanager.loadAllDatabases(settings.Root)
	if err != nil {
		fmt.Println("Error loading databses. Exiting", err)
		return
	}

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
