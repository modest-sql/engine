package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/modest-sql/network"
)

func main() {
	fmt.Println("Server started.")
	port := "3333"

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Server listening failed. Exit.")
		os.Exit(1)
	}
	fmt.Println("Server started to listen on " + port)

	server := network.Init()
	// listen
	server.Listen()
	fmt.Println("Server started to listen.")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection accepting failed.")
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fmt.Println("A new connection accepted.")
		server.Entrance <- conn
	}

}
