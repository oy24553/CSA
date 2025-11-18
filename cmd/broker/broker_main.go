package main

import (
	"log"
	"net"
	"net/rpc"
	"os"

	"uk.ac.bris.cs/gameoflife/gol"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8040"
	}

	broker := new(gol.Broker)
	if err := rpc.Register(broker); err != nil {
		log.Fatalf("Failed to register Broker: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", port, err)
	}

	log.Printf("[Broker] Listening on :%v ...", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Broker] Accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}