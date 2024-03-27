package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/replication"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func main() {

	serverConfig := config.New()

	fmt.Printf("Server starting as %s on port %s\n", serverConfig.Role, serverConfig.Port)

	kvStore := store.New()

	rdbFile := rdb.New(serverConfig)
	rdbFile.Inject(kvStore)

	l, err := net.Listen("tcp", "0.0.0.0:"+serverConfig.Port)
	if err != nil {
		fmt.Println("Failed to bind to port" + serverConfig.Port + ": " + err.Error())
		os.Exit(1)
	}
	defer l.Close()

	// handle replication stuff
	if serverConfig.Role == config.RoleSlave {
		go replication.ConnectToMaster(serverConfig, kvStore)
	} else {
		go replication.HandleReplicaWrite(serverConfig)
	}

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		go handleClient(conn, kvStore, serverConfig)
	}

}

func handleClient(conn net.Conn, kvStore *store.Store, serverConfig *config.ServerConfig) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		message, err := parser.Deserialize(reader)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error parsing commands: ", err.Error())
			}
			fmt.Println("Connection closed")
			break
		}

		fmt.Println("Commands: ", message.Commands, conn.RemoteAddr().String())

		if len(message.Commands) == 0 {
			continue
		}

		response := command.Handler(message.Commands, conn, kvStore, serverConfig)

		conn.Write(response)

	}
}
