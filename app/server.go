package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func getMasterPort(masterHost *string) string {
	if *masterHost == "" {
		return ""
	}

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == *masterHost {
			if i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		}
	}
	return ""
}

func connectToMaster(config *config.ServerConfig, kvStore *store.Store) {
	if config.MasterHost == "" || config.MasterPort == "" {
		panic("Master host and port are required to connect to master")
	}

	conn, err := net.Dial("tcp", config.MasterHost+":"+config.MasterPort)

	if err != nil {
		panic("Error connecting to master: " + err.Error())
	}

	defer conn.Close()

	// Send PING to master
	conn.Write(parser.SerializeArray([]string{command.PING}))

	// Send REPLCONF command to master twice
	conn.Write(parser.SerializeArray([]string{command.REPLCONF, "listening-port", config.Port}))
	conn.Write(parser.SerializeArray([]string{command.REPLCONF, "capa", "psync2"}))

	// Send PSYNC command to master
	conn.Write(parser.SerializeArray([]string{command.PSYNC, "?", "-1"}))

	fmt.Println("Connected to master")

	// Read from master
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Println("Error reading from master: ", err.Error())
			}

			break
		}

		commands, err := parser.Deserialize(buf[:n])

		if err != nil {
			fmt.Println("Error parsing commands recieved from master: ", err.Error())
			continue
		}

		if len(commands) == 0 {
			continue
		}

		if commands[0] == command.SET {
			command.Handler(commands, &conn, kvStore, config)
		}

	}

}

func main() {

	port := flag.String("port", "6379", "Port to bind to")
	masterHost := flag.String("replicaof", "", "Host of master server")

	flag.Parse()
	masterPort := getMasterPort(masterHost)

	serverConfig := config.New(*port, *masterHost, masterPort)

	if serverConfig.MasterHost == "" || serverConfig.MasterPort == "" {
	} else {
		serverConfig.Role = "slave"

	}

	fmt.Printf("Server starting as %s on port %s\n", serverConfig.Role, serverConfig.Port)

	kvStore := store.New()

	if serverConfig.Role == "slave" {
		go connectToMaster(serverConfig, kvStore)
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+serverConfig.Port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

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

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Println("Error reading from connection: ", err.Error())
			}

			break
		}

		commands, err := parser.Deserialize(buf[:n])

		if err != nil {
			fmt.Println("Error parsing commands: ", err.Error())
			continue
		}

		fmt.Println("Commands: ", commands)

		if len(commands) == 0 {
			continue
		}

		response := command.Handler(commands, &conn, kvStore, serverConfig)

		conn.Write(response)

	}
}
