package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const (
	PING = "PING"
	ECHO = "ECHO"
	SET  = "SET"
	GET  = "GET"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	kvStore := store.New()

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		go handleClient(conn, kvStore)
	}

}

func handleClient(conn net.Conn, kvStore *store.Store) {
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

		var response []byte

		switch strings.ToUpper(commands[0]) {
		case PING:
			response = parser.SerializeSimpleString("PONG")
		case ECHO:
			if len(commands) != 2 {
				response = parser.SerializeSimpleError("ERR wrong number of arguments for 'echo' command")
			} else {
				response = parser.SerializeBulkString(commands[1])
			}
		case SET:
			if len(commands) != 3 {
				response = parser.SerializeSimpleError("ERR wrong number of arguments for 'set' command")
			} else {
				kvStore.Set(commands[1], commands[2])
				response = parser.SerializeSimpleString("OK")

			}
		case GET:
			if len(commands) != 2 {
				response = parser.SerializeSimpleError("ERR wrong number of arguments for 'get' command")
			} else {
				value := kvStore.Get(commands[1])
				response = parser.SerializeBulkString(value)
			}
		default:
			response = parser.SerializeSimpleError(fmt.Sprintf("ERR Unknown command '%s'", commands[0]))
		}

		conn.Write(response)

	}
}
