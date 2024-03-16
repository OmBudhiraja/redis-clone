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
)

const (
	PING = "PING"
	ECHO = "ECHO"
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

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		go handleClient(conn)
	}

}

func handleClient(conn net.Conn) {
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

		fmt.Println("Commands: ", commands, err)

		if err != nil {
			fmt.Println("Error parsing commands: ", err.Error())
			continue
		}

		if len(commands) == 0 {
			continue
		}

		switch strings.ToUpper(commands[0]) {
		case PING:
			conn.Write(parser.SerializeSimpleString("PONG"))
		case ECHO:
			if len(commands) > 1 {
				conn.Write(parser.SerializeBulkString(commands[1]))
			}
		default:
			conn.Write(parser.SerializeBulkString("Unknown command"))
		}

	}
}
