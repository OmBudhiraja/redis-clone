package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const (
	PING = "PING"
	ECHO = "ECHO"
	SET  = "SET"
	GET  = "GET"
	PX   = "PX"
	INFO = "INFO"
)

type ServerConfig struct {
	role               string
	port               string
	masterHost         string
	masterPort         string
	master_replid      string
	master_repl_offset int
}

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

func connectToMaster(config ServerConfig) {
	if config.masterHost == "" || config.masterPort == "" {
		panic("Master host and port are required to connect to master")
	}

	conn, err := net.Dial("tcp", config.masterHost+":"+config.masterPort)

	if err != nil {
		panic("Error connecting to master: " + err.Error())
	}

	defer conn.Close()

	// Send PING to master
	conn.Write(parser.SerializeArray([]string{PING}))
}

func main() {

	port := flag.String("port", "6379", "Port to bind to")
	masterHost := flag.String("replicaof", "", "Host of master server")

	flag.Parse()
	masterPort := getMasterPort(masterHost)

	serverConfig := ServerConfig{
		role:               "master",
		port:               *port,
		masterHost:         *masterHost,
		masterPort:         masterPort,
		master_replid:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		master_repl_offset: 0,
	}

	if *masterHost == "" || masterPort == "" {
		fmt.Println("Starting as master")
	} else {
		serverConfig.role = "slave"
		connectToMaster(serverConfig)
		fmt.Println("Starting as replica of ", masterHost, ":", masterPort)
	}

	fmt.Println("Starting server on port: ", serverConfig.port)

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
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

		go handleClient(conn, kvStore, serverConfig)
	}

}

func handleClient(conn net.Conn, kvStore *store.Store, serverConfig ServerConfig) {
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
			if len(commands) != 3 && len(commands) != 5 {
				response = parser.SerializeSimpleError("ERR wrong number of arguments for 'set' command")
			} else {
				var expiry time.Time

				if len(commands) == 5 {
					if strings.ToUpper(commands[3]) != PX {
						response = parser.SerializeSimpleError("ERR syntax error")
						break
					}

					expiresIn, err := time.ParseDuration(commands[4] + "ms")

					if err != nil {
						response = parser.SerializeSimpleError("ERR invalid expire time in set")
						break
					}

					expiry = time.Now().Add(expiresIn)
				}

				kvStore.Set(commands[1], commands[2], expiry)
				response = parser.SerializeSimpleString("OK")

			}
		case GET:
			if len(commands) != 2 {
				response = parser.SerializeSimpleError("ERR wrong number of arguments for 'get' command")
			} else {
				value := kvStore.Get(commands[1])
				response = parser.SerializeBulkString(value)
			}
		case INFO:
			sb := strings.Builder{}
			sb.WriteString("# Replication \n")
			sb.WriteString("role:" + serverConfig.role + "\n")
			sb.WriteString("master_replid:" + serverConfig.master_replid + "\n")
			sb.WriteString(fmt.Sprintf("master_repl_offset:%d", serverConfig.master_repl_offset) + "\n")
			response = parser.SerializeBulkString(sb.String())
		default:
			response = parser.SerializeSimpleError(fmt.Sprintf("ERR Unknown command '%s'", commands[0]))
		}

		conn.Write(response)

	}
}
