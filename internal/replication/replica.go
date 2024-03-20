package replication

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/command"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func ConnectToMaster(config *config.ServerConfig, kvStore *store.Store) {

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

	fmt.Println("Connected to master?")

	// Read from master
	reader := bufio.NewReader(conn)
	for {
		commands, err := parser.Deserialize(reader)

		if err != nil {

			if !errors.Is(err, io.EOF) {
				fmt.Println("Error parsing commands recieved from master: ", err.Error())
			}
			fmt.Println("Connection to master closed")
			break
		}

		fmt.Println("Commands from master: ", commands)

		if len(commands) == 0 {
			continue
		}

		if strings.ToUpper(commands[0]) == command.FULLRESYNC {
			fmt.Println("Expecting RDB file")
			err := parser.ExpectRDBFile(reader)
			if err != nil {
				fmt.Println("Error expecting RDB file: ", err.Error())
				break
			}
			fmt.Println("RDB file received")
		}

		if strings.ToUpper(commands[0]) == command.SET {
			command.Handler(commands, conn, kvStore, config)
		}

	}
}

func HandleReplicaWrite(cfg *config.ServerConfig) {
	var wg sync.WaitGroup
	for cmds := range cfg.ReplicaWriteQueue {
		for i, replica := range cfg.Replicas {
			wg.Add(1)
			go func(replica config.Replica, cmds []string, i int) {
				defer wg.Done()
				replica.ConnAddr.Write(parser.SerializeArray(cmds))
			}(replica, cmds, i)
		}

		wg.Wait()
	}
}
