package replication

import (
	"bufio"
	"bytes"
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
	writeAndExpect(conn, parser.SerializeArray([]string{command.PING}), command.PONG)

	// Send REPLCONF command to master twice
	writeAndExpect(conn, parser.SerializeArray([]string{command.REPLCONF, "listening-port", config.Port}), command.OK)
	writeAndExpect(conn, parser.SerializeArray([]string{command.REPLCONF, "capa", "psync2"}), command.OK)

	// Send PSYNC command to master
	conn.Write(parser.SerializeArray([]string{command.PSYNC, "?", "-1"}))

	fmt.Println("Connected to master?")

	// Read from master
	reader := bufio.NewReader(conn)
	for {
		message, err := parser.Deserialize(reader)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error parsing commands recieved from master: ", err.Error())
			}
			fmt.Println("Connection to master closed")
			break
		}

		fmt.Println("Commands from master: ", message.Commands)

		if len(message.Commands) == 0 {
			continue
		}

		var response []byte
		leadCommand := strings.ToUpper(message.Commands[0])

		if leadCommand == command.FULLRESYNC {
			fmt.Println("Expecting RDB file")
			err := parser.ExpectRDBFile(reader)
			if err != nil {
				fmt.Println("Error expecting RDB file: ", err.Error())
				break
			}
			config.HandeshakeCompletedWithMaster = true
			fmt.Println("RDB file received")
			continue
		}

		response = command.Handler(message.Commands, conn, kvStore, config)

		if leadCommand == command.REPLCONF {
			conn.Write(response)
		}

		// update offset
		if config.HandeshakeCompletedWithMaster {
			fmt.Println("Updating offset", config.MasterReplOffset, message.ReadBytes, message.Commands)
			config.MasterReplOffset += message.ReadBytes
		}

	}
}

func HandleReplicaWrite(cfg *config.ServerConfig) {
	var wg sync.WaitGroup
	for cmds := range cfg.ReplicaWriteQueue {

		setCommands := parser.SerializeArray(cmds)
		m, err := parser.Deserialize(bufio.NewReader(bytes.NewReader(setCommands)))

		if err != nil {
			panic("Can't deserialize our own commands???")
		}

		for i, replica := range cfg.Replicas {
			wg.Add(1)
			go func(replica *config.Replica, cmds []string, i int) {
				defer wg.Done()
				replica.ConnAddr.Write(setCommands)
				replica.ExpectedOffset += m.ReadBytes
			}(replica, cmds, i)
		}

		wg.Wait()
	}
}

func writeAndExpect(conn net.Conn, cmd []byte, expect string) (err error) {
	conn.Write(cmd)
	reader := bufio.NewReader(conn)
	message, err := parser.Deserialize(reader)
	if err != nil {
		return err
	}

	if len(message.Commands) == 0 {
		return errors.New("no commands received")
	}

	if strings.ToUpper(message.Commands[0]) != expect {
		return errors.New("Expected " + expect + " but got " + message.Commands[0])
	}

	return nil
}
