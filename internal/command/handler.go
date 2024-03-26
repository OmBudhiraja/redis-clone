package command

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const (
	PING       = "PING"
	PONG       = "PONG"
	OK         = "OK"
	ECHO       = "ECHO"
	SET        = "SET"
	GET        = "GET"
	PX         = "PX"
	INFO       = "INFO"
	REPLCONF   = "REPLCONF"
	PSYNC      = "PSYNC"
	FULLRESYNC = "FULLRESYNC"
	ACK        = "ACK"
	GETACK     = "GETACK"
	WAIT       = "WAIT"
	CONFIG     = "CONFIG"
	KEYS       = "KEYS"
	TYPE       = "TYPE"
	XADD       = "XADD"
)

func Handler(cmds []string, conn net.Conn, kvStore *store.Store, cfg *config.ServerConfig) []byte {

	var response []byte

	switch strings.ToUpper(cmds[0]) {
	case GET:
		response = handleGetCommand(cmds, kvStore)
	case SET:
		response = handleSetCommand(cmds, kvStore, cfg)
	case PING:
		response = parser.SerializeSimpleString("PONG")
	case ECHO:
		if len(cmds) != 2 {
			response = parser.SerializeSimpleError("ERR wrong number of arguments for 'echo' command")
		} else {
			response = parser.SerializeBulkString(cmds[1])
		}
	case INFO:
		response = handleInfoCommand(cfg)
	case REPLCONF:
		response = handleRelpConfCommand(cmds, conn, cfg)
	case PSYNC:
		response = handlePsyncCommand(cfg, conn)
	case WAIT:
		response = handleWaitCommand(cmds, cfg)
	case XADD:
		response = handleXAddCommand(cmds, kvStore)
	case KEYS:
		response = handleKeysCommand(cmds, kvStore)
	case TYPE:
		response = handleTypeCommand(cmds, kvStore)
	case CONFIG:
		response = handleConfigCommand(cmds, cfg)
	default:
		response = parser.SerializeSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmds[0]))
	}

	return response
}

func handleGetCommand(cmds []string, kvStore *store.Store) []byte {
	if len(cmds) != 2 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'get' command")
	}

	value := kvStore.Get(cmds[1])
	return parser.SerializeBulkString(value)

}

func handleSetCommand(cmds []string, kvStore *store.Store, cfg *config.ServerConfig) []byte {
	if len(cmds) != 3 && len(cmds) != 5 {
		fmt.Println("Wrong args set", cmds)
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'set' command")
	}

	var expiry time.Time

	if len(cmds) == 5 {
		if strings.ToUpper(cmds[3]) != PX {
			return parser.SerializeSimpleError("ERR syntax error")
		}

		expiresIn, err := time.ParseDuration(cmds[4] + "ms")

		if err != nil {
			return parser.SerializeSimpleError("ERR invalid expire time in set")
		}

		expiry = time.Now().Add(expiresIn)
	}

	kvStore.Set(cmds[1], cmds[2], expiry)

	if cfg.Role == config.RoleMaster {
		cfg.ReplicaWriteQueue <- cmds
	}

	return parser.SerializeSimpleString("OK")

}

func handleWaitCommand(cmds []string, cfg *config.ServerConfig) []byte {
	if len(cmds) != 3 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'wait' command")
	}

	if cfg.Role == config.RoleSlave {
		return parser.SerializeSimpleError("ERR slaves can't be issued 'wait' command")
	}

	minNoOfReplicas, err := strconv.Atoi(cmds[1])

	if err != nil {
		return parser.SerializeSimpleError("ERR number of replicas is not a number")
	}

	timeout, err := strconv.Atoi(cmds[2]) // in milliseconds

	if err != nil {
		return parser.SerializeSimpleError("ERR timeout is not a number")
	}

	var ctx context.Context
	var ctxCancel context.CancelFunc

	if timeout == 0 {
		ctx = context.Background()
	} else {
		// create a context with timeout
		ctx, ctxCancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer ctxCancel()
	}

	var acksRecieved int
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	// wait for ack from all replicas
	for _, replica := range cfg.Replicas {
		getAckCommand := parser.SerializeArray([]string{REPLCONF, GETACK, "*"})
		go func(replica *config.Replica, ctx context.Context) {

			replica.ConnAddr.Write(getAckCommand)
		}(replica, ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return parser.SerializeInteger(acksRecieved)
		case <-ticker.C:
			acksRecieved = 0

			for _, replica := range cfg.Replicas {
				if replica.Offset >= replica.ExpectedOffset {
					replica.ExpectedOffset = replica.Offset
					acksRecieved++
				}
			}
			if acksRecieved >= minNoOfReplicas {
				return parser.SerializeInteger(acksRecieved)
			}

		}
	}

}

func handleXAddCommand(cmds []string, kvStore *store.Store) []byte {
	if len(cmds) < 3 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'xadd' command")
	}
	streamKey := cmds[1]
	entryId := cmds[2]

	pairs := cmds[3:]

	if len(pairs)%2 != 0 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'xadd' command")
	}

	id, err := kvStore.XAdd(streamKey, entryId, pairs)

	if err != nil {
		return parser.SerializeSimpleError(err.Error())

	}

	return parser.SerializeBulkString(id)
}

func handleKeysCommand(cmds []string, kvStore *store.Store) []byte {
	fmt.Println("keys command", cmds)

	if len(cmds) != 2 {
		return parser.SerializeSimpleError("Err wrong number of arguments for 'keys' command")
	}

	return parser.SerializeArray(kvStore.GetKeysWithPattern(cmds[1]))
}

func handleTypeCommand(cmds []string, kvStore *store.Store) []byte {
	if len(cmds) != 2 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'type' command")
	}

	return parser.SerializeSimpleString(kvStore.GetDataType(cmds[1]))
}

func handlePsyncCommand(cfg *config.ServerConfig, currConnection net.Conn) (response []byte) {
	if cfg.Role == config.RoleSlave {
		return parser.SerializeSimpleError("ERR unknown command 'psync'")
	}
	response = parser.SerializeSimpleString(fmt.Sprintf("%s %s %d", FULLRESYNC, cfg.MasterReplid, cfg.MasterReplOffset))

	b, err := hex.DecodeString(rdb.EMPTY_RDB_HEX)

	if err != nil {
		panic(err)
	}

	// send rdb file
	response = append(response, []byte(fmt.Sprintf("$%d\r\n%s", len(b), string(b)))...)

	cfg.AddReplica(currConnection)

	return response
}

func handleRelpConfCommand(cmds []string, conn net.Conn, cfg *config.ServerConfig) (response []byte) {

	if len(cmds) < 2 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'replconf' command")
	}

	switch strings.ToUpper(cmds[1]) {
	case ACK:
		if cfg.Role == config.RoleSlave {
			return parser.SerializeSimpleError("only master can receive ACK")
		}

		replicaOffset, err := strconv.Atoi(cmds[2])

		if err != nil {
			return parser.SerializeSimpleError("ERR invalid offset")
		}

		for _, replica := range cfg.Replicas {
			if replica.ConnAddr.RemoteAddr().String() == conn.RemoteAddr().String() {
				replica.Offset = replicaOffset
				break
			}
		}

	case GETACK:
		if cfg.Role == config.RoleMaster {
			return parser.SerializeSimpleError("only slave can receive GETACK")
		}
		offset := fmt.Sprintf("%d", cfg.MasterReplOffset)
		response = parser.SerializeArray([]string{REPLCONF, "ACK", offset})
	default:
		response = parser.SerializeSimpleString("OK")
	}

	return response
}

func handleConfigCommand(cmds []string, cfg *config.ServerConfig) []byte {
	if len(cmds) != 3 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'config' command")
	}

	if strings.ToUpper(cmds[1]) != "GET" {
		return parser.SerializeSimpleError("ERR unsupported subcommand for 'config' command")
	}

	switch strings.ToUpper(cmds[2]) {
	case "DIR":
		return parser.SerializeArray([]string{"dir", cfg.RDBDir})
	case "DBFILENAME":
		return parser.SerializeArray([]string{"dbfilename", cfg.RDBFileName})
	default:
		return parser.SerializeSimpleError("ERR unsupported CONFIG parameter")
	}
}

func handleInfoCommand(cfg *config.ServerConfig) []byte {
	sb := strings.Builder{}
	sb.WriteString("# Replication \n")
	sb.WriteString("role:" + cfg.Role + "\n")
	sb.WriteString("master_replid:" + cfg.MasterReplid + "\n")
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d", cfg.MasterReplOffset) + "\n")
	return parser.SerializeBulkString(sb.String())
}
