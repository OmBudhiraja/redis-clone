package command

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
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
)

const (
	EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

func Handler(cmds []string, conn net.Conn, kvStore *store.Store, cfg *config.ServerConfig) []byte {

	var response []byte

	switch strings.ToUpper(cmds[0]) {
	case GET:
		response = handleGet(cmds, kvStore)
	case SET:
		response = handleSet(cmds, kvStore, cfg)
	case PING:
		response = parser.SerializeSimpleString("PONG")
	case ECHO:
		if len(cmds) != 2 {
			response = parser.SerializeSimpleError("ERR wrong number of arguments for 'echo' command")
		} else {
			response = parser.SerializeBulkString(cmds[1])
		}
	case INFO:
		response = handleInfo(cfg)
	case REPLCONF:
		if cfg.Role == config.RoleSlave {
			break
		}
		response = parser.SerializeSimpleString("OK")
	case PSYNC:
		response = handlePsync(cfg, conn)
	default:
		response = parser.SerializeSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmds[0]))
	}

	return response
}

func handleGet(cmds []string, kvStore *store.Store) []byte {
	if len(cmds) != 2 {
		return parser.SerializeSimpleError("ERR wrong number of arguments for 'get' command")
	} else {
		value := kvStore.Get(cmds[1])
		return parser.SerializeBulkString(value)
	}
}

func handleSet(cmds []string, kvStore *store.Store, cfg *config.ServerConfig) []byte {
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

func handlePsync(cfg *config.ServerConfig, currConnection net.Conn) (response []byte) {
	if cfg.Role == config.RoleSlave {
		return parser.SerializeSimpleError("ERR unknown command 'psync'")
	}
	response = parser.SerializeSimpleString(fmt.Sprintf("%s %s %d", FULLRESYNC, cfg.MasterReplid, cfg.MasterReplOffset))

	b, err := hex.DecodeString(EMPTY_RDB_HEX)

	if err != nil {
		panic(err)
	}

	response = append(response, []byte(fmt.Sprintf("$%d\r\n%s", len(b), string(b)))...)

	cfg.Replicas = append(cfg.Replicas, config.Replica{
		ConnAddr: currConnection,
	})

	return response
}

func handleInfo(cfg *config.ServerConfig) []byte {
	sb := strings.Builder{}
	sb.WriteString("# Replication \n")
	sb.WriteString("role:" + cfg.Role + "\n")
	sb.WriteString("master_replid:" + cfg.MasterReplid + "\n")
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d", cfg.MasterReplOffset) + "\n")
	return parser.SerializeBulkString(sb.String())
}
