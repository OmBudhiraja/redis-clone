package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
	"github.com/codecrafters-io/redis-starter-go/internal/store/datatypes"
)

const (
	opAUX          byte = 0xFA
	opEOF          byte = 0xFF
	opSELECTDB     byte = 0xFE
	opRESIZEDB     byte = 0xFB
	opEXPIRETIME   byte = 0xFD
	opEXPIRETIMEMS byte = 0xFC
)

const (
	EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

type RDBFile struct {
	Items map[string]store.Data
}

func New(cfg *config.ServerConfig) *RDBFile {
	path := cfg.GetRDBFilePath()

	// currently only supports single db
	db := &RDBFile{
		Items: make(map[string]store.Data),
	}

	if path == "" {
		return db
	}

	_, err := os.Stat(path)

	if err == nil {

		file, err := os.Open(path)

		if err != nil {
			panic("INVALID PATH??")
		}

		db.Parse(file)
	}

	return db
}

func (rdbFile *RDBFile) Parse(r io.Reader) {
	reader := bufio.NewReader(r)

	magicString := string(readBytes(reader, 5))

	if magicString != "REDIS" {
		panic("Not a valid rdb file")
	}

	reader.Discard(4) // discard version

outerLoop:
	for {
		code, err := reader.ReadByte()

		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF???")
				break
			}

			panic(err)
		}

		switch code {
		case opAUX:
			key := readString(reader)
			switch key {
			case "redis-ver":
				fmt.Println(key, readString(reader))
			case "redis-bits":
				fmt.Println(key, readInteger(reader))
			case "ctime":
				fmt.Println(key, readString(reader))
			case "used-mem":
				fmt.Println(key, readString(reader))
			default:
				fmt.Println("unknown aux field", key, readString(reader))
			}

		case opSELECTDB:
			fmt.Println("SELECTDB", readInteger(reader))

		case opRESIZEDB:
			fmt.Println("RESIZEDB", readInteger(reader), readInteger(reader)) // hashTableSize, expiryHashTableSize

		case opEOF:
			fmt.Println("EOF")
			break outerLoop

		default:
			var expiration time.Time
			var valueType byte

			if code == opEXPIRETIMEMS {
				bytes := readBytes(reader, 8)
				expiration = time.UnixMilli(int64(binary.LittleEndian.Uint64(bytes)))
				valueType = readByte(reader)
			} else if code == opEXPIRETIME {
				bytes := readBytes(reader, 4)
				expiration = time.Unix(int64(binary.LittleEndian.Uint32(bytes)), 0)
				valueType = readByte(reader)
			} else {
				valueType = code
			}

			key := readString(reader)
			switch valueType {
			case 0: // strings
				value := readString(reader)

				// skip expired keys
				if expiration.IsZero() || expiration.After(time.Now()) {
					rdbFile.Items[key] = &datatypes.String{
						DataType: "string",
						Value:    value,
						Expiry:   expiration,
					}
				}
			default:
				panic(fmt.Sprintf("unknown value type: %08b", valueType))
			}
		}
	}
}

func (rdb *RDBFile) Inject(store *store.Store) {
	for key, entry := range rdb.Items {
		stringEntry, ok := entry.(*datatypes.String)
		if ok {
			store.Set(key, stringEntry.Value, stringEntry.Expiry)
		}
	}
}

func readString(reader *bufio.Reader) string {
	bytesLength := readInteger(reader)
	bytes := readBytes(reader, bytesLength)

	return string(bytes)
}

func readInteger(reader *bufio.Reader) int {
	b := readByte(reader)

	firstTwoBits := b & 0b1100_0000
	lastSixBits := b & 0b0011_1111

	switch firstTwoBits {
	// The next 6 bits represent the length
	case 0b0000_0000:
		return int(lastSixBits)

	// Read one additional byte. The combined 14 bits represent the length
	case 0b0100_0000:
		nextByte := readByte(reader)
		return int(lastSixBits)<<8 | int(nextByte)

	// Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
	case 0b1000_0000:
		next4Bytes := readBytes(reader, 4)
		return int(binary.LittleEndian.Uint32(next4Bytes))

	// The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings
	case 0b1100_0000:
		switch lastSixBits {
		case 0:
			return int(readByte(reader))
		case 1:
			return int(binary.LittleEndian.Uint16(readBytes(reader, 2)))
		case 2:
			return int(binary.LittleEndian.Uint32(readBytes(reader, 4)))
		default:
			panic(fmt.Sprintf("integer encoding does not YET handle: %08b", lastSixBits))
		}

	default:
		panic(fmt.Sprintf("length encoding does not YET handle: %08b", lastSixBits))
	}

}

func readByte(reader *bufio.Reader) byte {
	return readBytes(reader, 1)[0]
}

func readBytes(reader *bufio.Reader, n int) []byte {
	b := make([]byte, n)

	_, err := reader.Read(b)

	if err != nil {
		panic(err)
	}

	return b
}
