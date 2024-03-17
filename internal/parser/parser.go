package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	CRLF_RAW = `\r\n`
	CRLF_INT = "\r\n"

	RESP_ARRAY         = '*'
	RESP_BULK_STRING   = '$'
	RESP_SIMPLE_STRING = '+'
	RESP_ERROR         = '-'
)

func Deserialize(input []byte) ([]string, error) {

	byteStream := bufio.NewReader(bytes.NewReader(input))

	dataTypeByte, err := byteStream.ReadByte()

	if err != nil {
		return nil, err
	}

	var commands []string

	switch dataTypeByte {
	case RESP_ARRAY:
		commands, err = parseArray(byteStream)
	case RESP_SIMPLE_STRING:
		commands = append(commands, parseSimpleString(byteStream))
	case RESP_BULK_STRING:
		str, err := parseBulkString(byteStream)

		if err == nil {
			commands = append(commands, str)
		}
	}

	return commands, err

}

func SerializeBulkString(input string) []byte {
	// null bulk string
	if input == "" {
		return []byte("$-1\r\n")
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(input), input))
}

func SerializeSimpleString(input string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", input))
}

func SerializeSimpleError(input string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", input))
}

// format - *<number of elements>\r\n<elements>\r\n
func parseArray(byteStream *bufio.Reader) ([]string, error) {
	noOfElements, err := strconv.Atoi(readUntilCRLF(byteStream))

	if err != nil {
		return nil, err
	}

	commands := make([]string, noOfElements)

	for i := 0; i < noOfElements; i++ {
		dataTypeByte, err := byteStream.ReadByte()

		if err != nil {
			return commands, err
		}

		switch dataTypeByte {
		case RESP_SIMPLE_STRING:
			commands[i] = parseSimpleString(byteStream)
		case RESP_BULK_STRING:
			str, err := parseBulkString(byteStream)

			if err != nil {
				return commands, err
			}

			commands[i] = str

		default:
			fmt.Println("Unknown type of command", dataTypeByte)
		}

	}

	return commands, nil
}

// format - +<data>\r\n
func parseSimpleString(byteStream *bufio.Reader) string {
	return readUntilCRLF(byteStream)
}

// format - $<length>\r\n<data>\r\n
func parseBulkString(byteStream *bufio.Reader) (string, error) {
	length, err := strconv.Atoi(readUntilCRLF(byteStream))

	if err != nil {
		return "", err
	}

	data := readUntilCRLF(byteStream)

	if len(data) != length {
		return "", errors.New("length of data is not equal to the length specified")
	}

	return data, nil
}

func readUntilCRLF(byteStream *bufio.Reader) string {
	var result string
	var buffer string

	for {
		b, err := byteStream.ReadByte()

		if err != nil {
			return ""
		}

		buffer += string(b)

		if strings.HasSuffix(buffer, CRLF_RAW) {
			result = buffer[:len(buffer)-len(CRLF_RAW)]
			break
		}
		if strings.HasSuffix(buffer, CRLF_INT) {
			result = buffer[:len(buffer)-len(CRLF_INT)]
			break
		}
	}

	return result
}
