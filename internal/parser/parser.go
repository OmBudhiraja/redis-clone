package parser

import (
	"bufio"
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
	RESP_INTEGER       = ':'
	RESP_ERROR         = '-'
)

type Message struct {
	ReadBytes int
	Commands  []string
}

func Deserialize(byteStream *bufio.Reader) (Message, error) {

	var message Message
	var n int

	dataTypeByte, err := byteStream.ReadByte()

	if err != nil {
		return message, err
	}

	message.ReadBytes++

	var commands []string

	switch dataTypeByte {
	case RESP_ARRAY:
		commands, n, err = parseArray(byteStream)
		message.ReadBytes += n

	case RESP_SIMPLE_STRING:
		res, n := parseSimpleString(byteStream)
		commands = append(commands, strings.Split(res, " ")...)
		message.ReadBytes += n

	case RESP_BULK_STRING:
		str, n, err := parseBulkString(byteStream)

		if err == nil {
			commands = append(commands, str)
			message.ReadBytes += n
		}
	case RESP_INTEGER:
		_, n, err := parseInteger(byteStream)
		if err == nil {
			message.ReadBytes += n
		}

	}

	message.Commands = commands

	return message, err

}

func SerializeArray(input []string) []byte {

	var buffer string

	for _, v := range input {
		//  SerializeBulkString(v)
		buffer += string(SerializeBulkString(v))
	}

	return []byte(fmt.Sprintf("*%d\r\n%s", len(input), buffer))
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

func SerializeInteger(input int) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", input))
}

func SerializeSimpleError(input string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", input))
}

// format - *<number of elements>\r\n<elements>\r\n
func parseArray(byteStream *bufio.Reader) ([]string, int, error) {
	var bytesRead int

	value, n := readUntilCRLF(byteStream)
	bytesRead += n

	noOfElements, err := strconv.Atoi(value)

	if err != nil {
		return nil, bytesRead, err
	}

	commands := make([]string, noOfElements)

	for i := 0; i < noOfElements; i++ {
		dataTypeByte, err := byteStream.ReadByte()
		bytesRead++

		if err != nil {
			return commands, bytesRead, err
		}

		switch dataTypeByte {
		case RESP_SIMPLE_STRING:
			commands[i], n = parseSimpleString(byteStream)
			bytesRead += n
		case RESP_BULK_STRING:
			str, n, err := parseBulkString(byteStream)

			if err != nil {
				return commands, bytesRead, err
			}

			commands[i] = str
			bytesRead += n

		default:
			fmt.Println("Unknown type of command", dataTypeByte)
		}

	}

	return commands, bytesRead, nil
}

// format - +<data>\r\n
func parseSimpleString(byteStream *bufio.Reader) (string, int) {
	return readUntilCRLF(byteStream)
}

// format - $<length>\r\n<data>\r\n
func parseBulkString(byteStream *bufio.Reader) (string, int, error) {

	var bytesRead int

	value, n := readUntilCRLF(byteStream)
	bytesRead += n

	commandLength, err := strconv.Atoi(value)

	if err != nil {
		return "", bytesRead, err
	}

	data, n := readUntilCRLF(byteStream)
	bytesRead += n

	if len(data) != commandLength {
		return "", bytesRead, errors.New("length of data is not equal to the length specified")
	}

	return data, bytesRead, nil
}

func parseInteger(byteStream *bufio.Reader) (int, int, error) {
	value, n := readUntilCRLF(byteStream)

	intValue, err := strconv.Atoi(value)

	return intValue, n, err

}

// rdb file format - $<length>\r\n<data> (without trailing CRLF)
func ExpectRDBFile(bytesStream *bufio.Reader) error {
	dataType, err := bytesStream.ReadByte()

	if err != nil {
		return err
	}

	if dataType != RESP_BULK_STRING {
		return errors.New("expected bulk string but got byte: " + string(dataType))
	}

	bytes, _ := readUntilCRLF(bytesStream)
	bytesOfStream, err := strconv.Atoi(bytes)

	if err != nil {
		return err
	}

	rdbBuffer := make([]byte, bytesOfStream)

	_, err = bytesStream.Read(rdbBuffer)

	return err
}

func readUntilCRLF(byteStream *bufio.Reader) (string, int) {
	var result string
	var buffer string

	var n int
	for {
		b, err := byteStream.ReadByte()

		if err != nil {
			return "", n
		}

		n++

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

	return result, n
}
