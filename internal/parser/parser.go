package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func getRawCommands(input []byte) []string {
	commands := strings.Split(string(input), `\r\n`)
	return filter(commands, func(c string) bool {
		return c != "\n"
	})
}

func Deserialize(input []byte) ([]string, error) {
	rawCommands := getRawCommands(input)
	rawCommandsLen := len(rawCommands)
	var parsedCommands []string

	if len(rawCommands) == 0 {
		return nil, errors.New("no command found")
	}

	// parse arrays
	if input[0] == '*' {
		_, err := strconv.Atoi(rawCommands[0][1:])

		if err != nil {
			return nil, errors.New("elements count is not a integer")
		}

		for idx, command := range rawCommands[1:] {
			if strings.HasPrefix(command, "$") {
				parseBulkString(command, rawCommands[idx+1])
			}
		}

		for i := 1; i < rawCommandsLen; i++ {
			if strings.HasPrefix(rawCommands[i], "$") {

				if i+1 >= rawCommandsLen {
					return nil, errors.New("invalid command")
				}

				command, err := parseBulkString(rawCommands[i], rawCommands[i+1])

				if err != nil {
					return nil, err
				}

				parsedCommands = append(parsedCommands, command)

				i++
			} else if strings.HasPrefix(rawCommands[i], "+") {
				parsedCommands = append(parsedCommands, rawCommands[i][1:])
			} else {
				fmt.Println("Unknown command")
			}
		}

	}

	return parsedCommands, nil
}

func SerializeBulkString(input string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(input), input))
}

func SerializeSimpleString(input string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", input))

}

// format - $<length>\r\n<data>\r\n
func parseBulkString(command, data string) (string, error) {
	dataLen, err := strconv.Atoi(command[1:])
	if err != nil {
		return "", errors.New("invalid data length")
	}

	if dataLen != len(data) {
		return "", errors.New("data length does not match with specified length")
	}

	return data, nil
}
func filter[I interface{}](arr []I, fn func(I) bool) []I {
	result := make([]I, 0)

	for _, v := range arr {
		if fn(v) {
			result = append(result, v)
		}
	}

	return result
}
