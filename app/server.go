package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleClient(conn)
	}

}

func handleClient(conn net.Conn) {
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatal("Error reading from connection: ", err.Error())
		}

		fmt.Println("Message recieved: ", string(buf[:n]))

		conn.Write([]byte("+PONG\r\n"))
	}
}
