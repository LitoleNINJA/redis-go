package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Printf("Listening on %s\n", l.Addr().String())

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handlePing(conn)
	}
}

func handlePing(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("Received: %s", buf[:n])
		msg := []byte("+PONG\r\n")
		_, err = conn.Write(msg)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Sent: %s", msg)
	}
}
