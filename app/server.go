package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
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

		go handleCommand(conn)
	}
}

func handleCommand(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("Received: %s From: %s\n", buf[:n], conn.RemoteAddr())

		// buf := []byte("*1\r\n$4\r\nping\r\n")
		cmd, msg := parseCommand(string(buf[:]))
		var res []byte
		switch cmd {
		case "ping":
			res = []byte("+PONG\r\n")
		case "echo":
			res = []byte(fmt.Sprintf("+%s\r\n", msg))
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			return
		}

		_, err = conn.Write(res)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
		fmt.Printf("Sent: %s\n", res)
	}
}

// *2\r\n$4\r\necho\r\n$3\r\nhey\r\n
// array, 2 elements
// element 1 - simple string, 4 chars "echo"
// element 2 - simple string, 3 chars "hey"
func parseCommand(buf string) (string, string) {
	a := strings.Split(buf, "\r\n")
	fmt.Printf("Array: %s\n", a)
	cmd, msg := "", ""
	for i := 1; i < len(a); i++ {
		if len(a[i]) == 0 {
			continue
		}
		switch a[i][0] {
		case '$':
			if cmd == "" {
				cmd = a[i+1]
			} else {
				msg = a[i+1]
			}
		}
	}
	fmt.Printf("Command: %s, Message: %s\n", cmd, msg)
	return cmd, msg
}
