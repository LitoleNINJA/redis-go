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

var rdb = map[string]string{}

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
		// buf := []byte("*3\r\n$3\r\nset\r\n$5\r\ngrape\r\n$10\r\nstrawberry\r\n")
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("Received: %s From: %s\n", buf[:n], conn.RemoteAddr())

		cmd, args := parseCommand(string(buf[:]))
		var res []byte
		switch cmd {
		case "ping":
			res = []byte("+PONG\r\n")
		case "echo":
			res = []byte(fmt.Sprintf("+%s\r\n", args[0]))
		case "set":
			res = []byte("+OK\r\n")
			rdb[args[0]] = args[1]
			fmt.Println("RDB: ", rdb)
		case "get":
			val, ok := rdb[args[0]]
			if !ok {
				fmt.Println("Key not found: ", args[0])
				res = []byte("$-1\r\n")
			} else {
				fmt.Println("Value: ", val)
				res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
			}
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

// *3\r\n$3\r\nset\r\n$5\r\ngrape\r\n$10\r\nstrawberry\r\n
// array, 3 elements
// element 1 - bulk string, 3 chars "set"
// element 2 - bulk string, 5 chars "grape"
// element 3 - bulk string, 10 chars "strawberry
func parseCommand(buf string) (string, []string) {
	a := strings.Split(buf, "\r\n")
	fmt.Printf("Array: %v Length: %v\n", a, len(a))
	var cmd string
	args := make([]string, 0)
	for i := 1; i < len(a); i++ {
		if len(a[i]) == 0 {
			continue
		}
		switch a[i][0] {
		case '$':
			if cmd == "" {
				cmd = a[i+1]
			} else {
				args = append(args, a[i+1])
			}
		}
	}
	fmt.Printf("Command: %s, Args: %v\n", cmd, args)
	return cmd, args
}
