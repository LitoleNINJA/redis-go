package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

// RedisDB is a simple in-memory key-value store
type redisValue struct {
	value     string
	createdAt int64
	expiry    int64
}

var rdb = map[string]redisValue{}

func main() {
	port := "6379"
	if len(os.Args) >= 2 {
		port = os.Args[2]
	}
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
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
		// buf := []byte("*5\r\n$3\r\nset\r\n$5\r\ngrape\r\n$10\r\nstrawberry\r\n$2\r\npx\r\n$3\r\n100\r\n")
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
			var exp int64
			if len(args) < 4 {
				exp = 0
			} else {
				exp, _ = strconv.ParseInt(args[3], 10, 64)
			}
			rdb[args[0]] = redisValue{
				value:     args[1],
				createdAt: time.Now().UnixMilli(),
				expiry:    exp,
			}
			fmt.Println("RDB: ", rdb)
		case "get":
			val, ok := rdb[args[0]]
			if !ok {
				fmt.Println("Key not found: ", args[0])
				res = []byte("$-1\r\n")
			} else {
				fmt.Println("Value: ", val)
				timeElapsed := time.Now().UnixMilli() - val.createdAt
				if val.expiry > 0 && timeElapsed > val.expiry {
					fmt.Println("Key expired: ", args[0])
					res = []byte("$-1\r\n")
					delete(rdb, args[0])
				} else {
					res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value))
				}
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
				cmd = strings.ToLower(a[i+1])
			} else {
				args = append(args, strings.ToLower(a[i+1]))
			}
		}
	}
	fmt.Printf("Command: %s, Args: %v\n", cmd, args)
	return cmd, args
}
