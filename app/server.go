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

type redisValue struct {
	value     string
	createdAt int64
	expiry    int64
}

// RedisDB is a simple in-memory key-value store
type redisDB struct {
	data map[string]redisValue
}

type replicationInfo struct {
	role string
}

func (rdb redisDB) setValue(key string, value string, expiry int64) {
	rdb.data[key] = redisValue{
		value:     value,
		createdAt: time.Now().UnixMilli(),
		expiry:    expiry,
	}
}

func (rdb redisDB) getValue(key string) (string, bool) {
	val, ok := rdb.data[key]
	if !ok {
		fmt.Println("Key not found: ", key)
		return "", false
	}
	timeElapsed := time.Now().UnixMilli() - val.createdAt
	if val.expiry > 0 && timeElapsed > val.expiry {
		fmt.Println("Key expired: ", key)
		delete(rdb.data, key)
		return "", false
	}
	return val.value, true
}

func (info replicationInfo) infoResp() []byte {
	return []byte(fmt.Sprintf("$%d\r\nrole:%s\r\n", len(info.role), info.role))
}

var rdb = redisDB{
	data: make(map[string]redisValue),
}

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
		// buf := []byte("*2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n")
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
			rdb.setValue(args[0], args[1], exp)
		case "get":
			val, ok := rdb.getValue(args[0])
			if !ok {
				res = []byte("$-1\r\n")
			} else {
				res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
			}
		case "info":
			info := replicationInfo{
				role: "master",
			}
			res = info.infoResp()
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
