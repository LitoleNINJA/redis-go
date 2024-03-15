package main

import (
	"flag"
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
type redisDB struct {
	data map[string]redisValue
	role string
}

type redisValue struct {
	value     string
	createdAt int64
	expiry    int64
}

type replicationInfo struct {
	role               string
	master_replid      string
	master_repl_offset int
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
	l1 := "role:" + info.role
	l2 := "master_replid:" + info.master_replid
	l3 := "master_repl_offset:" + strconv.Itoa(info.master_repl_offset)
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(l1)+len(l2)+len(l3), l1+"\r\n"+l2+"\r\n"+l3)
	fmt.Println("InfoResponse: ", resp)

	resp = fmt.Sprintf("$%d\r\n%s\r\n", len(resp), resp)
	resp = fmt.Sprintf("$%d%s%s%s", len(resp), "\r\n", resp, "\r\n")
	return []byte(resp)
}

var rdb = redisDB{
	data: make(map[string]redisValue),
	role: "master",
}

var port = flag.String("port", "6379", "Port to listen on")
var isReplica = flag.String("replicaof", "", "Replica of")

func main() {
	var masterIp, masterPort string
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if args[i] == "--replicaof" && i+2 < len(args) {
			masterIp = args[i+1]
			masterPort = args[i+2]
			break
		}
	}

	flag.Parse()
	if *isReplica != "" {
		rdb.role = "slave"
	}

	if rdb.role == "slave" {
		fmt.Printf("Replica of: %s:%s\n", masterIp, masterPort)
		// ping master
		conn, err := net.Dial("tcp", masterIp+":"+masterPort)
		if err != nil {
			fmt.Println("Error connecting to master: ", err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		_, err = conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
		if err != nil {
			fmt.Println("Error writing to master: ", err.Error())
			os.Exit(1)
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from master: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Received: %s\n", buf[:n])
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
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
			var info replicationInfo
			info.role = rdb.role
			info.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
			info.master_repl_offset = 0
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
