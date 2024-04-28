package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
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
	data     map[string]redisValue
	role     string
	replID   string
	mux      *sync.Mutex
	buffer   []string
	replicas map[string]net.Conn
	offset   int
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
		return "$-1\r\n", false
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
	data:     make(map[string]redisValue),
	role:     "master",
	replID:   "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
	mux:      &sync.Mutex{},
	buffer:   make([]string, 0),
	replicas: make(map[string]net.Conn),
	offset:   0,
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

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Printf("%s Listening on %s\n", rdb.role, l.Addr().String())

	if rdb.role == "slave" {
		fmt.Printf("Replica of: %s:%s\n", masterIp, masterPort)
		conn, err := handleHandshake(masterIp, masterPort)
		if err != nil {
			fmt.Println("Error during handshake: ", err.Error())
			os.Exit(1)
		}
		handleConnection(conn)
	} else {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}

			handleConnection(conn)
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	totalBytes := 0
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("\nReceived: %s\n From: %s\n", printCommand(buf[:n]), conn.RemoteAddr())
		totalBytes = n

		addCommandToBuffer(string(buf))

		for len(rdb.buffer) > 0 {
			cmd, args := parseCommand(rdb.buffer[0])
			rdb.buffer = rdb.buffer[1:]

			res := handleCommand(cmd, args, conn, totalBytes)

			if len(res) > 0 {
				_, err = conn.Write(res)
				if err != nil {
					fmt.Println("Error writing to connection: ", err.Error())
					return
				}
				fmt.Printf("Sent: %s\n", printCommand(res))
			}
		}
	}
}

func handleCommand(cmd string, args []string, conn net.Conn, totalBytes int) []byte {
	var res []byte
	switch cmd {
	case "ping":
		res = []byte("+PONG\r\n")
	case "echo":
		res = []byte(fmt.Sprintf("+%s\r\n", args[0]))
	case "set":
		var exp int64
		if len(args) < 4 {
			exp = 0
		} else {
			exp, _ = strconv.ParseInt(args[3], 10, 64)
		}
		rdb.mux.Lock()
		rdb.setValue(args[0], args[1], exp)
		rdb.mux.Unlock()
		if rdb.role == "master" {
			fmt.Printf("Set key: %s, value: %s, expiry: %d\n", args[0], args[1], exp)
			res = []byte("+OK\r\n")
			migrateToSlaves(args[0], args[1])
		} else {
			fmt.Println("Slave received set command: ", args[0], args[1], exp)
			res = []byte("")
		}
	case "get":
		val, ok := rdb.getValue(args[0])
		if !ok {
			res = []byte(val)
		} else {
			res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		}
	case "info":
		var info replicationInfo
		info.role = rdb.role
		info.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		info.master_repl_offset = 0
		res = info.infoResp()
	case "replconf":
		if rdb.role == "master" {
			if args[0] == "ack" {
				fmt.Println("Received ACK from slave")
				return []byte("")
			} else {
				res = []byte("+OK\r\n")
			}
		} else {
			res = []byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(strconv.Itoa(rdb.offset)), strconv.Itoa(rdb.offset)))
			fmt.Println("Sending ACK to master")
		}
	case "psync":
		if rdb.role == "master" {
			res = []byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
			fmt.Printf("Sent: %s\n", printCommand(res))
			conn.Write(res)
			emptyRdbFileHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
			emptyRdbFile, err := hex.DecodeString(emptyRdbFileHex)
			if err != nil {
				fmt.Println("Error decoding RDB file: ", err.Error())
				return []byte("")
			}
			res = []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRdbFile), emptyRdbFile))
			conn.Write(res)
			fmt.Printf("Sent: %s\n", printCommand(res))
			res = []byte("")

			// add slave to replicas
			rdb.replicas[conn.RemoteAddr().String()] = conn

			// ask for ack from slaves
			getACK()
		} else {
			res = []byte("-ERR not a master\r\n")
		}
	case "type":
		_, ok := rdb.getValue(args[0])
		if !ok {
			res = []byte(fmt.Sprintf("+%s\r\n", "none"))
		} else {
			res = []byte(fmt.Sprintf("+%s\r\n", "string"))
		}
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		if rdb.role == "master" {
			res = []byte("-ERR unknown command\r\n")
		} else {
			res = []byte("")
		}
	}

	if rdb.role == "slave" {
		rdb.offset += totalBytes
		fmt.Printf("\nCurrent Bytes: %d,  Bytes processed: %d\n", totalBytes, rdb.offset)
	}

	return res
}
