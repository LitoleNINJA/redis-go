package main

import (
	"encoding/hex"
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
	data     map[string]redisValue
	role     string
	buffer   []string
	replicas map[string]net.Conn
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
	data:     make(map[string]redisValue),
	role:     "master",
	buffer:   make([]string, 0),
	replicas: make(map[string]net.Conn),
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
		err := handleHandshake(masterIp, masterPort)
		if err != nil {
			fmt.Println("Error during handshake: ", err.Error())
			os.Exit(1)
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Printf("%s Listening on %s\n", rdb.role, l.Addr().String())

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
		fmt.Printf("\nReceived: %s From: %s\n", buf[:n], conn.RemoteAddr())

		addCommandToBuffer(string(buf))

		for len(rdb.buffer) > 0 {
			cmd, args := parseCommand(rdb.buffer[0])
			rdb.buffer = rdb.buffer[1:]

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
				migrateToSlaves(args[0], args[1])
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
			case "replconf":
				if rdb.role == "master" {
					res = []byte("+OK\r\n")
				} else {
					res = []byte("-ERR not a master\r\n")
				}
			case "psync":
				if rdb.role == "master" {
					res = []byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
					conn.Write(res)
					emptyRdbFileHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
					emptyRdbFile, err := hex.DecodeString(emptyRdbFileHex)
					if err != nil {
						fmt.Println("Error decoding empty RDB file: ", err.Error())
						return
					}
					res = []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRdbFile), emptyRdbFile))
					// add slave to replicas
					rdb.replicas[conn.RemoteAddr().String()] = conn
				} else {
					res = []byte("-ERR not a master\r\n")
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
}

func addCommandToBuffer(buf string) {
	a := strings.Split(buf, "*")
	for i := 0; i < len(a); i++ {
		if len(a[i]) > 0 {
			rdb.buffer = append(rdb.buffer, a[i])
		}
	}
	fmt.Printf("Buffer: %v :ength: %d\n", rdb.buffer, len(rdb.buffer))
}

func parseCommand(buf string) (string, []string) {
	a := strings.Split(buf, "\r\n")
	n, _ := strconv.ParseInt(a[0], 10, 64)

	var cmd string
	args := make([]string, 0)
	for i := 0; i < int(n); i++ {
		pos := 2*i + 1
		if len(a[i]) == 0 {
			continue
		}
		switch a[pos][0] {
		case '$':
			if cmd == "" {
				cmd = strings.ToLower(a[pos+1])
			} else {
				args = append(args, strings.ToLower(a[pos+1]))
			}
		}
	}
	fmt.Printf("Command: %s, Args: %v\n", cmd, args)
	return cmd, args
}

func handleHandshake(masterIp, masterPort string) error {
	conn, err := net.Dial("tcp", masterIp+":"+masterPort)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return err
	}
	defer conn.Close()

	err = pingMaster(conn)
	if err != nil {
		fmt.Println("Error pinging master: ", err.Error())
		return err
	}

	err = sendREPLConf(conn, "listening-port", *port)
	if err != nil {
		fmt.Println("Error sending REPLCONF 1: ", err.Error())
		return err
	}
	err = sendREPLConf(conn, "capa", "psync2")
	if err != nil {
		fmt.Println("Error sending REPLCONF 2: ", err.Error())
		return err
	}

	err = sendPSYNC(conn, "?", -1)
	if err != nil {
		fmt.Println("Error sending PSYNC: ", err.Error())
		return err
	}

	return nil
}

func pingMaster(conn net.Conn) error {
	// ping master
	_, err := conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		return err
	}
	fmt.Printf("Received: %s\n", buf[:n])
	if string(buf[:n]) != "+PONG\r\n" {
		return fmt.Errorf("master did not respond with PONG: %s", string(buf[:n]))
	}
	return nil
}

func sendREPLConf(conn net.Conn, cmd, args string) error {
	// REPLCONF to master
	_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd), cmd, len(args), args)))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		return err
	}
	fmt.Printf("Received: %s\n", buf[:n])
	if string(buf[:n]) != "+OK\r\n" {
		return fmt.Errorf("master did not respond with OK: %s", string(buf[:n]))
	}
	return nil
}

func sendPSYNC(conn net.Conn, replId string, offset int) error {
	// PSYNC to master
	offset_str := strconv.Itoa(offset)
	_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(replId), replId, len(offset_str), offset_str)))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		return err
	}
	fmt.Printf("Received: %s\n", buf[:n])
	return nil
}

func migrateToSlaves(key, value string) {
	for _, conn := range rdb.replicas {
		res := []byte(fmt.Sprintf("*3\r\n$3\r\nset\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
		_, err := conn.Write(res)
		if err != nil {
			fmt.Println("Error writing to replica: ", err.Error())
		}
		fmt.Printf("Sent Migration: %s to %s\n", string(res), conn.RemoteAddr())
	}
}
