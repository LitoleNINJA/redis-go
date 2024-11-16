package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var rdb = redisDB{
	data:        make(map[string]redisValue),
	role:        "master",
	replID:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
	mux:         &sync.Mutex{},
	buffer:      make([]string, 0),
	replicas:    make(map[string]net.Conn),
	offset:      0,
	ackCnt:      0,
	ackChan:     make(chan struct{}, 10),
	rdbFile:     rdbFile{data: make(map[string]redisValue)},
	redisStream: redisStream{data: make(map[string][]redisStreamEntry), streamIds: make(map[string]int)},
}

var port = flag.String("port", "6379", "Port to listen on")
var isReplica = flag.String("replicaof", "", "Replica of")
var dir = flag.String("dir", "", "Directory to store RDB file")
var dbFileName = flag.String("dbfilename", "", "RDB file name")
var handshakeComplete = false
var lastStreamID = ""

func main() {
	var masterIp, masterPort string
	flag.Parse()
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if args[i] == "--replicaof" && i+1 < len(args) {
			IPandPORT := args[i+1]
			masterIp = strings.Split(IPandPORT, " ")[0]
			masterPort = strings.Split(IPandPORT, " ")[1]
			break
		}
	}
	if *isReplica != "" {
		rdb.role = "slave"
	}
	if *dir != "" && *dbFileName != "" {
		rdbFile, err := readRDBFile(*dir, *dbFileName)
		if err != nil {
			fmt.Println("Error reading RDB file: ", err.Error())
		} else {
			rdb.rdbFile = rdbFile
			fmt.Println("RDB file loaded")

			for k, v := range rdb.rdbFile.data {
				rdb.setValue(k, v.value, "string", v.createdAt, v.expiry)
				fmt.Println("Loaded key: ", k, v.value, time.Now().UnixMilli()-v.createdAt, v.expiry)
			}
		}
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
		go handleConnection(conn)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("\nReceived: %s\n From: %s\n", printCommand(buf[:n]), conn.RemoteAddr())

		addCommandToBuffer(string(buf), n)

		for len(rdb.buffer) > 0 {
			if string(rdb.buffer[0][0]) != "*" {
				rdb.buffer = rdb.buffer[1:]
				continue
			}
			cmd, args, totalBytes := parseCommand(rdb.buffer[0])
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
		if rdb.role == "master" {
			res = []byte("+PONG\r\n")
		} else {
			res = []byte("")
		}
	case "echo":
		res = []byte(fmt.Sprintf("+%s\r\n", args[0]))
	case "set":
		var exp int64
		if len(args) < 4 {
			exp = 0
		} else {
			exp, _ = strconv.ParseInt(args[3], 10, 64)
		}

		res = setKeyValue(args[0], args[1], exp, totalBytes)
	case "get":
		val, err := rdb.getValue(args[0])
		if err != "" {
			res = []byte(err)
		} else {
			res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value))
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
				rdb.incrementACK()
				fmt.Println("Ack count: ", rdb.getAckCnt())
				return []byte("")
			} else {
				res = []byte("+OK\r\n")
			}
		} else {
			res = []byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(strconv.Itoa(rdb.offset)), strconv.Itoa(rdb.offset)))
			fmt.Println("Sending ACK to master")
			handshakeComplete = true
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

			// ask for ack from slaves (for testing)
			// getACK()
		} else {
			res = []byte("-ERR not a master\r\n")
		}
	case "wait":
		if rdb.offset == 0 {
			fmt.Println("Master has not propagated any commands")
			res = []byte(fmt.Sprintf(":%d\r\n", len(rdb.replicas)))
		} else {
			minRepCnt, _ := strconv.Atoi(args[0])
			timeout, _ := strconv.Atoi(args[1])
			endTime := time.Now().Add(time.Duration(timeout) * time.Millisecond)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			rdb.setAckCnt(0)

			getACK()
			done := false

			for !done {
				select {
				case <-rdb.ackChan:
					if rdb.getAckCnt() >= minRepCnt {
						done = true
					}
				case <-tick.C:
					if time.Now().After(endTime) {
						done = true
					}
				}
			}
			res = []byte(fmt.Sprintf(":%d\r\n", rdb.getAckCnt()))
		}
	case "config":
		if args[0] == "get" && args[1] == "dir" {
			res = []byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(*dir), *dir))
		} else if args[0] == "get" && args[1] == "dbfilename" {
			res = []byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(*dbFileName), *dbFileName))
		} else {
			res = []byte("-ERR unsupported CONFIG parameter\r\n")
		}
	case "keys":
		keys := make([]string, 0)
		for k := range rdb.rdbFile.data {
			keys = append(keys, k)
		}
		res = []byte(fmt.Sprintf("*%d\r\n", len(keys)))
		for _, k := range keys {
			res = append(res, fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)...)
		}
	case "type":
		if val, ok := rdb.data[args[0]]; ok {
			res = []byte(fmt.Sprintf("+%s\r\n", val.valType))
		} else {
			res = []byte("+none\r\n")
		}
	case "xadd":
		key := args[0]
		id := args[1]

		if id == "*" {
			t := int(time.Now().UnixMilli())
			val, ok := rdb.redisStream.streamIds[strconv.Itoa(t)]
			if !ok {
				id = strconv.Itoa(t) + "-0"
			} else {
				id = strconv.Itoa(t) + "-" + strconv.Itoa(val+1)
			}
		} else if strings.HasSuffix(id, "-*") {
			parts := strings.Split(id, "-")
			val, ok := rdb.redisStream.streamIds[parts[0]]
			if !ok {
				if strings.HasPrefix(id, "0-") {
					id = id[:len(id)-1] + "1"
				} else {
					id = id[:len(id)-1] + "0"
				}
			} else {
				id = id[:len(id)-1] + strconv.Itoa(val+1)
			}
		} else {
			err := validateStreamID(id)
			if err != "" {
				res = []byte(fmt.Sprintf("-ERR %s\r\n", err))
				break
			}
		}
		lastStreamID = id
		fields := make(map[string]string)
		for i := 2; i < len(args); i += 2 {
			fields[args[i]] = args[i+1]
		}

		rdb.redisStream.data[key] = append(rdb.redisStream.data[key], redisStreamEntry{id: id, fields: fields})
		parts := strings.Split(id, "-")
		val, _ := strconv.Atoi(parts[1])
		rdb.redisStream.streamIds[parts[0]] = val
		fmt.Printf("Added stream entry: %s, %s\n", key, id)
		rdb.setValue(key, id, "stream", time.Now().UnixMilli(), 0)
		res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id))
	case "xrange":
		key := args[0]
		start := args[1]
		end := args[2]
		var startVal string
		var startSeq int
		var endVal string
		var endSeq int
		if strings.Contains(start, "-") {
			parts := strings.Split(start, "-")
			startVal = parts[0]
			startSeq, _ = strconv.Atoi(parts[1])
		} else {
			startVal = start
			startSeq = 0
		}
		if strings.Contains(end, "-") {
			parts := strings.Split(end, "-")
			endVal = parts[0]
			endSeq, _ = strconv.Atoi(parts[1])
		} else {
			endVal = end
			endSeq = 0
		}
		if end == "+" {
			endVal = "9999999999999"
			endSeq = 999999999
		}

		entries := make([]redisStreamEntry, 0)
		for _, v := range rdb.redisStream.data[key] {
			parts := strings.Split(v.id, "-")
			val, _ := strconv.Atoi(parts[1])
			if (parts[0] > startVal || (parts[0] == startVal && val >= startSeq)) && (parts[0] < endVal || (parts[0] == endVal && val <= endSeq)) {
				entries = append(entries, v)
			}
		}
		fmt.Println("Stream entries: ", entries)

		resString := fmt.Sprintf("*%d\r\n", len(entries))
		for _, entry := range entries {
			resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.id), entry.id)
			for k, v := range entry.fields {
				resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
			}
		}
		res = []byte(resString)
	case "xread":
		if args[0] == "streams" {
			entries := handleXRead(args[1:])
			res := fmt.Sprintf("*%d\r\n", len(entries))
			for _, entry := range entries {
				res += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*1\r\n", len(entry.key), entry.key)
				for _, entry := range entry.entry {
					res += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.id), entry.id)
					for k, v := range entry.fields {
						res += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
					}
				}
			}
			return []byte(res)
		} else if args[0] == "block" {
			go handleBlockXRead(args[1:], conn)
			res = []byte("")
		}
	case "incr":
		val, err := rdb.getValue(args[0])
		if err != "" {
			fmt.Printf("%s : value not found !\n", args[0])
			setKeyValue(args[0], "1", 0, totalBytes)
			res = []byte(":1\r\n")
			break
		}
		if val.valType != "int" {
			fmt.Printf("Can not increment value of type : %s", val.valType)
			res = []byte("-ERR value is not an integer or out of range\r\n")
			break
		}

		intVal, _ := strconv.ParseInt(val.value, 10, 64)
		intVal++
		stringVal := strconv.FormatInt(intVal, 10)
		setKeyValue(args[0], stringVal, 0, totalBytes)

		res = []byte(":" + stringVal + "\r\n")
	case "multi":
		res = []byte("+OK\r\n")
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		if rdb.role == "master" {
			res = []byte("-ERR unknown command\r\n")
		} else {
			res = []byte("")
		}
	}

	if rdb.role == "slave" && handshakeComplete {
		rdb.offset += totalBytes
		fmt.Printf("\nCmd: %s,  Current Bytes: %d,  Bytes processed: %d\n", cmd, totalBytes, rdb.offset)
	}

	return res
}
