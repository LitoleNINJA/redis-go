package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	DefaultPort          = "6379"
	DefaultReplicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	EmptyRDBFileHex      = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	MaxStreamID          = "9999999999999"
	MaxSequenceID        = 999999999
	TickerInterval       = 10 * time.Millisecond
)

var (
	port              = flag.String("port", DefaultPort, "Port to listen on")
	isReplica         = flag.String("replicaof", "", "Replica of")
	dir               = flag.String("dir", "", "Directory to store RDB file")
	dbFileName        = flag.String("dbfilename", "", "RDB file name")
	handshakeComplete = false
	lastStreamID      = ""
)

func main() {
	flag.Parse()

	masterIP, masterPort := parseReplicaConfig()
	rdb := createRedisDB()

	if *isReplica != "" {
		rdb.role = "slave"
	}

	loadRDBFileIfConfigured(rdb)

	listener := startServer(rdb.role)
	defer listener.Close()

	if rdb.role == "slave" {
		startReplication(masterIP, masterPort, rdb)
	}

	acceptConnections(listener, rdb)
}

func parseReplicaConfig() (string, string) {
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if args[i] == "--replicaof" && i+1 < len(args) {
			ipAndPort := args[i+1]
			parts := strings.Split(ipAndPort, " ")
			return parts[0], parts[1]
		}
	}
	return "", ""
}

func createRedisDB() *redisDB {
	return &redisDB{
		data:        make(map[string]redisValue),
		role:        "master",
		replID:      DefaultReplicationID,
		mux:         &sync.Mutex{},
		buffer:      make([]string, 0),
		replicas:    make(map[string]net.Conn),
		offset:      0,
		ackCnt:      0,
		ackChan:     make(chan struct{}, 10),
		rdbFile:     rdbFile{data: make(map[string]redisValue)},
		redisStream: redisStream{data: make(map[string][]redisStreamEntry), streamIds: make(map[string]int)},
		connStates:  make(map[string]*connectionState),
		stateMux:    &sync.RWMutex{},
	}
}

func loadRDBFileIfConfigured(rdb *redisDB) {
	if *dir == "" || *dbFileName == "" {
		return
	}

	rdbFile, err := readRDBFile(*dir, *dbFileName)
	if err != nil {
		fmt.Println("Error reading RDB file: ", err.Error())
		return
	}

	rdb.rdbFile = rdbFile
	fmt.Println("RDB file loaded")

	for k, v := range rdb.rdbFile.data {
		rdb.setValue(k, v.value, "string", v.createdAt, v.expiry)
		fmt.Println("Loaded key: ", k, v.value, time.Now().UnixMilli()-v.createdAt, v.expiry)
	}
}

func startServer(role string) net.Listener {
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Printf("%s Listening on %s\n", role, listener.Addr().String())
	return listener
}

func acceptConnections(listener net.Listener, rdb *redisDB) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, rdb)
	}
}

func handleConnection(conn net.Conn, rdb *redisDB) {
	defer func() {
		rdb.removeConnState(conn.RemoteAddr().String())
		conn.Close()
	}()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("\nReceived: %s\n From: %s\n", printCommand(buf[:n]), conn.RemoteAddr())

		addCommandToBuffer(string(buf), n, rdb)
		processBufferedCommands(conn, rdb)
	}
}

func processBufferedCommands(conn net.Conn, rdb *redisDB) {
	for len(rdb.buffer) > 0 {
		if string(rdb.buffer[0][0]) != "*" {
			rdb.buffer = rdb.buffer[1:]
			continue
		}

		cmd, args, totalBytes := parseCommand(rdb.buffer[0])
		rdb.buffer = rdb.buffer[1:]

		response := handleCommand(cmd, args, conn, totalBytes, rdb)

		if len(response) > 0 {
			_, err := conn.Write(response)
			if err != nil {
				fmt.Println("Error writing to connection: ", err.Error())
				return
			}
			fmt.Printf("Sent: %s\n", printCommand(response))
		}
	}
}

func (rdb *redisDB) getConnState(addr string) *connectionState {
	rdb.stateMux.RLock()
	state, exists := rdb.connStates[addr]
	rdb.stateMux.RUnlock()

	if !exists {
		rdb.stateMux.Lock()
		state = &connectionState{
			multi:    false,
			cmdQueue: make([]redisCommands, 0),
		}
		rdb.connStates[addr] = state
		rdb.stateMux.Unlock()
	}

	return state
}

func (rdb *redisDB) removeConnState(addr string) {
	rdb.stateMux.Lock()
	delete(rdb.connStates, addr)
	rdb.stateMux.Unlock()
}
