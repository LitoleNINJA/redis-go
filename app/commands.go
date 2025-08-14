package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func handleCommand(cmd string, args []string, conn net.Conn, totalBytes int, rdb *redisDB) []byte {
	connAddr := conn.RemoteAddr().String()
	connState := rdb.getConnState(connAddr)

	if rdb.role == "master" && connState.multi && cmd != "multi" && cmd != "exec" && cmd != "discard" {
		connState.cmdQueue = append(connState.cmdQueue, redisCommands{
			cmd:  cmd,
			args: args,
		})
		return []byte("+QUEUED\r\n")
	}

	var response []byte
	switch cmd {
	case "ping":
		response = handlePingCommand(rdb)
	case "echo":
		response = handleEchoCommand(args)
	case "set":
		response = handleSetCommand(args, totalBytes, rdb)
	case "get":
		response = handleGetCommand(args, rdb)
	case "info":
		response = handleInfoCommand(rdb)
	case "replconf":
		response = handleReplConfCommand(args, rdb)
	case "psync":
		response = handlePSyncCommand(conn, rdb)
	case "wait":
		response = handleWaitCommand(args, rdb)
	case "config":
		response = handleConfigCommand(args)
	case "keys":
		response = handleKeysCommand(rdb)
	case "type":
		response = handleTypeCommand(args, rdb)
	case "xadd":
		response = handleXAddCommand(args, rdb)
	case "xrange":
		response = handleXRangeCommand(args, rdb)
	case "xread":
		response = handleXReadCommand(args, conn, rdb)
	case "incr":
		response = handleIncrCommand(args, totalBytes, rdb)
	case "multi":
		response = handleMultiCommand(connState)
	case "exec":
		response = handleExecCommand(connState, conn, totalBytes, rdb)
	case "discard":
		response = handleDiscardCommand(connState)
	default:
		response = handleUnknownCommand(cmd, rdb)
	}

	updateSlaveOffset(cmd, totalBytes, rdb)
	return response
}

func handlePingCommand(rdb *redisDB) []byte {
	if rdb.role == "master" {
		return []byte("+PONG\r\n")
	}
	return []byte("")
}

func handleEchoCommand(args []string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", args[0]))
}

func handleSetCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	var exp int64
	if len(args) < 4 {
		exp = 0
	} else {
		exp, _ = strconv.ParseInt(args[3], 10, 64)
	}
	return setKeyValue(args[0], args[1], exp, totalBytes, rdb)
}

func handleGetCommand(args []string, rdb *redisDB) []byte {
	val, err := rdb.getValue(args[0])
	if err != "" {
		return []byte(err)
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value))
}

func handleInfoCommand(rdb *redisDB) []byte {
	info := replicationInfo{
		role:               rdb.role,
		master_replid:      DefaultReplicationID,
		master_repl_offset: 0,
	}
	return info.infoResp()
}

func handleConfigCommand(args []string) []byte {
	if args[0] == "get" && args[1] == "dir" {
		return []byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(*dir), *dir))
	} else if args[0] == "get" && args[1] == "dbfilename" {
		return []byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(*dbFileName), *dbFileName))
	}
	return []byte("-ERR unsupported CONFIG parameter\r\n")
}

func handleKeysCommand(rdb *redisDB) []byte {
	keys := make([]string, 0)
	for k := range rdb.rdbFile.data {
		keys = append(keys, k)
	}
	response := []byte(fmt.Sprintf("*%d\r\n", len(keys)))
	for _, k := range keys {
		response = append(response, fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)...)
	}
	return response
}

func handleTypeCommand(args []string, rdb *redisDB) []byte {
	if val, ok := rdb.data[args[0]]; ok {
		return []byte(fmt.Sprintf("+%s\r\n", val.valType))
	}
	return []byte("+none\r\n")
}

func handleXAddCommand(args []string, rdb *redisDB) []byte {
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
			return []byte(fmt.Sprintf("-ERR %s\r\n", err))
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
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id))
}

func handleXRangeCommand(args []string, rdb *redisDB) []byte {
	key := args[0]
	start := args[1]
	end := args[2]

	startVal, startSeq := parseStreamID(start)
	endVal, endSeq := parseStreamID(end)

	if end == "+" {
		endVal = MaxStreamID
		endSeq = MaxSequenceID
	}

	entries := make([]redisStreamEntry, 0)
	for _, v := range rdb.redisStream.data[key] {
		parts := strings.Split(v.id, "-")
		val, _ := strconv.Atoi(parts[1])
		if (parts[0] > startVal || (parts[0] == startVal && val >= startSeq)) &&
			(parts[0] < endVal || (parts[0] == endVal && val <= endSeq)) {
			entries = append(entries, v)
		}
	}
	fmt.Println("Stream entries: ", entries)

	return buildStreamResponse(entries)
}

func parseStreamID(id string) (string, int) {
	if strings.Contains(id, "-") {
		parts := strings.Split(id, "-")
		seq, _ := strconv.Atoi(parts[1])
		return parts[0], seq
	}
	return id, 0
}

func buildStreamResponse(entries []redisStreamEntry) []byte {
	resString := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.id), entry.id)
		for k, v := range entry.fields {
			resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
		}
	}
	return []byte(resString)
}

func handleXReadCommand(args []string, conn net.Conn, rdb *redisDB) []byte {
	switch args[0] {
	case "streams":
		entries := handleXRead(args[1:], rdb)
		response := fmt.Sprintf("*%d\r\n", len(entries))
		for _, entry := range entries {
			response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*1\r\n", len(entry.key), entry.key)
			for _, entryData := range entry.entry {
				response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entryData.id), entryData.id)
				for k, v := range entryData.fields {
					response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
				}
			}
		}
		return []byte(response)
	case "block":
		go handleBlockXRead(args[1:], conn, rdb)
		return []byte("")
	}
	return []byte("")
}

func handleIncrCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	val, err := rdb.getValue(args[0])
	if err != "" {
		fmt.Printf("%s : value not found !\n", args[0])
		setKeyValue(args[0], "1", 0, totalBytes, rdb)
		return []byte(":1\r\n")
	}
	if val.valType != "int" {
		fmt.Printf("Can not increment value of type : %s", val.valType)
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	intVal, _ := strconv.ParseInt(val.value, 10, 64)
	intVal++
	stringVal := strconv.FormatInt(intVal, 10)
	setKeyValue(args[0], stringVal, 0, totalBytes, rdb)
	return []byte(":" + stringVal + "\r\n")
}

func handleMultiCommand(connState *connectionState) []byte {
	connState.multi = true
	connState.cmdQueue = make([]redisCommands, 0)
	return []byte("+OK\r\n")
}

func handleExecCommand(connState *connectionState, conn net.Conn, totalBytes int, rdb *redisDB) []byte {
	if !connState.multi {
		return []byte("-ERR EXEC without MULTI\r\n")
	}
	if len(connState.cmdQueue) == 0 {
		return []byte("*0\r\n")
	}

	connState.multi = false
	resString := ""
	for _, command := range connState.cmdQueue {
		fmt.Println("\nExecuting cmd : ", command.cmd)
		curRes := handleCommand(command.cmd, command.args, conn, totalBytes, rdb)
		resString += string(curRes)
	}
	return []byte("*" + strconv.FormatInt(int64(len(connState.cmdQueue)), 10) + "\r\n" + resString)
}

func handleDiscardCommand(connState *connectionState) []byte {
	if !connState.multi {
		return []byte("-ERR DISCARD without MULTI\r\n")
	}
	connState.multi = false
	connState.cmdQueue = make([]redisCommands, 0)
	return []byte("+OK\r\n")
}

func handleUnknownCommand(cmd string, rdb *redisDB) []byte {
	fmt.Printf("Unknown command: %s\n", cmd)
	if rdb.role == "master" {
		return []byte("-ERR unknown command\r\n")
	}
	return []byte("")
}

func updateSlaveOffset(cmd string, totalBytes int, rdb *redisDB) {
	if rdb.role == "slave" && handshakeComplete {
		rdb.offset += totalBytes
		fmt.Printf("\nCmd: %s,  Current Bytes: %d,  Bytes processed: %d\n", cmd, totalBytes, rdb.offset)
	}
}
