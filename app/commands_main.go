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

	// check if user is Authenticated
	if !connState.isAuth {
		return []byte("-NOAUTH Authentication required.\r\n")
	}

	// check if in subs mode, only allow certain commands
	if connState.subMode {
		allowedCommands := []string{"ping", "subscribe", "unsubscribe", "psubscribe", "punsubscribe"}
		if !contains(allowedCommands, cmd) {
			return encodeError(fmt.Sprintf("Can't execute '%s': only (P|S)UBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", cmd))
		}
	}

	if rdb.role == "master" && connState.multi && cmd != "multi" && cmd != "exec" && cmd != "discard" {
		connState.cmdQueue = append(connState.cmdQueue, redisCommands{
			cmd:  cmd,
			args: args,
		})
		return encodeSimpleString("QUEUED")
	}

	var response []byte
	switch cmd {
	case "ping":
		response = handlePingCommand(rdb, connState.subMode)
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
	case "rpush":
		response = handleRpushCommand(args, totalBytes, rdb)
	case "lrange":
		response = handleLrangeCommand(args, rdb)
	case "lpush":
		response = handleLpushCommand(args, totalBytes, rdb)
	case "llen":
		response = handleLLenCommand(args, rdb)
	case "lpop":
		response = handleLPopCommand(args, totalBytes, rdb)
	case "blpop":
		response = handleBLpopCommand(args, rdb)
	case "zadd":
		response = handleZaddCommand(args, totalBytes, rdb)
	case "zrank":
		response = handleZrankCommand(args, rdb)
	case "zrange":
		response = handleZrangeCommand(args, rdb)
	case "zcard":
		response = handleZcardCommand(args, rdb)
	case "zscore":
		response = handleZscoreCommand(args, rdb)
	case "zrem":
		response = handleZremCommand(args, rdb)
	case "subscribe":
		response = handleSubscribeCommand(args, rdb, &conn)
	case "publish":
		response = handlePublishCommand(args, rdb)
	case "unsubscribe":
		response = handleUnsubCommand(args, rdb, &conn)
	case "geoadd":
		response = handleGeoaddCommand(args, rdb, totalBytes)
	case "geopos":
		response = handleGeoposCommnad(args, rdb)
	case "geodist":
		response = handleGeodistCommand(args, rdb)
	case "geosearch":
		response = handleGeosearchCommand(args, rdb)
	case "acl":
		response = handleAclCommand(args)
	case "auth":
		response = handleAuthCommand(args)
	default:
		response = handleUnknownCommand(cmd, rdb)
	}

	updateSlaveOffset(cmd, totalBytes, rdb)
	return response
}

func handlePingCommand(rdb *redisDB, subMode bool) []byte {
	if subMode {
		return encodeArray([]any{"pong", ""})
	}
	if rdb.role == "master" {
		return encodeSimpleString("PONG")
	}
	return []byte("")
}

func handleEchoCommand(args []string) []byte {
	return encodeBulkString(args[0])
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
		return encodeNull()
	}
	valueStr := fmt.Sprintf("%v", val.value)
	return encodeBulkString(valueStr)
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
	if strings.ToLower(args[0]) == "get" && args[1] == "dir" {
		return encodeArray([]any{"dir", *dir})
	} else if strings.ToLower(args[0]) == "get" && args[1] == "dbfilename" {
		return encodeArray([]any{"dbfilename", *dbFileName})
	}
	return encodeError("unsupported CONFIG parameter")
}

func handleKeysCommand(rdb *redisDB) []byte {
	keys := make([]string, 0)
	for k := range rdb.rdbFile.data {
		keys = append(keys, k)
	}
	anyKeys := make([]any, len(keys))
	for i, k := range keys {
		anyKeys[i] = k
	}
	return encodeArray(anyKeys)
}

func handleTypeCommand(args []string, rdb *redisDB) []byte {
	if val, ok := rdb.data[args[0]]; ok {
		return encodeSimpleString(val.valType)
	}
	return encodeSimpleString("none")
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
		setKeyValue(args[0], 1, 0, totalBytes, rdb)
		return encodeInteger(1)
	}
	if val.valType != "int" {
		fmt.Printf("Can not increment value of type : %s", val.valType)
		return encodeError("value is not an integer or out of range")
	}

	valueStr := fmt.Sprintf("%v", val.value)
	intVal, _ := strconv.ParseInt(valueStr, 10, 64)
	intVal++
	setKeyValue(args[0], intVal, 0, totalBytes, rdb)
	return encodeInteger(intVal)
}

func handleMultiCommand(connState *connectionState) []byte {
	connState.multi = true
	connState.cmdQueue = make([]redisCommands, 0)
	return encodeSimpleString("OK")
}

func handleExecCommand(connState *connectionState, conn net.Conn, totalBytes int, rdb *redisDB) []byte {
	if !connState.multi {
		return encodeError("EXEC without MULTI")
	}
	if len(connState.cmdQueue) == 0 {
		connState.multi = false
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
		return encodeError("DISCARD without MULTI")
	}
	connState.multi = false
	connState.cmdQueue = make([]redisCommands, 0)
	return encodeSimpleString("OK")
}

func handleUnknownCommand(cmd string, rdb *redisDB) []byte {
	fmt.Printf("Unknown command: %s\n", cmd)
	if rdb.role == "master" {
		return encodeError("unknown command")
	}
	return []byte("")
}

func handleRpushCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'rpush' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if exists {
		if val.valType != "list" {
			return encodeError(fmt.Sprintf("value is not a list: %s", key))
		}

		debug("RPUSH: Key %s already exists with value %v\n", key, val.value)
		list := val.value.([]string)
		list = append(list, args[1:]...)
		setKeyValue(key, list, 0, totalBytes, rdb)

		return encodeInteger(int64(len(list)))
	} else {
		value := make([]string, 0)
		value = append(value, args[1:]...)
		setKeyValue(key, value, 0, totalBytes, rdb)

		debug("RPUSH: Key %s created with value %v\n", key, value)
		return encodeInteger(int64(len(value)))
	}
}

func handleLrangeCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 3 {
		return encodeError("wrong number of arguments for 'lrange' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return []byte("*0\r\n")
	}
	if val.valType != "list" {
		return encodeError(fmt.Sprintf("value is not a list: %s", key))
	}

	list := val.value.([]string)

	startIdx, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeError("start index is not an integer")
	}
	endIndex, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeError("end index is not an integer")
	}

	// Handle negative indices
	if startIdx < 0 {
		startIdx = len(list) + startIdx
	}
	if endIndex < 0 {
		endIndex = len(list) + endIndex
	}

	if startIdx < 0 {
		startIdx = 0
	}
	if endIndex >= len(list) {
		endIndex = len(list) - 1
	}
	if startIdx > endIndex || startIdx >= len(list) {
		return []byte("*0\r\n")
	}

	result := make([]string, 0)
	for i := startIdx; i <= endIndex; i++ {
		result = append(result, list[i])
	}

	anyResult := make([]any, len(result))
	for i, v := range result {
		anyResult[i] = v
	}
	return encodeArray(anyResult)
}

func handleLpushCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'rpush' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if exists {
		if val.valType != "list" {
			return encodeError(fmt.Sprintf("value is not a list: %s", key))
		}

		debug("RPUSH: Key %s already exists with value %v\n", key, val.value)
		list := val.value.([]string)
		for _, element := range args[1:] {
			list = append([]string{element}, list...)
		}
		setKeyValue(key, list, 0, totalBytes, rdb)

		return encodeInteger(int64(len(list)))
	} else {
		value := make([]string, 0)
		value = append(value, args[1:]...)
		setKeyValue(key, value, 0, totalBytes, rdb)

		debug("RPUSH: Key %s created with value %v\n", key, value)
		return encodeInteger(int64(len(value)))
	}
}

func handleLLenCommand(args []string, rdb *redisDB) []byte {
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'llen' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return encodeInteger(0)
	}

	list := val.value.([]string)

	return encodeInteger(int64(len(list)))
}

func handleLPopCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	if len(args) < 1 {
		return encodeError("wrong number of arguments for 'lpop' command")
	}

	key := args[0]
	value, exists := rdb.data[key]
	if !exists {
		return encodeNull()
	}

	list := value.value.([]string)
	if len(list) == 0 {
		return encodeNull()
	}

	if len(args) == 1 {
		removedValue := list[0]
		setKeyValue(key, list[1:], 0, totalBytes, rdb)

		return encodeBulkString(removedValue)
	} else {
		popCount, _ := strconv.Atoi(args[1])
		removedValues := make([]string, popCount)
		if popCount > len(list) {
			popCount = len(list)
		}

		for i := 0; i < popCount; i++ {
			removedValues[i] = list[i]
		}

		setKeyValue(key, list[popCount:], 0, totalBytes, rdb)

		anyRemovedValues := make([]any, len(removedValues))
		for i, v := range removedValues {
			anyRemovedValues[i] = v
		}
		return encodeArray(anyRemovedValues)
	}
}

func handleBLpopCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'blpop' command")
	}

	key := args[0]
	val, exists := rdb.data[key]

	if !exists {
		debug("BLPOP: Key %s does not exist, blocking...\n", key)
		return handleBlockPop(key, args[1], rdb)
	} else {
		if val.valType != "list" {
			return encodeError(fmt.Sprintf("value is not a list: %s", key))
		}

		list := val.value.([]string)

		if len(list) == 0 {
			debug("BLPOP: Key %s exists but list is empty, blocking...\n", key)
			return handleBlockPop(key, args[1], rdb)
		}

		return handleLPopCommand(args, 0, rdb)
	}
}

func handleZaddCommand(args []string, totalBytes int, rdb *redisDB) []byte {
	if len(args) < 3 || len(args)%2 == 0 {
		return encodeError("wrong number of arguments for 'zadd' command")
	}

	key := args[0]
	val, exists := rdb.data[key]

	var ss *sortedSet
	if exists {
		if val.valType != "zset" {
			return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		ss = val.value.(*sortedSet)
	} else {
		ss = newSortedSet()
	}

	addedCount, err := ss.addMultiple(args[1:])
	if err != nil {
		return encodeError(err.Error())
	}

	setKeyValue(key, ss, 0, totalBytes, rdb)
	return encodeInteger(int64(addedCount))
}

func handleZrankCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'zrank' command")
	}

	key := args[0]
	member := args[1]

	val, exists := rdb.data[key]
	if !exists {
		debug("ZRANK: Key %s does not exist\n", key)
		return encodeNull()
	}

	if val.valType != "zset" {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ss := val.value.(*sortedSet)
	rank, found := ss.rank(member)

	if !found {
		return encodeNull()
	}

	debug("ZRANK: Found member %s in key %s with rank %d\n", member, key, rank)
	return encodeInteger(int64(rank))
}

func handleZrangeCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 3 {
		return encodeError("wrong number of arguments for 'zrange' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		debug("ZRANGE: Key %s does not exist\n", key)
		return []byte("*0\r\n")
	}
	if val.valType != "zset" {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ss := val.value.(*sortedSet)
	values := ss.getValues()

	start, _ := strconv.ParseInt(args[1], 10, 64)
	end, _ := strconv.ParseInt(args[2], 10, 64)

	if start < 0 {
		start = int64(len(values)) + start
	}
	if end < 0 {
		end = int64(len(values)) + end
	}
	if start < 0 {
		start = 0
	}

	if start >= int64(ss.size()) {
		return []byte("*0\r\n")
	}
	if end >= int64(ss.size()) {
		end = int64(ss.size() - 1)
	}
	if start > end {
		return []byte("*0\r\n")
	}

	resp := make([]string, 0)
	for i := start; i <= end; i++ {
		resp = append(resp, values[i])
	}

	anyResp := make([]any, len(resp))
	for i, v := range resp {
		anyResp[i] = v
	}
	return encodeArray(anyResp)
}

func handleZcardCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 1 {
		return encodeError("wrong number of arguments for 'zcard' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return encodeInteger(0)
	}

	ss := val.value.(*sortedSet)

	return encodeInteger(int64(ss.size()))
}

func handleZscoreCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'zscore' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return encodeNull()
	}

	ss := val.value.(*sortedSet)

	score, found := ss.getScore(args[1])
	if !found {
		return encodeNull()
	}

	return encodeBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

func handleZremCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'zrem' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return encodeInteger(0)
	}

	ss := val.value.(*sortedSet)
	found := ss.remove(args[1])
	if !found {
		return encodeInteger(0)
	}

	return encodeInteger(1)
}

func handleSubscribeCommand(args []string, rdb *redisDB, conn *net.Conn) []byte {
	if len(args) < 1 {
		return encodeError("wrong number of arguments for 'subscribe' command")
	}

	count := subscribe(args[0], rdb, conn)

	return encodeArray([]any{"subscribe", args[0], int64(count)})
}

func handlePublishCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'publish' command")
	}

	count := publish(args[0], args[1], rdb)

	return encodeInteger(int64(count))
}

func handleUnsubCommand(args []string, rdb *redisDB, conn *net.Conn) []byte {
	if len(args) < 1 {
		return encodeError("wrong number of arguments for 'unsubscribe' command")
	}

	count := unsubscribe(args[0], rdb, conn)

	return encodeArray([]any{"unsubscribe", args[0], int64(count)})
}

func handleGeoaddCommand(args []string, rdb *redisDB, totalBytes int) []byte {
	if len(args) < 4 {
		return encodeError("wrong number of arguments for 'geoadd' command")
	}

	lon, _ := strconv.ParseFloat(args[1], 64)
	lat, _ := strconv.ParseFloat(args[2], 64)

	if lat > MAX_LATITUDE || lat < MIN_LATITUDE || lon < MIN_LONGITUDE || lon > MAX_LONGITUDE {
		return encodeError(fmt.Sprintf("invalid longitude,latitude pair (%f,%f)", lon, lat))
	}

	key := args[0]
	location := args[3]
	score := convertGeoScore(lat, lon)

	handleZaddCommand([]string{key, score, location}, totalBytes, rdb)

	return encodeInteger(1)
}

func handleGeoposCommnad(args []string, rdb *redisDB) []byte {
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'geopos' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		resp := []any{}
		for i := 0; i < len(args)-1; i++ {
			resp = append(resp, []byte("*-1\r\n"))
		}
		return encodeArray(resp)
	}

	ss := val.value.(*sortedSet)

	resp := []any{}
	for i := 1; i < len(args); i++ {
		score, found := ss.getScore(args[i])
		if !found {
			resp = append(resp, []byte("*-1\r\n"))
			continue
		}

		lat, lon := decodeGeoScore(score)
		resp = append(resp, encodeArray([]any{encodeBulkString(fmt.Sprintf("%f", lon)), encodeBulkString(fmt.Sprintf("%f", lat))}))
	}

	return encodeArray(resp)
}

func handleGeodistCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 3 {
		return encodeError("wrong number of arguments for 'geodist' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return encodeNull()
	}

	ss := val.value.(*sortedSet)
	score1, found1 := ss.getScore(args[1])
	score2, found2 := ss.getScore(args[2])
	if !found1 || !found2 {
		return encodeNull()
	}

	lat1, lon1 := decodeGeoScore(score1)
	lat2, lon2 := decodeGeoScore(score2)

	dist := geohashGetDistance(lon1, lat1, lon2, lat2)

	return encodeBulkString(fmt.Sprintf("%f", dist))
}

func handleGeosearchCommand(args []string, rdb *redisDB) []byte {
	if len(args) < 7 || args[1] != "FROMLONLAT" || args[4] != "BYRADIUS" {
		return encodeError("wrong number of arguments for 'geosearch' command")
	}

	key := args[0]
	val, exists := rdb.data[key]
	if !exists {
		return []byte("*0\r\n")
	}

	ss := val.value.(*sortedSet)
	x, _ := strconv.ParseFloat(args[2], 64)
	y, _ := strconv.ParseFloat(args[3], 64)

	// for each place in ss
	results := make([]string, 0)
	for k := range ss.members {
		score, _ := ss.getScore(k)
		lat, lon := decodeGeoScore(score)
		dist := geohashGetDistance(x, y, lon, lat)

		radius, _ := strconv.ParseFloat(args[5], 64)
		if dist <= radius {
			results = append(results, k)
		}
	}

	anyResults := make([]any, len(results))
	for i, v := range results {
		anyResults[i] = v
	}

	return encodeArray(anyResults)
}

func handleAclCommand(args []string) []byte {
	switch args[0] {
	case "WHOAMI":
		return encodeBulkString("default")

	case "GETUSER":
		if len(args) < 2 {
			return encodeError(("wrong number of arguments for 'GETUSER' command"))
		}

		user := args[1]
		flags := auth.getFlagsForUser(user)
		passwords := auth.getPasswordsForUser(user)
		return encodeArray([]any{encodeBulkString("flags"), encodeArray(flags), encodeBulkString("passwords"), encodeArray(passwords)})

	case "SETUSER":
		if len(args) < 3 {
			return encodeError(("wrong number of arguments for 'SETUSER' command"))
		}

		user := args[1]
		auth.setPasswordForUser(user, args[2])

		return encodeSimpleString("OK")
	default:
		return encodeError(fmt.Sprintf("Unknown args: '%s' after ACL", args[0]))
	}
}

func handleAuthCommand(args []string) []byte {
	if len(args) < 2 {
		return encodeError(("wrong number of arguments for 'AUTH' command"))
	}

	user, pass := args[0], args[1]
	
	if auth.authenticateUser(user, pass) {
		return encodeSimpleString("OK")
	}

	return []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n")
}