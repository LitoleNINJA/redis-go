package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Redis response encoding functions
func encodeSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func encodeBulkString(s string) []byte {
	if s == "" {
		return []byte("$-1\r\n") // null bulk string
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func encodeInteger(i int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

func encodeArray(elements []any) []byte {
	result := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		switch v := element.(type) {
		case string:
			result += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		case int:
			result += fmt.Sprintf(":%d\r\n", v)
		case int64:
			result += fmt.Sprintf(":%d\r\n", v)
		case []byte:
			result += string(v)
		default:
			result += fmt.Sprintf("$%d\r\n%v\r\n", len(fmt.Sprintf("%v", v)), v)
		}
	}

	debug("Encoded array: %s", result)
	return []byte(result)
}

func encodeError(msg string) []byte {
	return []byte(fmt.Sprintf("-ERR %s\r\n", msg))
}

func encodeNull() []byte {
	return []byte("$-1\r\n")
}

type XReadEntry struct {
	key   string
	entry []redisStreamEntry
}

func updateSlaveOffset(cmd string, totalBytes int, rdb *redisDB) {
	if rdb.role == "slave" && handshakeComplete {
		rdb.offset += totalBytes
		fmt.Printf("\nCmd: %s,  Current Bytes: %d,  Bytes processed: %d\n", cmd, totalBytes, rdb.offset)
	}
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

func handleXRead(args []string, rdb *redisDB) []XReadEntry {
	keyEndPos := findKeyEndPosition(args)
	keys := args[:keyEndPos]
	ids := args[keyEndPos:]

	fmt.Println("Keys: ", keys, " IDs: ", ids)

	entries := make([]XReadEntry, 0)
	for i := 0; i < len(keys); i++ {
		streamEntries := getStreamEntriesAfterID(rdb, keys[i], ids[i])
		if len(streamEntries) > 0 {
			entries = append(entries, XReadEntry{
				key:   keys[i],
				entry: streamEntries,
			})
		}
	}

	fmt.Println("Entries: ", entries)
	return entries
}

func calculateRetryCount(duration int) int {
	if duration != 0 {
		return 1
	}
	return DefaultRetryCount
}

func performBlockingRead(args []string, retryCount int, rdb *redisDB) []byte {
	for i := 0; i < retryCount; i++ {
		entries := handleXRead(args, rdb)
		if len(entries) > 0 {
			return buildXReadResponse(entries)
		}
		if i < retryCount-1 {
			time.Sleep(RetryInterval)
		}
	}
	return []byte("*-1\r\n")
}

func buildXReadResponse(entries []XReadEntry) []byte {
	response := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*1\r\n", len(entry.key), entry.key)
		for _, streamEntry := range entry.entry {
			response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(streamEntry.id), streamEntry.id)
			for k, v := range streamEntry.fields {
				response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
			}
		}
	}
	return []byte(response)
}

func determineValueType(value any) string {
	switch v := value.(type) {
	case string:
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return "int"
		}
		return "string"
	case int, int64, int32:
		return "int"
	case []string:
		return "list"
	case map[string]string:
		return "hash"
	case *sortedSet:
		return "zset"
	default:
		return "string"
	}
}

func findKeyEndPosition(args []string) int {
	for i, arg := range args {
		if strings.Contains(arg, "-") {
			return i
		}
	}
	return 1
}

func getStreamEntriesAfterID(rdb *redisDB, key, afterID string) []redisStreamEntry {
	var entries []redisStreamEntry
	for _, entry := range rdb.redisStream.data[key] {
		if entry.id > afterID {
			entries = append(entries, entry)
		}
	}
	return entries
}

func validateStreamID(id string) string {
	if id == "0-0" {
		return "The ID specified in XADD must be greater than 0-0"
	}
	if id <= lastStreamID {
		return "The ID specified in XADD is equal or smaller than the target stream top item"
	}
	return ""
}

func setKeyValue(key string, value any, exp int64, totalBytes int, rdb *redisDB) []byte {
	valueType := determineValueType(value)
	rdb.setValue(key, value, valueType, time.Now().UnixMilli(), exp)

	// Notify BLPOP waiters when a list changes (non-blocking)
	if valueType == "list" && rdb.dataChan != nil {
		select {
		case rdb.dataChan <- struct{}{}:
		default:
		}
	}

	if rdb.role == "master" {
		rdb.offset += totalBytes
		migrateToSlaves(key, value, rdb)
		return encodeSimpleString("OK")
	}

	fmt.Println("Slave received set command: ", key, value, exp)
	return []byte("")
}

func handleBlockXRead(args []string, conn net.Conn, rdb *redisDB) {
	duration, _ := strconv.Atoi(args[0])
	retryCount := calculateRetryCount(duration)

	if duration != 0 {
		fmt.Println("Blocking XREAD for ", duration, "ms")
		time.Sleep(time.Millisecond * time.Duration(duration))
	} else {
		fmt.Println("Blocking XREAD till new data arrives")
	}

	if args[3] == "$" {
		args[3] = lastStreamID
	}

	response := performBlockingRead(args[2:], retryCount, rdb)
	fmt.Printf("Sent: %s\n", printCommand(response))
	conn.Write(response)
}

func handleBlockPop(key string, timeStr string, rdb *redisDB) []byte {
	timeout, _ := strconv.ParseFloat(timeStr, 64)
	duration := time.Duration(timeout*1000) * time.Millisecond
	endTime := time.Now().Add(duration)

	ticker := time.NewTicker(TickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rdb.dataChan:
			debug("BLPOP: Key %s notified of data change, rechecking...\n", key)
			val, exists := rdb.data[key]
			if !exists || val.valType != "list" {
				continue
			}
			list := val.value.([]string)
			if len(list) == 0 {
				continue
			}

			elem := list[0]
			rdb.setValue(key, list[1:], "list", time.Now().UnixMilli(), 0)
			return encodeArray([]any{key, elem})
		case <-ticker.C:
			if timeout != 0 && time.Now().After(endTime) {
				debug("Timeout reached, returning null\n")
				return []byte("*-1\r\n")
			}
		}
	}
}
