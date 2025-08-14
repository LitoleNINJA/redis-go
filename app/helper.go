package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	BufferSize        = 1024
	RetryInterval     = 100 * time.Millisecond
	DefaultRetryCount = 50
	RDBBufferSize     = 4096
)

const (
	// Value types
	RDBStringType   = 0
	RDBListType     = 1
	RDBSetType      = 2
	RDBZSetType     = 3
	RDBHashType     = 4
	RDBZipMapType   = 9
	RDBZipListType  = 10
	RDBIntSetType   = 11
	RDBSortedSetZip = 12
	RDBHashMapZip   = 13
	RDBListQuick    = 14

	// Special types
	RDBExpireMS      = 252 // FC
	RDBExpireSeconds = 253 // FD
	RDBSelectDB      = 254 // FE
	RDBEOF           = 255 // FF

	// Auxiliary field
	RDBAux = 250 // FA

	// Resizedb
	RDBResizeDB = 251 // FB
)

type XReadEntry struct {
	key   string
	entry []redisStreamEntry
}

func addCommandToBuffer(buf string, n int, rdb *redisDB) {
	prev := 0
	for i := 0; i < n; i++ {
		if buf[i] == '*' && unicode.IsDigit(rune(buf[i+1])) {
			str := buf[prev:i]
			if len(str) > 1 && str[0] != '*' {
				parts := strings.Split(str, "$")
				parts[1] = "$" + parts[1]
				for _, part := range parts {
					if len(part) > 0 {
						rdb.buffer = append(rdb.buffer, part)
					}
				}
			} else if len(str) > 1 {
				rdb.buffer = append(rdb.buffer, str)
			}
			prev = i
		}
	}
	rdb.buffer = append(rdb.buffer, buf[prev:n])
	fmt.Printf("Buffer: %s\n", printCommand([]byte(strings.Join(rdb.buffer, ", "))))
}

func parseCommand(buf string) (string, []string, int) {
	parts := strings.Split(buf, "\r\n")
	if len(parts) == 1 {
		parts = strings.Split(buf, "\\r\\n")
	}

	argCount, _ := strconv.ParseInt(string(parts[0][1]), 10, 64)

	var cmd string
	args := make([]string, 0)

	for i := 0; i < int(argCount); i++ {
		pos := 2*i + 1
		if len(parts[i]) == 0 {
			continue
		}
		if parts[pos][0] == '$' {
			if cmd == "" {
				cmd = strings.ToLower(parts[pos+1])
			} else {
				args = append(args, strings.ToLower(parts[pos+1]))
			}
		}
	}

	fmt.Printf("Command: %s, Args: %v\n", cmd, args)
	return cmd, args, len(buf)
}

func handleHandshake(masterIP, masterPort string) (net.Conn, error) {
	conn, err := net.Dial("tcp", masterIP+":"+masterPort)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return nil, err
	}

	if err := pingMaster(conn); err != nil {
		fmt.Println("Error pinging master: ", err.Error())
		return nil, err
	}

	if err := sendREPLConf(conn, "listening-port", *port); err != nil {
		fmt.Println("Error sending REPLCONF 1: ", err.Error())
		return nil, err
	}

	if err := sendREPLConf(conn, "capa", "psync2"); err != nil {
		fmt.Println("Error sending REPLCONF 2: ", err.Error())
		return nil, err
	}

	if err := sendPSYNC(conn, "?", -1); err != nil {
		fmt.Println("Error sending PSYNC: ", err.Error())
		return nil, err
	}

	return conn, nil
}

func pingMaster(conn net.Conn) error {
	_, err := conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}

	response, err := readResponse(conn)
	if err != nil {
		return err
	}

	fmt.Printf("Received: %s\n", response)
	if string(response) != "+PONG\r\n" {
		return fmt.Errorf("master did not respond with PONG: %s", string(response))
	}
	return nil
}

func sendREPLConf(conn net.Conn, cmd, args string) error {
	message := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd), cmd, len(args), args)
	_, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}

	response, err := readResponse(conn)
	if err != nil {
		return err
	}

	fmt.Printf("Received: %s\n", response)
	if string(response) != "+OK\r\n" {
		return fmt.Errorf("master did not respond with OK: %s", string(response))
	}
	return nil
}

func sendPSYNC(conn net.Conn, replID string, offset int) error {
	offsetStr := strconv.Itoa(offset)
	message := fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(replID), replID, len(offsetStr), offsetStr)
	_, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}
	return nil
}

func readResponse(conn net.Conn) ([]byte, error) {
	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		return nil, err
	}
	return buf[:n], nil
}

func migrateToSlaves(key, value string, rdb *redisDB) {
	message := fmt.Sprintf("*3\r\n$3\r\nset\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	messageBytes := []byte(message)

	for _, conn := range rdb.replicas {
		_, err := conn.Write(messageBytes)
		if err != nil {
			fmt.Println("Error writing to replica: ", err.Error())
			continue
		}
		fmt.Printf("\nSent Migration: %s to %s\n", printCommand(messageBytes), conn.RemoteAddr())
	}
}

func getACK(rdb *redisDB) {
	ackMessage := []byte("*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")

	for _, conn := range rdb.replicas {
		_, err := conn.Write(ackMessage)
		if err != nil {
			fmt.Println("Error writing to slave: ", err.Error())
			continue
		}
		fmt.Printf("Sent: %s\n", printCommand(ackMessage))
		fmt.Printf("\nGet ACK from %s\n", conn.RemoteAddr())
	}
}

func printCommand(res []byte) string {
	cmd := string(res)
	cmd = strings.ReplaceAll(cmd, "\n", "\\n")
	cmd = strings.ReplaceAll(cmd, "\r", "\\r")
	return cmd
}

func readRDBFile(dir string, fileName string) (rdbFile, error) {
	file, err := os.Open(dir + "/" + fileName)
	if err != nil {
		return rdbFile{}, err
	}
	defer file.Close()

	buf := make([]byte, RDBBufferSize)
	n, err := file.Read(buf)
	if err != nil {
		return rdbFile{}, err
	}

	fmt.Printf("Read RDB file: %d bytes\n", n)
	
	var rdb rdbFile
	if len(buf) < 9 || string(buf[:5]) != "REDIS" {
		return rdb, fmt.Errorf("invalid RDB file format")
	}
	buf = buf[9:] // Skip the first 9 bytes (MAGIC + VERSION)

	reader := bytes.NewReader(buf)
	err = parseRDBFile(reader, &rdb)
	if err != nil {
		return rdb, err
	}

	return rdb, nil
}

func parseRDBFile(reader *bytes.Reader, rdb *rdbFile) error {
	n, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading RDB file: %v", err)
	}

	switch n {
	case RDBAux:
		_, err = readEncodedString(reader)

	}

	return nil
}

func readEncodedString(reader *bytes.Reader) ([]byte, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading run length: %v", err)
	}

	if length == 0 {
		fmt.Println("Run length is zero, returning empty slice")
		return []byte{}, nil
	}

	data := make([]byte, length)
	_, err = reader.Read(data)
	if err != nil {
		return nil, fmt.Errorf("error reading run length data: %v", err)
	}		

	return data, nil
}

func createRedisValue(value string, expiry int64) redisValue {
	if expiry == 0 {
		return redisValue{
			value:     value,
			createdAt: time.Now().UnixMilli(),
			expiry:    0,
		}
	}
	return redisValue{
		value:     value,
		createdAt: expiry,
		expiry:    1,
	}
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
	return []byte("$-1\r\n")
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

func setKeyValue(key string, value string, exp int64, totalBytes int, rdb *redisDB) []byte {
	valueType := determineValueType(value)
	rdb.setValue(key, value, valueType, time.Now().UnixMilli(), exp)

	if rdb.role == "master" {
		rdb.offset += totalBytes
		migrateToSlaves(key, value, rdb)
		return []byte("+OK\r\n")
	}

	fmt.Println("Slave received set command: ", key, value, exp)
	return []byte("")
}

func determineValueType(value string) string {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "int"
	}
	return "string"
}
