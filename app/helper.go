package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// handle multiple commands at once and add them to buffer
func addCommandToBuffer(buf string, n int) {
	prev := 0
	for i := 0; i < n; i++ {
		if buf[i] == '*' && unicode.IsDigit(rune(buf[i+1])) {
			str := buf[prev:i]
			if len(str) > 1 && str[0] != '*' {
				a := strings.Split(str, "$")
				a[1] = "$" + a[1]
				for _, s := range a {
					if len(s) > 0 {
						rdb.buffer = append(rdb.buffer, s)
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

// parse command from buffer and return command and args
func parseCommand(buf string) (string, []string, int) {
	a := strings.Split(buf, "\r\n")
	// for local testing
	if len(a) == 1 {
		a = strings.Split(buf, "\\r\\n")
	}
	n, _ := strconv.ParseInt(string(a[0][1]), 10, 64)

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
	return cmd, args, len(buf)
}

// start handshake with master
func handleHandshake(masterIp, masterPort string) (net.Conn, error) {
	conn, err := net.Dial("tcp", masterIp+":"+masterPort)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return nil, err
	}

	err = pingMaster(conn)
	if err != nil {
		fmt.Println("Error pinging master: ", err.Error())
		return nil, err
	}

	err = sendREPLConf(conn, "listening-port", *port)
	if err != nil {
		fmt.Println("Error sending REPLCONF 1: ", err.Error())
		return nil, err
	}
	err = sendREPLConf(conn, "capa", "psync2")
	if err != nil {
		fmt.Println("Error sending REPLCONF 2: ", err.Error())
		return nil, err
	}

	err = sendPSYNC(conn, "?", -1)
	if err != nil {
		fmt.Println("Error sending PSYNC: ", err.Error())
		return nil, err
	}

	return conn, nil
}

// ping master from slave
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

// send REPLCONF to master
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

// send PSYNC to master
func sendPSYNC(conn net.Conn, replId string, offset int) error {
	// PSYNC to master
	offset_str := strconv.Itoa(offset)
	_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(replId), replId, len(offset_str), offset_str)))
	if err != nil {
		fmt.Println("Error writing to master: ", err.Error())
		return err
	}
	return nil
}

// migrate commands to slaves
func migrateToSlaves(key, value string) {
	for _, conn := range rdb.replicas {
		res := []byte(fmt.Sprintf("*3\r\n$3\r\nset\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
		_, err := conn.Write(res)
		if err != nil {
			fmt.Println("Error writing to replica: ", err.Error())
		}
		fmt.Printf("\nSent Migration: %s to %s\n", printCommand(res), conn.RemoteAddr())
	}
}

// ask for ACK from slaves (for debugging)
func getACK() {
	for _, conn := range rdb.replicas {
		res := []byte("*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
		_, err := conn.Write(res)
		fmt.Printf("Sent: %s\n", printCommand(res))
		if err != nil {
			fmt.Println("Error writing to slave: ", err.Error())
		}
		fmt.Printf("\nGet ACK from %s\n", conn.RemoteAddr())
	}
}

// print command for debugging
func printCommand(res []byte) string {
	cmd := string(res)
	cmd = strings.ReplaceAll(cmd, "\n", "\\n")
	cmd = strings.ReplaceAll(cmd, "\r", "\\r")
	return cmd
}

// read RDB file and return data
func readRDBFile(dir string, fileName string) (rdbFile, error) {
	file, err := os.Open(dir + "/" + fileName)
	if err != nil {
		fmt.Println("Error opening RDB file: ", err.Error())
		return rdbFile{}, err
	}
	defer file.Close()

	buf := make([]byte, 4096)
	n, err := file.Read(buf)
	if err != nil {
		fmt.Println("Error reading RDB file: ", err.Error())
		return rdbFile{}, err
	}

	var keys []byte
	for i := 0; i < n; i++ {
		if i+2 < n && buf[i] == 254 && buf[i+1] == 0 && buf[i+2] == 251 {
			keys = buf[i+3:]
			break
		}
	}
	if len(keys) == 0 {
		fmt.Println("No keys found in RDB file")
		return rdbFile{}, nil
	}
	keys = keys[2:]

	var rdbfile rdbFile
	rdbfile.data = make(map[string]redisValue)
	var exp int64 = 0
	for i := 0; i < len(keys); {
		// check if value type is string
		if keys[i] == 0 {
			keyLength := int(keys[i+1])
			if keyLength == 0 {
				break
			}
			key := string(keys[i+2 : i+2+keyLength])
			i += 2 + keyLength

			valLength := int(keys[i])
			value := string(keys[i+1 : i+1+valLength])
			i += 1 + valLength

			if exp == 0 {
				rdbfile.data[key] = redisValue{value: value, createdAt: time.Now().UnixMilli(), expiry: 0}
			} else {
				rdbfile.data[key] = redisValue{value: value, createdAt: exp, expiry: 1}
			}
			exp = 0
		} else if keys[i] == 252 {
			t := keys[i+1 : i+9]
			exp = int64(binary.LittleEndian.Uint64(t))
			i += 9
		} else if keys[i] == 253 {
			t := keys[i+1 : i+5]
			exp = int64(binary.LittleEndian.Uint32(t)) * 1000
			i += 5
		} else {
			i++
		}
	}
	return rdbfile, nil
}

// validate stream ID
func validateStreamID(id string) string {
	if id == "0-0" {
		return "The ID specified in XADD must be greater than 0-0"
	}
	if id <= lastStreamID {
		return "The ID specified in XADD is equal or smaller than the target stream top item"
	}
	return ""
}

// handle non-blocking XADD command
func handleXRead(args []string) []struct {
	key   string
	entry []redisStreamEntry
} {
	pos := 1
	for i := 0; i < len(args); i++ {
		if strings.Contains(args[i], "-") {
			pos = i
			break
		}
	}
	keys := args[:pos]
	ids := args[pos:]
	fmt.Println("Keys: ", keys, " IDs: ", ids)
	entries := make([]struct {
		key   string
		entry []redisStreamEntry
	}, 0)
	for i := 0; i < len(keys); i++ {
		for _, v := range rdb.redisStream.data[keys[i]] {
			if v.id > ids[i] {
				entries = append(entries, struct {
					key   string
					entry []redisStreamEntry
				}{key: keys[i], entry: []redisStreamEntry{v}})
			}
		}
	}

	fmt.Println("Entries: ", entries)
	return entries
}

// handle block XREAD command
func handleBlockXRead(args []string, conn net.Conn) {
	duration, _ := strconv.Atoi(args[0])
	fmt.Println("Blocking XREAD for ", duration, "ms")
	time.Sleep(time.Millisecond * time.Duration(duration))

	entries := handleXRead(args[2:])
	var res []byte
	if len(entries) > 0 {
		resString := fmt.Sprintf("*%d\r\n", len(entries))
		for _, entry := range entries {
			resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*1\r\n", len(entry.key), entry.key)
			for _, entry := range entry.entry {
				resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.id), entry.id)
				for k, v := range entry.fields {
					resString += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
				}
			}
		}
		res = []byte(resString)
	} else {
		res = []byte("$-1\r\n")
	}

	fmt.Printf("Sent: %s\n", printCommand(res))
	conn.Write(res)
}
