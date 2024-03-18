package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// handle multiple commands at once and add them to buffer
func addCommandToBuffer(buf string) {
	a := strings.Split(buf, "*")
	for i := 0; i < len(a); i++ {
		if len(a[i]) > 0 {
			rdb.buffer = append(rdb.buffer, a[i])
		}
	}
	// fmt.Printf("Buffer: %s", rdb.buffer)
}

// parse command from buffer and return command and args
func parseCommand(buf string) (string, []string) {
	a := strings.Split(buf, "\r\n")
	// for local testing
	// if len(a) == 1 {
	// 	a = strings.Split(buf, "\\r\\n")
	// }
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

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		return err
	}
	fmt.Printf("Received: %s\n", buf[:n])
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
		fmt.Printf("Sent Migration: %s to %s\n", string(res), conn.RemoteAddr())
	}
}

// ask for ACK from slaves
func getACK() {
	for _, conn := range rdb.replicas {
		res := []byte("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n")
		_, err := conn.Write(res)
		if err != nil {
			fmt.Println("Error writing to slave: ", err.Error())
		}
		fmt.Printf("\nGet ACK from %s\n", conn.RemoteAddr())
	}
}
