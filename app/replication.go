package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"
)

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

func migrateToSlaves(key string, value any, rdb *redisDB) {
	valueStr := fmt.Sprintf("%v", value)
	message := fmt.Sprintf("*3\r\n$3\r\nset\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(valueStr), valueStr)
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

func startReplication(masterIP, masterPort string, rdb *redisDB) {
	fmt.Printf("Replica of: %s:%s\n", masterIP, masterPort)
	conn, err := handleHandshake(masterIP, masterPort)
	if err != nil {
		fmt.Println("Error during handshake: ", err.Error())
		return
	}
	go handleConnection(conn, rdb)
}

// Replication-related functions
func handleReplConfCommand(args []string, rdb *redisDB) []byte {
	if rdb.role == "master" {
		if args[0] == "ack" {
			fmt.Println("Received ACK from slave")
			rdb.incrementACK()
			fmt.Println("Ack count: ", rdb.getAckCnt())
			return []byte("")
		}
		return []byte("+OK\r\n")
	}

	offsetStr := strconv.Itoa(rdb.offset)
	response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)
	fmt.Println("Sending ACK to master")
	handshakeComplete = true
	return []byte(response)
}

func handlePSyncCommand(conn net.Conn, rdb *redisDB) []byte {
	if rdb.role != "master" {
		return []byte("-ERR not a master\r\n")
	}

	response := []byte("+FULLRESYNC " + DefaultReplicationID + " 0\r\n")
	fmt.Printf("Sent: %s\n", printCommand(response))
	conn.Write(response)

	emptyRdbFile, err := hex.DecodeString(EmptyRDBFileHex)
	if err != nil {
		fmt.Println("Error decoding RDB file: ", err.Error())
		return []byte("")
	}

	rdbResponse := []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRdbFile), emptyRdbFile))
	conn.Write(rdbResponse)
	fmt.Printf("Sent: %s\n", printCommand(rdbResponse))

	rdb.replicas[conn.RemoteAddr().String()] = conn
	return []byte("")
}

func handleWaitCommand(args []string, rdb *redisDB) []byte {
	if rdb.offset == 0 {
		fmt.Println("Master has not propagated any commands")
		return []byte(fmt.Sprintf(":%d\r\n", len(rdb.replicas)))
	}

	minRepCnt, _ := strconv.Atoi(args[0])
	timeout, _ := strconv.Atoi(args[1])
	endTime := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	ticker := time.NewTicker(TickerInterval)
	defer ticker.Stop()

	rdb.setAckCnt(0)
	getACK(rdb)

	for {
		select {
		case <-rdb.ackChan:
			if rdb.getAckCnt() >= minRepCnt {
				return []byte(fmt.Sprintf(":%d\r\n", rdb.getAckCnt()))
			}
		case <-ticker.C:
			if time.Now().After(endTime) {
				return []byte(fmt.Sprintf(":%d\r\n", rdb.getAckCnt()))
			}
		}
	}
}
