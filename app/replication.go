package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"
)

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

func startReplication(masterIP, masterPort string, rdb *redisDB) {
	fmt.Printf("Replica of: %s:%s\n", masterIP, masterPort)
	conn, err := handleHandshake(masterIP, masterPort)
	if err != nil {
		fmt.Println("Error during handshake: ", err.Error())
		return
	}
	go handleConnection(conn, rdb)
}
