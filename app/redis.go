package main

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	RESPInteger = ':'
	RESPString  = '+'
	RESPBulk    = '$'
	RESPArray   = '*'
	RESPError   = '-'
)

const (
	KeyNotFoundResponse = "$-1\r\n"
)

type redisDB struct {
	data        map[string]redisValue
	dataChan    chan struct{}
	role        string
	replID      string
	mux         *sync.Mutex
	buffer      []string
	replicas    map[string]net.Conn
	offset      int
	ackCnt      int
	ackChan     chan struct{}
	rdbFile     rdbFile
	redisStream redisStream
	connStates  map[string]*connectionState
	stateMux    *sync.RWMutex
	channels    map[net.Conn]*ChannelList
}

type connectionState struct {
	multi    bool
	cmdQueue []redisCommands
	subMode  bool
	isAuth   bool
}

type redisValue struct {
	value     any
	valType   string
	createdAt int64
	expiry    int64
}

type replicationInfo struct {
	role               string
	master_replid      string
	master_repl_offset int
}

type rdbFile struct {
	data map[string]redisValue
}

type redisStream struct {
	data      map[string][]redisStreamEntry
	streamIds map[string]int
}

type redisStreamEntry struct {
	id     string
	fields map[string]string
}

type redisCommands struct {
	cmd  string
	args []string
}

func createRedisDB() *redisDB {
	return &redisDB{
		data:     make(map[string]redisValue),
		dataChan: make(chan struct{}, 10),
		role:     "master",
		replID:   DefaultReplicationID,
		mux:      &sync.Mutex{},
		buffer:   make([]string, 0),
		replicas: make(map[string]net.Conn),
		offset:   0,
		ackCnt:   0,
		ackChan:  make(chan struct{}, 10),
		rdbFile:  rdbFile{data: make(map[string]redisValue)},
		redisStream: redisStream{
			data:      make(map[string][]redisStreamEntry),
			streamIds: make(map[string]int),
		},
		connStates: make(map[string]*connectionState),
		stateMux:   &sync.RWMutex{},
		channels:   make(map[net.Conn]*ChannelList),
	}
}

func (rdb *redisDB) setValue(key string, value any, valType string, createdAt int64, expiry int64) {
	debug("Set key: %s, value: %v, expiry: %d\n", key, value, expiry)
	rdb.mux.Lock()
	defer rdb.mux.Unlock()

	rdb.data[key] = redisValue{
		value:     value,
		valType:   valType,
		createdAt: createdAt,
		expiry:    expiry,
	}
}

func (rdb *redisDB) getValue(key string) (redisValue, string) {
	rdb.mux.Lock()
	defer rdb.mux.Unlock()

	val, exists := rdb.data[key]
	if !exists {
		fmt.Println("Key not found: ", key)
		return redisValue{}, KeyNotFoundResponse
	}

	if rdb.isExpired(val) {
		fmt.Println("Key expired: ", key)
		delete(rdb.data, key)
		return redisValue{}, KeyNotFoundResponse
	}

	timeElapsed := time.Now().UnixMilli() - val.createdAt
	fmt.Printf("GET for key %s, Value: %v, Type: %s, TimeElapsed: %d, CreatedAt: %d, Expiry: %d\n",
		key, val.value, val.valType, timeElapsed, val.createdAt, val.expiry)
	return val, ""
}

func (rdb *redisDB) isExpired(val redisValue) bool {
	if val.expiry <= 0 {
		return false
	}
	timeElapsed := time.Now().UnixMilli() - val.createdAt
	return timeElapsed > val.expiry
}

func (info *replicationInfo) infoResp() []byte {
	roleInfo := "role:" + info.role
	replIDInfo := "master_replid:" + info.master_replid
	offsetInfo := "master_repl_offset:" + strconv.Itoa(info.master_repl_offset)

	content := roleInfo + "\r\n" + replIDInfo + "\r\n" + offsetInfo
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
	fmt.Println("InfoResponse: ", response)

	finalResponse := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	finalResponse = fmt.Sprintf("$%d%s%s%s", len(finalResponse), "\r\n", finalResponse, "\r\n")
	return []byte(finalResponse)
}

func (rdb *redisDB) incrementACK() {
	rdb.mux.Lock()
	rdb.ackCnt++
	rdb.mux.Unlock()
	rdb.ackChan <- struct{}{}
}

func (rdb *redisDB) getAckCnt() int {
	rdb.mux.Lock()
	defer rdb.mux.Unlock()
	return rdb.ackCnt
}

func (rdb *redisDB) setAckCnt(cnt int) {
	rdb.mux.Lock()
	defer rdb.mux.Unlock()
	rdb.ackCnt = cnt
}
