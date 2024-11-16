package main

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

// RedisDB is a simple in-memory key-value store
type redisDB struct {
	data        map[string]redisValue
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
	multi       bool
}

type redisValue struct {
	value     string
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

func (rdb *redisDB) setValue(key string, value string, valType string, createdAt int64, expiry int64) {
	fmt.Printf("Set key: %s, value: %s, expiry: %d\n", key, value, expiry)
	rdb.mux.Lock()
	rdb.data[key] = redisValue{
		value:     value,
		valType:   valType,
		createdAt: createdAt,
		expiry:    expiry,
	}
	rdb.mux.Unlock()
}

func (rdb *redisDB) getValue(key string) (redisValue, string) {
	val, ok := rdb.data[key]
	if !ok {
		fmt.Println("Key not found: ", key)
		return redisValue{}, "$-1\r\n"
	}
	timeElapsed := time.Now().UnixMilli() - val.createdAt
	fmt.Printf("GET for key %s, Value : %s, Type : %s, TimeElapsed : %d, CreatedAt : %d, Expiry : %d\n", key, val.value, val.valType, timeElapsed, val.createdAt, val.expiry)
	if val.expiry > 0 && timeElapsed > val.expiry {
		fmt.Println("Key expired: ", key)
		delete(rdb.data, key)
		return redisValue{}, "$-1\r\n"
	}
	return val, ""
}

func (info *replicationInfo) infoResp() []byte {
	l1 := "role:" + info.role
	l2 := "master_replid:" + info.master_replid
	l3 := "master_repl_offset:" + strconv.Itoa(info.master_repl_offset)
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(l1)+len(l2)+len(l3), l1+"\r\n"+l2+"\r\n"+l3)
	fmt.Println("InfoResponse: ", resp)

	resp = fmt.Sprintf("$%d\r\n%s\r\n", len(resp), resp)
	resp = fmt.Sprintf("$%d%s%s%s", len(resp), "\r\n", resp, "\r\n")
	return []byte(resp)
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

func (rdb *redisDB) setMulti(value bool) {
	rdb.mux.Lock()
	rdb.multi = value
	rdb.mux.Unlock()
}
