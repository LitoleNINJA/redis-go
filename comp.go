package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const respMsg = "+PONG\r\n"
const crlf = "\r\n"

var port = flag.String("port", "6379", "Port to listen on")
var replicaOf = flag.String("replicaof", "", "replica of")

type RedisServer struct {
	Role        string
	Port        string
	ReplicaAddr string
	ReplicaPort string
	Entries     map[string]*Item
	MasterInfo  *MasterInfo
}

type MasterInfo struct {
	ReplId     string
	ReplOffset int
}

func NewRedis(role, port, rAddr, rPort string) *RedisServer {
	mInfo := &MasterInfo{
		ReplId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		ReplOffset: 0,
	}
	return &RedisServer{
		Role:        role,
		Port:        port,
		ReplicaAddr: rAddr,
		ReplicaPort: rPort,
		Entries:     make(map[string]*Item),
		MasterInfo:  mInfo,
	}
}
func (s *RedisServer) Server() {
	l, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			os.Exit(1)
		}
		go s.handleConn(conn)
	}
}
func (s *RedisServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		req := make([]byte, 1024)
		_, err := conn.Read(req)
		if err != nil {
			return
		}
		fmt.Println("buf=", string(req), "----ending buf---")
		crlfSplit := strings.Split(string(req), "\\r\\n")
		fmt.Println("crlfSplit=", crlfSplit, "len=", len(crlfSplit), "-----ending crlfSplit-------")
		op := crlfSplit[2]
		switch strings.ToLower(op) {
		case "echo":
			raw := crlfSplit[4]
			res := buildBulkStr(raw)
			fmt.Println("my_resp=", string(res), "---end resp")
			conn.Write(res)
		case "set":
			k, v := crlfSplit[4], crlfSplit[6]
			it := &Item{V: v}
			if len(crlfSplit) > 8 && crlfSplit[8] == "px" {
				tLen, _ := strconv.Atoi(crlfSplit[10])
				exTime := time.Now().Add(time.Duration(tLen) * time.Millisecond)
				it.exTime = &exTime
			}
			s.Entries[k] = it
			conn.Write([]byte("+OK\r\n"))
		case "get":
			k := crlfSplit[4]
			v, exist := s.Entries[k]
			if !exist {
				_, _ = conn.Write([]byte("$-1\r\n"))
				continue
			}
			if v.exTime != nil && time.Now().After(*v.exTime) {
				_, _ = conn.Write([]byte("$-1\r\n"))
				continue
			}
			res := buildBulkStr(v.V)
			fmt.Println("my_resp=", v, "---end resp")
			conn.Write(res)
			continue
		case "ping":
			fmt.Println("ping step")
			_, _ = conn.Write([]byte(respMsg))
			continue
		case "info":
			// fmt.Println("info step=", string(s.ServerInfo()), "info step=")
			if s.Role == "slave" {
				_, _ = conn.Write(buildBulkStrF("role:%s", "slave"))
				continue
			}
			bs := string(s.ServerInfo())
			fmt.Println("info step=", bs, len(bs))
			conn.Write(buildBulkStrF("$%d\r\n%s\r\n", len(bs), bs))
			continue
		default:
			_, _ = conn.Write([]byte(respMsg))
		}
	}
}
func (s *RedisServer) ServerInfo() []byte {
	b := bytes.Buffer{}
	l1 := "role:" + s.Role
	b.WriteString(l1 + crlf)
	l2 := "master_replid:" + s.MasterInfo.ReplId
	b.WriteString(l2 + crlf)
	l3 := "master_repl_offset:" + strconv.Itoa(s.MasterInfo.ReplOffset)
	b.WriteString(l3 + crlf)
	return append([]byte("$"+strconv.Itoa(len(l1)+len(l2)+len(l3))+crlf), b.Bytes()...)
}

func main() {
	flag.Parse()
	role := "master"
	if replicaOf != nil && len(*replicaOf) > 0 {
		fmt.Println("replicaOf=", *replicaOf, "=replicaOf")
		role = "slave"
	}
	s := NewRedis(role, *port, "", "")
	s.Server()
}
func buildBulkStr(raw string) []byte {
	b := bytes.Buffer{}
	b.WriteString(fmt.Sprintf("$%d%s%s%s", len(raw), crlf, raw, crlf))
	return b.Bytes()
}
func buildBulkStrF(format string, args ...any) []byte {
	raw := fmt.Sprintf(format, args...)
	fmt.Println("raw : ", raw, "len=", len(raw))
	b := bytes.Buffer{}
	b.WriteString(fmt.Sprintf("$%d%s%s%s", len(raw), crlf, raw, crlf))
	fmt.Println("final : ", fmt.Sprintf("$%d%s%s%s", len(raw), crlf, raw, crlf))
	return b.Bytes()
}

type Item struct {
	V      string
	exTime *time.Time
}
