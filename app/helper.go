package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

var (
	debugEnabled = os.Getenv("DEBUG") != ""
	debugLogger  = log.New(os.Stderr, "[DEBUG] ", 0)
)

func debug(format string, args ...any) {

	if debugEnabled {
		debugLogger.Printf(format, args...)
	}
}

const (
	RDBStringType    = 0
	RDBListType      = 1
	RDBSetType       = 2
	RDBZSetType      = 3
	RDBHashType      = 4
	RDBZipMapType    = 9
	RDBZipListType   = 10
	RDBIntSetType    = 11
	RDBSortedSetZip  = 12
	RDBHashMapZip    = 13
	RDBListQuick     = 14
	RDBExpireMS      = 252
	RDBExpireSeconds = 253
	RDBSelectDB      = 254
	RDBEOF           = 255
	RDBAux           = 250
	RDBResizeDB      = 251
	RDBBufferSize    = 4096
)

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

	debug("Read RDB file: %d bytes\n", n)

	rdb := rdbFile{
		data: make(map[string]redisValue),
	}
	if len(buf) < 9 || string(buf[:5]) != "REDIS" {
		return rdb, fmt.Errorf("invalid RDB file format")
	}
	buf = buf[9:]

	reader := bytes.NewReader(buf)
	err = parseRDBFile(reader, &rdb)
	if err != nil {
		return rdb, err
	}

	return rdb, nil
}

func parseRDBFile(reader *bytes.Reader, rdb *rdbFile) error {
	var expTime []byte
	for {
		n, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading RDB file: %v", err)
		}

		switch n {
		case RDBAux:
			_, err = readEncodedString(reader)
			if err != nil {
				return fmt.Errorf("error reading RDB AUX: %v", err)
			}
			_, err = readEncodedString(reader)
			if err != nil {
				return fmt.Errorf("error reading RDB AUX value: %v", err)
			}
			debug("Read RDB AUX")
		case RDBSelectDB:
			_, err = reader.ReadByte()
			if err != nil {
				return fmt.Errorf("error reading RDB DB NBR: %v", err)
			}
			debug("Selected RDB DB")
		case RDBResizeDB:
			_, _, err = readEncodedLength(reader)
			if err != nil {
				return fmt.Errorf("error reading Hash Table Size: %v", err)
			}
			_, _, err = readEncodedLength(reader)
			if err != nil {
				return fmt.Errorf("error reading Expire Hash Table Size: %v", err)
			}
			debug("Resized RDB DB")
		case RDBEOF:
			debug("End of RDB file reached")
			return nil
		case RDBExpireSeconds:
			expTime = make([]byte, 4)
			_, err = reader.Read(expTime)
			if err != nil {
				return fmt.Errorf("error reading RDB expire millis: %v", err)
			}
		case RDBExpireMS:
			expTime = make([]byte, 8)
			_, err = reader.Read(expTime)
			if err != nil {
				return fmt.Errorf("error reading RDB expire millis: %v", err)
			}

		default:
			// assuming we are reading key-value pairs now with n as valueType
			debug("Reading key-value pair with type: %x\n", n)
			key, err := readEncodedString(reader)
			if err != nil {
				return fmt.Errorf("error reading key: %v", err)
			}

			value, err := readEncodedString(reader)
			if err != nil {
				return fmt.Errorf("error reading value for key %s: %v", key, err)
			}
			debug("Read key-value pair: %s -> %s\n", key, value)

			if len(expTime) > 0 {
				var expiry int64
				if len(expTime) == 4 {
					expiry = int64(binary.LittleEndian.Uint32(expTime)) - time.Now().UnixMilli()
				} else if len(expTime) == 8 {
					expiry = int64(binary.LittleEndian.Uint64(expTime)) - time.Now().UnixMilli()
				}
				debug("Setting expiry for key %s: %d\n", key, expiry)

				rdb.data[key] = redisValue{
					value:     value,
					valType:   "string",
					createdAt: time.Now().UnixMilli(),
					expiry:    func() int64 { if expiry < 1 { return 1 }; return expiry }(),
				}
			} else {
				rdb.data[key] = redisValue{
					value:     value,
					valType:   "string",
					createdAt: time.Now().UnixMilli(),
					expiry:    0,
				}
			}

		}
	}
}

func readEncodedString(reader *bytes.Reader) (string, error) {
	len, isSpecial, err := readEncodedLength(reader)
	if err != nil {
		return "", fmt.Errorf("error reading encoded string length: %v", err)
	}

	if isSpecial {
		debug("Reading special encoded string of format: %d\n", len)
		switch len {
		case 0:
			len = 1
		case 1:
			len = 2
		case 2:
			len = 4
		default:
			return "", fmt.Errorf("invalid special encoded length: %d", len)
		}
		debug("Special encoded string length: %d bytes\n", len)
	}

	value := make([]byte, len)
	_, err = reader.Read(value)
	if err != nil {
		return "", fmt.Errorf("error reading encoded string: %v", err)
	}

	return string(value), nil
}

func readEncodedLength(reader *bytes.Reader) (int, bool, error) {
	n, err := reader.ReadByte()
	if err != nil {
		return 0, false, fmt.Errorf("error reading encoded length: %v", err)
	}

	switch n >> 6 {
	case 0:
		return int(n & 0x3F), false, nil
	case 1:
		len, err := reader.ReadByte()
		return int(n&0x3F) + int(len), false, err
	case 2:
		len := make([]byte, 4)
		_, err = reader.Read(len)
		return int(binary.BigEndian.Uint32(len)), false, err
	case 3:
		return int(n & 0x3F), true, nil
	}

	return 0, false, fmt.Errorf("invalid encoded length format")
}
