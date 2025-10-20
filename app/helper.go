package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

var (
	debugEnabled = os.Getenv("DEBUG") != ""
	debugLogger  = log.New(os.Stdout, "[DEBUG] ", 0)
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
	MIN_LATITUDE     = -85.05112878
	MAX_LATITUDE     = 85.05112878
	MIN_LONGITUDE    = -180.0
	MAX_LONGITUDE    = 180.0
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
					expiry: func() int64 {
						if expiry < 1 {
							return 1
						}
						return expiry
					}(),
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

// Helper function to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func convertGeoScore(lat, lon float64) string {
	LATITUDE_RANGE := float64(MAX_LATITUDE - MIN_LATITUDE)
	LONGITUDE_RANGE := float64(MAX_LONGITUDE - MIN_LONGITUDE)

	normalized_latitude := math.Pow(2, 26) * (lat - MIN_LATITUDE) / LATITUDE_RANGE
	normalized_longitude := math.Pow(2, 26) * (lon - MIN_LONGITUDE) / LONGITUDE_RANGE

	normalized_latitude_int := int64(normalized_latitude)
	normalized_longitude_int := int64(normalized_longitude)

	score := interleave(normalized_latitude_int, normalized_longitude_int)
	debug("Geo score for lat %f, lon %f: %d\n", lat, lon, score)

	return fmt.Sprintf("%d", score)
}

// Interleaves the bits of two 32-bit integers (lat and lon) into a single 64-bit integer.
func interleave(lat, lon int64) int64 {
	lat = spread_int32_to_int64(lat)
	lon = spread_int32_to_int64(lon)

	// The lon value is then shifted 1 bit to the left
	lon_shifted := lon << 1

	// Next, lat and lon_shifted are combined using a bitwise OR
	return lat | lon_shifted
}

// Spreads a 32-bit integer to a 64-bit integer by inserting 32 zero bits in-between.
func spread_int32_to_int64(num int64) int64 {
	// Ensure only lower 32 bits are non-zero.
	num = num & 0xFFFFFFFF

	// Bitwise operations to spread 32 bits into 64 bits with zeros in-between
	num = (num | (num << 16)) & 0x0000FFFF0000FFFF
	num = (num | (num << 8)) & 0x00FF00FF00FF00FF
	num = (num | (num << 4)) & 0x0F0F0F0F0F0F0F0F
	num = (num | (num << 2)) & 0x3333333333333333
	num = (num | (num << 1)) & 0x5555555555555555

	return num
}

func decodeGeoScore(score float64) (float64, float64) {
	scoreInt := int64(score)

	lon := scoreInt >> 1
	lat := scoreInt

	// Compact both latitude and longitude back to 32-bit integers
	grid_latitude_number := compact_int64_to_int32(lat)
	grid_longitude_number := compact_int64_to_int32(lon)

	LATITUDE_RANGE := float64(MAX_LATITUDE - MIN_LATITUDE)
	LONGITUDE_RANGE := float64(MAX_LONGITUDE - MIN_LONGITUDE)

	// Calculate the grid boundaries
	grid_latitude_min := MIN_LATITUDE + LATITUDE_RANGE * (float64(grid_latitude_number) / (math.Pow(2, 26)))
	grid_latitude_max := MIN_LATITUDE + LATITUDE_RANGE * (float64(grid_latitude_number + 1) / (math.Pow(2, 26)))
	grid_longitude_min := MIN_LONGITUDE + LONGITUDE_RANGE * (float64(grid_longitude_number) / (math.Pow(2, 26)))
	grid_longitude_max := MIN_LONGITUDE + LONGITUDE_RANGE * (float64(grid_longitude_number + 1) / (math.Pow(2, 26)))
    
    // Calculate the center point of the grid cell
    latitude := (grid_latitude_min + grid_latitude_max) / 2
    longitude := (grid_longitude_min + grid_longitude_max) / 2
    return latitude, longitude
}

func compact_int64_to_int32(v int64) int64 {
	// Keep only the bits in even positions
    v = v & 0x5555555555555555

    // Reverse the spreading process by shifting and masking
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    
    return v
}