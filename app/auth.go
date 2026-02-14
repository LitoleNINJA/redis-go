package main

import (
	"crypto/sha256"
	"fmt"
)

type AuthValues struct {
	noPass   bool
	password string
}

func getFlagsForUser(user string, rdb *redisDB) []any {
	val, ok := rdb.auth[user]; 
	if !ok {
		return []any{}
	}	

	flags := make([]any, 0)
	if val.noPass {
		flags = append(flags, "nopass")
	}
	return flags
}

func getPasswordsForUser(user string, rdb *redisDB) []any {
	val, ok := rdb.auth[user]; 
	if !ok {
		return []any{}
	}	

	pass := make([]any, 0)
	if !val.noPass {
		pass = append(pass, val.password)
	}
	return pass
}

func setPasswordForUser(user string, password string, rdb *redisDB) {
	val, ok := rdb.auth[user]; 
	if !ok {
		return 
	}	
	
	hashedPassword := sha256hash(password)
	val.password = hashedPassword
	val.noPass = false

	rdb.auth[user] = val
}

func sha256hash(password string) string {
	hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hash)
}
