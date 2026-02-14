package main

import (
	"crypto/sha256"
	"fmt"
)

type Auth map[string]AuthValues

type AuthValues struct {
	noPass   bool
	password string
}

func createAuth() Auth {
	return Auth{
		"default": {noPass: true},
	}
}

func (auth Auth) getFlagsForUser(user string) []any {
	val, ok := auth[user]; 
	if !ok {
		return []any{}
	}	

	flags := make([]any, 0)
	if val.noPass {
		flags = append(flags, "nopass")
	}
	return flags
}

func (auth Auth) getPasswordsForUser(user string) []any {
	val, ok := auth[user]; 
	if !ok {
		return []any{}
	}	

	pass := make([]any, 0)
	if !val.noPass {
		pass = append(pass, val.password)
	}
	return pass
}

func (auth Auth) setPasswordForUser(user string, password string) {
	val, ok := auth[user]; 
	if !ok {
		return 
	}	

	if len(password) == 0 || password[0] != '>' {
		return
	}

	password = password[1:]
	
	hashedPassword := sha256hash(password)
	val.password = hashedPassword
	val.noPass = false

	auth[user] = val
}

func (auth Auth) authenticateUser(user string, pass string) bool {
	val, ok := auth[user]
	if !ok {
		return false
	}

	if val.noPass {
		return true
	}

	hasedPass := sha256hash(pass)
	storedPass := val.password

	return hasedPass == storedPass
}

func (auth Auth) isNoPass(user string) bool {
	val, ok := auth[user]
	if !ok {
		return false
	}

	return val.noPass
}

func sha256hash(password string) string {
	hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hash)
}
