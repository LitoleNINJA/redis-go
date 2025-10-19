package main

import "slices"

func subscribe(ch string, rdb *redisDB) int {
	if !slices.Contains(rdb.channels, ch) {
		rdb.channels = append(rdb.channels, ch)
	}

	return len(rdb.channels)
}
