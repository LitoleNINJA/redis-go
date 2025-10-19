package main

func subscribe(ch string, rdb *redisDB) int {
	exists := false
	for _, channel := range rdb.channels {
		if channel == ch {
			exists = true
			break
		}
	}

	if !exists {
		rdb.channels = append(rdb.channels, ch)
	}

	return len(rdb.channels)
}
