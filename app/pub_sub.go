package main

import (
	"net"
)

type ChannelList struct {
	channels []string
	subCount int
}

func subscribe(ch string, rdb *redisDB, conn *net.Conn) int {
	subscribedChannels, exists := rdb.channels[*conn]
	if !exists {
		subscribedChannels = ChannelList{
			channels: make([]string, 0),
			subCount: 0,
		}
		rdb.channels[*conn] = subscribedChannels
	}

	found := false
	for _, channel := range subscribedChannels.channels {
		if channel == ch {
			found = true
			break
		}
	}

	if !found {
		subscribedChannels.channels = append(subscribedChannels.channels, ch)
		subscribedChannels.subCount++

		rdb.channels[*conn] = subscribedChannels
	}

	return subscribedChannels.subCount
}
