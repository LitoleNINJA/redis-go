package main

import (
	"fmt"
	"net"
)

const (
	BufferedChannelSize = 100
)

type ChannelList struct {
	channels []string
	subCount int
	msgChan  chan []byte
	done     chan bool
}

func subscribe(ch string, rdb *redisDB, conn *net.Conn) int {
	// lock
	rdb.mux.Lock()
	defer rdb.mux.Unlock()

	subscribedChannels, exists := rdb.channels[*conn]
	if !exists {
		subscribedChannels = &ChannelList{
			channels: make([]string, 0),
			subCount: 0,
			msgChan:  make(chan []byte, BufferedChannelSize),
			done:     make(chan bool, 1),
		}
		rdb.channels[*conn] = subscribedChannels

		// call listener
		go subscibeListener(conn, subscribedChannels, rdb)
	}

	// check if already subscribed
	for _, channel := range subscribedChannels.channels {
		if channel == ch {
			debug("Client already subscribed to channel: %s\n", ch)
			return subscribedChannels.subCount
		}
	}

	subscribedChannels.channels = append(subscribedChannels.channels, ch)
	subscribedChannels.subCount++

	rdb.channels[*conn] = subscribedChannels

	connState := rdb.getConnState((*conn).RemoteAddr().String())
	connState.subMode = true
	rdb.setConnState((*conn).RemoteAddr().String(), *connState)

	return subscribedChannels.subCount
}

func publish(ch string, msg string, rdb *redisDB) int {
	rdb.mux.Lock()
	defer rdb.mux.Unlock()

	count := 0
	for _, channelList := range rdb.channels {
		if contains(channelList.channels, ch) {
			resp := encodeArray([]any{"message", ch, msg})

			select {
			case channelList.msgChan <- resp:
				debug("Message published to channel: %s\n", ch)
				count++
			default:
				debug("Subscriber channel full, dropping message for channel: %s\n", ch)
			}
		}
	}

	return count
}

func subscibeListener(conn *net.Conn, channelList *ChannelList, rdb *redisDB) {
	// cleanup
	defer func() {
		rdb.mux.Lock()
		delete(rdb.channels, *conn)
		rdb.mux.Unlock()

		close(channelList.msgChan)
		debug("Cleaning up subscriber listener for: %s\n", (*conn).RemoteAddr().String())
	}()

	for {
		select {
		case msg, ok := <-channelList.msgChan:
			if !ok {
				debug("Message channel closed for: %s\n", (*conn).RemoteAddr().String())
				return
			}

			debug("Sending message to subscriber: %s\n", (*conn).RemoteAddr().String())

			_, err := (*conn).Write(msg)
			if err != nil {
				fmt.Println("Error writing to subscriber: ", err.Error())
				return
			}

			fmt.Printf("Sent to sub : %s\n", printCommand(msg))
		case <-channelList.done:
			return
		}
	}
}

func unsubscribe(ch string, rdb *redisDB, conn *net.Conn) int {
    rdb.mux.Lock()
    defer rdb.mux.Unlock()
    
    channelList, exists := rdb.channels[*conn]
    if !exists {
        return 0
    }
    
    // Remove channel from list
    for i, channel := range channelList.channels {
        if channel == ch {
            channelList.channels = append(channelList.channels[:i], channelList.channels[i+1:]...)
            channelList.subCount--
            break
        }
    }
    
    // If no more subscriptions, exit sub mode
    if channelList.subCount == 0 {
        channelList.done <- true
        
        connAddr := (*conn).RemoteAddr().String()
        connState := rdb.getConnState(connAddr)
        connState.subMode = false
        rdb.setConnState(connAddr, *connState)
    }
    
    return channelList.subCount
}