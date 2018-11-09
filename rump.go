package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
)

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Structure to separate the key, the value, and the TTL
type kvt struct {
	key   string
	value string
	time  int64 //milliseconds via PTTL
}

// Scan and queue source keys.
func get(conn redis.Conn, queue chan<- kvt) {
	var (
		cursor int64
		keys   []string
	)

	for {
		// Scan a batch of keys.
		values, err := redis.Values(conn.Do("SCAN", cursor))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)

		// Get pipelined dumps.
		for _, key := range keys {
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		for _, key := range keys {
			conn.Send("PTTL", key)
		}

		pttls, err := redis.Int64s(conn.Do(""))
		handle(err)

		for i, k := range keys {
			queue <- kvt{k, dumps[i], pttls[i]}
		}

		// Last iteration of scan.
		if cursor == 0 {
			close(queue)
			break
		}
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan kvt) {
	for kvt := range queue {
		if kvt.time == -1 {
			conn.Send("RESTORE", kvt.key, "0", kvt.value, "REPLACE")
		} else {
			conn.Send("RESTORE", kvt.key, kvt.time, kvt.value, "REPLACE")
		}
	}

	_, err := conn.Do("")
	handle(err)
}

func main() {
	from := flag.String("from", "", "example: redis://127.0.0.1:6379/0")
	to := flag.String("to", "", "example: redis://127.0.0.1:6379/1")
	flag.Parse()

	source, err := redis.DialURL(*from)
	handle(err)
	destination, err := redis.DialURL(*to)
	handle(err)
	defer source.Close()
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := make(chan kvt, 100)

	// Scan and send to queue.
	go get(source, queue)

	// Restore keys as they come into queue.
	put(destination, queue)

	fmt.Println("Sync done.")
}
