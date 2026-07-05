package main

import (
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var counter atomic.Int64

	// Alternative Design:
	//
	// Snowflake ID - 64-bit identifier composed of fixed bit fields.
	//
	//   0                                                     63
	//   |------------------|--------------|--------------------|
	//   |  timestamp (ms)  |  machine id  |  sequence counter  |
	//   |     (41-bit)     |   (10-bit)   |      (12-bit)      |
	//   |------------------|--------------|--------------------|
	//
	// The timestamp encodes milliseconds since a fixed custom epoch and
	// provides a time-based ordering signal. The machine ID distinguishes
	// between nodes in a distributed system, and the sequence counter
	// disambiguates multiple IDs generated within the same millisecond on a
	// single node.
	//
	// Custom epoch (rather than the Unix epoch) is commonly used to align the
	// finite 41-bit timestamp range with the expected operational lifetime of
	// the node, maximizing the usable duration before timestamp overflow.
	n.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%s-%d", n.ID(), counter.Add(1))

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
