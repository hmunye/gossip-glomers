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

	// Generating unique identifiers within a single process is straightforward.
	// Monotonic counters or random UUIDs are usually sufficient. Distributed
	// systems, however, have several ways to generate IDs without relying on
	// shared state, each with different consistency, availability, and
	// performance trade-offs.
	//
	// Using wall-clock time alone may seem like a simple solution, but it does
	// not provide reliable uniqueness. Clocks on different machines are never
	// perfectly synchronized, so two nodes may generate the same timestamp at
	// the same moment. Furthermore, clock skew and drift can cause time to move
	// backwards, violating assumptions about monotonic ordering.
	//
	// Another approach is to coordinate ID generation through consensus or a
	// centralized counter. This can guarantee properties such as uniqueness and
	// global ordering, but it also introduces additional latency and
	// operational complexity. Each generated ID requires communication between
	// nodes, and during a network partition, ID generation may become
	// unavailable if a quorum cannot be reached.
	n.Handle("generate", func(msg maelstrom.Message) error {
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
		// provides a time-ordered property. The machine ID distinguishes
		// between nodes in a distributed system, and the sequence counter
		// disambiguates multiple IDs generated within the same millisecond on a
		// single node.
		//
		// A custom epoch (rather than the Unix epoch) is commonly used to align
		// the finite 41-bit timestamp range to within the expected operational
		// lifetime of the system, maximizing the usable duration before
		// timestamp overflow.
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": fmt.Sprintf("%s-%d", n.ID(), counter.Add(1))})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
