package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	// Within a single process, threads share the same virtual-address space and
	// can communicate through shared data structures synchronized with
	// primitives such as mutexes. Communication between processes on the same
	// machine typically relies on IPC (Inter-Process Communication) mechanisms
	// such as sockets, pipes, or memory-mapped pages (mmap).
	//
	// In a distributed system, nodes communicate by exchanging messages over a
	// network. Unlike threads, nodes cannot share memory or directly inspect
	// each other's state, which introduces additional challenges:
	//
	//   - Messages may be lost
	//   - Messages may be delayed
	//   - Messages may be received out of order
	//
	// Since nodes cannot rely on shared state, every message must contain
	// enough information for the receiver to interpret it independently.
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
