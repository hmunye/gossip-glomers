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

	n.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%d-%s", counter.Add(1), n.ID())

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
