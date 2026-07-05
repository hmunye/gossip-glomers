package main

import (
	"log"
	"time"

	"github.com/hmunye/gossip-glomers/internal/broadcast"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	broadcast.New(n).
		WithFanout(3).
		WithInterval(150 * time.Millisecond).
		Run()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
