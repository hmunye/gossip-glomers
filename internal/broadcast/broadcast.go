// Package broadcast implements a gossip-based protocol for disseminating
// messages across a cluster of Maelstrom nodes.
//
// In a distributed system, nodes do not share memory, so coordination and
// information exchange must occur explicitly over the network. A naive approach
// is full fan-out broadcasting, where each node sends every message directly to
// all other nodes. While this achieves immediate dissemination in small
// systems, it does not scale: each broadcast generates O(N) outbound messages
// per node, leading to O(N^2) total traffic across the cluster. In addition,
// this approach is not tolerant to partitions or partial failures, since
// dropped messages are not naturally recovered.
//
// Gossip protocols address this by replacing global fan-out with repeated local
// exchange. Each node forwards messages to a small, typically random subset of
// peers. Those peers repeat the process, causing information to propagate
// probabilistically across the system over multiple rounds.
//
// From a systems perspective, this approach trades immediate consistency for
// scalable, fault-tolerant dissemination, with the following properties:
//
//   - Partition tolerance: messages continue to propagate along any available
//     network paths; partitions slow convergence rather than preventing it
//
//   - Eventual consistency: given sufficient rounds of communication and a
//     sufficiently connected network, all nodes eventually converge on the same
//     set of messages
//
//   - Partial failure tolerance: dissemination relies on redundant peer-to-peer
//     propagation, so the failure or unavailability of individual nodes reduces
//     redundancy but does not prevent eventual convergence
//
// The primary tradeoff is latency and redundancy. Information spreads over
// multiple hops rather than directly, so convergence time depends on fan-out
// (number of peers contacted per round) and the gossip interval (how frequently
// gossip rounds occur).
package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Broadcaster represents the protocol state for a [maelstrom.Node].
type Broadcaster struct {
	n *maelstrom.Node

	fanout            int
	interval, timeout time.Duration

	messages map[int]struct{}
	msgMu    sync.Mutex

	known   map[string]map[int]struct{}
	knownMu sync.Mutex
}

// New returns a Broadcaster using the given [maelstrom.Node].
func New(n *maelstrom.Node) *Broadcaster {
	return &Broadcaster{
		n:        n,
		fanout:   3,
		interval: 100 * time.Millisecond,
		timeout:  time.Second,
		messages: make(map[int]struct{}),
		known:    make(map[string]map[int]struct{}),
	}
}

// WithFanout configures the number of peers the Broadcaster exchanges gossip
// messages with on each interval. Defaults to 3.
//
// Higher fan-out reduces convergence time (latency) but increases network
// traffic. Lower fan-out reduces network traffic but slows propagation.
func (b *Broadcaster) WithFanout(fanout int) *Broadcaster {
	b.fanout = fanout
	return b
}

// WithInterval configures how frequently the Broadcaster gossips with its
// peers. Defaults to 100ms.
//
// Shorter intervals reduce convergence time (latency) but increase network
// traffic. Longer intervals reduce network traffic but slow propagation.
func (b *Broadcaster) WithInterval(interval time.Duration) *Broadcaster {
	b.interval = interval
	return b
}

// WithTimeout configures how long the Broadcaster waits for an RPC response
// from a peer before giving up. Defaults to 1s.
func (b *Broadcaster) WithTimeout(timeout time.Duration) *Broadcaster {
	b.timeout = timeout
	return b
}

// Run registers the "broadcast", "topology", "read", and "gossip" message
// handlers on the underlying [maelstrom.Node] and periodically sends gossip
// messages to its peers in the background.
func (b *Broadcaster) Run() {
	b.n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		b.msgMu.Lock()
		defer b.msgMu.Unlock()

		b.messages[int(body["message"].(float64))] = struct{}{}

		delete(body, "message")

		body["type"] = "broadcast_ok"

		return b.n.Reply(msg, body)
	})

	b.n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		body["type"] = "read_ok"
		body["messages"] = b.snapshotMessages()

		return b.n.Reply(msg, body)
	})

	b.n.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		// Ignoring the provided topology in favor of a random peer subset
		// derived from the full cluster each round.
		body["type"] = "topology_ok"

		return b.n.Reply(msg, body)
	})

	b.n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		b.msgMu.Lock()
		defer b.msgMu.Unlock()

		for _, msg := range body["messages"].([]any) {
			b.messages[int(msg.(float64))] = struct{}{}
		}

		delete(body, "messages")
		body["type"] = "gossip_ok"

		return b.n.Reply(msg, body)
	})

	go b.gossip()
}

func (b *Broadcaster) gossip() {
	var peers []string

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for range ticker.C {
		if peers == nil {
			if len(b.n.NodeIDs()) == 0 {
				continue
			}

			peers = slices.Clone(b.n.NodeIDs())

			if i := slices.Index(peers, b.n.ID()); i != -1 {
				peers = slices.Delete(peers, i, i+1)
			}
		}

		shufflePeers(peers)
		subset := peers[:min(b.fanout, len(peers))]

		for _, peer := range subset {
			b.knownMu.Lock()

			peer_msgs := b.known[peer]
			delta := b.deltaMessages(peer_msgs)

			b.knownMu.Unlock()

			if len(delta) == 0 {
				continue
			}

			body := make(map[string]any)

			body["type"] = "gossip"
			body["messages"] = delta

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
				defer cancel()

				_, err := b.n.SyncRPC(ctx, peer, body)
				if err != nil {
					log.Println(err)
					return
				}

				b.knownMu.Lock()

				peer_msgs := b.known[peer]
				if peer_msgs == nil {
					peer_msgs = make(map[int]struct{}, len(delta))
				}

				for _, msg := range delta {
					peer_msgs[msg] = struct{}{}
				}

				b.knownMu.Unlock()
			}()
		}
	}
}

// deltaMessages returns the set difference between locally stored messages and
// messages already known by a specific peer.
//
// Transmitting missing messages only reduces network traffic and payload size.
func (b *Broadcaster) deltaMessages(peer_msgs map[int]struct{}) []int {
	b.msgMu.Lock()
	defer b.msgMu.Unlock()

	delta := make([]int, 0)

	for msg := range b.messages {
		if _, ok := peer_msgs[msg]; !ok {
			delta = append(delta, msg)
		}
	}

	return delta
}

// snapshotMessages returns a snapshot of the current message set as a slice.
//
// Snapshots are used for transmission and represent a point-in-time view of the
// Broadcaster’s state.
func (b *Broadcaster) snapshotMessages() []int {
	b.msgMu.Lock()
	defer b.msgMu.Unlock()

	msgs := make([]int, 0, len(b.messages))

	for k := range b.messages {
		msgs = append(msgs, k)
	}

	return msgs
}

// shufflePeers randomizes peer order within the given slice.
//
// Randomization of peers reduces systematic bias in propagation paths, avoids
// network hotspots, and improves robustness and convergence behavior.
func shufflePeers(peers []string) {
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
}
