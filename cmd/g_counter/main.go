package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// The "seq-kv" service provides sequential consistency. All operations
	// appear to take effect in a single global order that is consistent across
	// all nodes, although different nodes may temporarily observe stale values.
	//
	// For example, if node A writes 2 and later writes 4 to key "a", another
	// node may still read 2 for a period of time even after the write of 4 has
	// completed. Over time, all nodes will eventually observe the same sequence
	// of writes in the same order, but not necessarily at the same time.
	//
	// Since reads may be stale, clients cannot assume they are operating on the
	// latest state. If an update must be applied conditionally based on the
	// current value, operations such as compare-and-swap (CAS) are required to
	// ensure correctness under concurrent modification.
	kv := maelstrom.NewSeqKV(n)

	var mu sync.Mutex

	// The counter is implemented as a per-node sharded counter over a shared
	// key-value store. Each node uses its own node ID as the key and maintains
	// a single integer value representing its count.
	//
	// Each increment performs a read-modify-write cycle on the node's own key:
	// the current value is read from the KV store, incremented by the requested
	// delta, and written back. Since each node only writes to its own key,
	// there are no write conflicts across the cluster and no need for
	// coordination or consensus.
	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		mu.Lock()

		val, err := kv.Read(ctx, n.ID())

		mu.Unlock()

		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); !ok || rpcErr.Code != maelstrom.KeyDoesNotExist {
				return err
			}

			val = 0
		}

		mu.Lock()

		err = kv.Write(ctx, n.ID(), val.(int)+delta)

		mu.Unlock()

		if err != nil {
			return err
		}

		delete(body, "delta")
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	// The logical value of the counter is computed by reading each node's
	// individual shard (keyed by node ID) from the shared key-value store and
	// summing the values. Each node maintains its own independent counter value
	// under its own key, so the global counter is represented as the sum of all
	// per-node values.
	//
	// Reads may observe partial or stale state if updates are in flight, which
	// is the purpose behind the CAS loop. Correctness relies on the assumption
	// that each node's key is independently maintained and eventually reflects
	// all increments issued to that node.
	n.Handle("read", func(msg maelstrom.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		var counter int

		for _, peer := range n.NodeIDs() {
			var peer_count int

			for {
				val, err := kv.Read(ctx, peer)
				if err != nil {
					if rpcErr, ok := err.(*maelstrom.RPCError); !ok || rpcErr.Code != maelstrom.KeyDoesNotExist {
						return err
					}
				}

				err = kv.CompareAndSwap(ctx, peer, val, val, false)
				if err != nil {
					if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
						continue
					}

					if rpcErr, ok := err.(*maelstrom.RPCError); !ok || rpcErr.Code != maelstrom.KeyDoesNotExist {
						return err
					}

					val = 0
				}

				peer_count = val.(int)
				break
			}

			counter += peer_count
		}

		body := make(map[string]any)

		body["type"] = "read_ok"
		body["value"] = counter

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
