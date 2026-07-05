package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TXNRequest struct {
	TXN [][3]any `json:"txn"`
}

type Store struct {
	entries map[int]int
	mu      sync.Mutex
}

func main() {
	n := maelstrom.NewNode()
	kv := Store{entries: make(map[int]int)}

	// Executes a transaction over the local replica. Transactions acquire a
	// mutex for the local key/value store, serializing execution at this node.
	// As a result, reads observe all writes performed earlier in the same
	// transaction, and no concurrent transaction can observe a partially
	// applied state.
	//
	// Once the transaction has committed locally and the client has been
	// replied to, its write set is propagated asynchronously to every replica.
	// Replication is performed without coordination or consensus, so replicas
	// may apply transactions at different times and temporarily diverge.
	//
	// This design provides local serial execution and eventual consistency of
	// replicas, but does not provide linearizability or visibility of
	// transactions atomically across the cluster.
	n.Handle("txn", func(msg maelstrom.Message) error {
		var body TXNRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		txn_out := make([][3]any, 0, len(body.TXN))
		writes := make([][3]any, 0)

		kv.mu.Lock()

		for _, operation := range body.TXN {
			ty := operation[0].(string)
			key := int(operation[1].(float64))
			val := operation[2]

			switch ty {
			case "r":
				v, ok := kv.entries[key]
				if !ok {
					txn_out = append(txn_out, [3]any{"r", key, nil})
				} else {
					txn_out = append(txn_out, [3]any{"r", key, v})
				}
			case "w":
				kv.entries[key] = int(val.(float64))

				txn_out = append(txn_out, operation)
				writes = append(writes, operation)
			}
		}

		kv.mu.Unlock()

		if err := n.Reply(msg, map[string]any{"type": "txn_ok", "txn": txn_out}); err != nil {
			return err
		}

		for _, peer := range n.NodeIDs() {
			if peer != n.ID() {
				err := n.RPC(peer, map[string]any{"type": "txn", "txn": writes}, func(msg maelstrom.Message) error {
					return nil
				})
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
