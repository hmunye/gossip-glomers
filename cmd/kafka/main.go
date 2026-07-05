package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequest struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendResponse struct {
	Offset int `json:"offset"`
}

type PollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Msgs map[string][][2]int `json:"msgs"`
}

type CommitRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type ListCommitsRequest struct {
	Keys []string `json:"keys"`
}

type ListCommitsResponse struct {
	Offsets map[string]int `json:"offsets"`
}

// Broker is an in-memory, Kafka-like structure that stores topic partitions as
// append-only logs.
//
// Each topic partition is represented as an ordered sequence of records that is
// only ever appended to. Producers append records to a partition, and consumers
// read sequentially from it.
//
// Consumer progress is tracked via committed offsets per topic, which indicate
// the last successfully processed record. The broker maintains these logs and
// per-consumer offset states.
type Broker struct {
	logs map[string][]int
	lMu  sync.Mutex

	commits map[string]int
	cMu     sync.Mutex
}

func hashID(id string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(id))

	return h.Sum64()
}

func main() {
	n := maelstrom.NewNode()

	broker := Broker{
		logs:    make(map[string][]int),
		commits: make(map[string]int),
	}

	var peers []string

	n.Handle("init", func(msg maelstrom.Message) error {
		// Sort peers to ensure deterministic ordering for index-based
		// partitioning across the cluster, including key-based routing in
		// sharded operations.
		//
		// Sharding is the partitioning of a dataset or request space across
		// multiple nodes, where each node is responsible for a subset of the
		// keyspace.
		slices.Sort(n.NodeIDs())
		peers = slices.Clone(n.NodeIDs())

		return nil
	})

	// Keys associated with an append-only log are deterministically assigned
	// to nodes using hash-based sharding with modulo partitioning over the
	// sorted peer set. Each node is responsible for a subset of the keyspace.
	//
	// If the current node owns the key, the message is appended to the local
	// append-only log for that key, and the resulting offset is returned,
	// otherwise, the request is synchronously forwarded to the owning node,
	// which performs the append and returns the assigned offset.
	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		idx := hashID(body.Key) % uint64(len(peers))
		peer := peers[idx]

		var offset int

		if peer != n.ID() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			rpc, err := n.SyncRPC(ctx, peer, msg.Body)
			if err != nil {
				log.Println(err)
				return err
			}

			var incoming SendResponse
			if err := json.Unmarshal(rpc.Body, &incoming); err != nil {
				return err
			}

			offset = incoming.Offset
		} else {
			broker.lMu.Lock()

			broker.logs[body.Key] = append(broker.logs[body.Key], body.Msg)
			offset = len(broker.logs[body.Key]) - 1

			broker.lMu.Unlock()
		}

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	// Reads messages from per-key append-only logs starting at the requested
	// offsets. For each key, the request specifies a starting offset into the
	// log. If the current node owns the key, it returns all available messages
	// from that offset onward (at most 10). If another node owns the key, the
	// request is forwarded to the owning node, which performs the same
	// offset-based read and returns its result.
	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][][2]int, len(body.Offsets))

		for key, offset := range body.Offsets {
			idx := hashID(key) % uint64(len(peers))
			peer := peers[idx]

			if peer != n.ID() {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				outgoing := map[string]any{
					"type": "poll",
					"offsets": map[string]int{
						key: offset,
					},
				}

				rpc, err := n.SyncRPC(ctx, peer, outgoing)
				if err != nil {
					log.Println(err)
					return err
				}

				var incoming PollResponse
				if err := json.Unmarshal(rpc.Body, &incoming); err != nil {
					return err
				}

				msgs[key] = incoming.Msgs[key]
			} else {
				broker.lMu.Lock()

				logs := broker.logs[key]
				poll_len := min(offset+10, len(logs))

				key_msgs := make([][2]int, 0, poll_len-offset)

				for i, msg := range logs[offset:poll_len] {
					key_msgs = append(key_msgs, [2]int{offset + i, msg})
				}

				msgs[key] = key_msgs

				broker.lMu.Unlock()
			}
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	// Records the latest processed offset for each key provided. Each offset
	// indicates that all messages up to and including that position in the
	// per-key append-only log have been successfully processed.
	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			idx := hashID(key) % uint64(len(peers))
			peer := peers[idx]

			if peer != n.ID() {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				outgoing := map[string]any{
					"type": "commit_offsets",
					"offsets": map[string]int{
						key: offset,
					},
				}

				_, err := n.SyncRPC(ctx, peer, outgoing)
				if err != nil {
					log.Println(err)
					return err
				}
			} else {
				broker.cMu.Lock()

				broker.commits[key] = offset

				broker.cMu.Unlock()
			}
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	// Queries the latest committed offset for each requested key. Each returned
	// offset represents the last processed position in the corresponding
	// per-key append-only log. Keys that have no committed state on the node
	// may be omitted.
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommitsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		for _, key := range body.Keys {
			idx := hashID(key) % uint64(len(peers))
			peer := peers[idx]

			if peer != n.ID() {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				outgoing := map[string]any{
					"type": "list_committed_offsets",
					"keys": []string{key},
				}

				rpc, err := n.SyncRPC(ctx, peer, outgoing)
				if err != nil {
					log.Println(err)
					return err
				}

				var incoming ListCommitsResponse
				if err := json.Unmarshal(rpc.Body, &incoming); err != nil {
					return err
				}

				offset, ok := incoming.Offsets[key]
				if ok {
					offsets[key] = offset
				}
			} else {
				broker.cMu.Lock()

				offset, ok := broker.commits[key]
				if ok {
					offsets[key] = offset
				}

				broker.cMu.Unlock()
			}
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
