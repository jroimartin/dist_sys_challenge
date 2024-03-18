// Copyright 2024 Roi Martin

// Broadcast solves Fly.io [challenge 3: Broadcast].
//
// [challenge 3: Broadcast]: https://fly.io/dist-sys/3a/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jroimartin/maelstrom"
)

func main() {
	n := maelstrom.NewNode(os.Stdin, os.Stdout)
	r := maelstrom.NewRetrier(n, 2*time.Second, 500*time.Millisecond)
	b := newBroadcaster(n, r)
	n.HandleFunc("broadcast", b.HandleBroadcast)
	n.HandleFunc("read", b.HandleRead)
	n.HandleFunc("topology", b.HandleTopology)
	n.HandleFunc("sync", b.HandleSync)
	log.Fatalln(n.Serve())
}

type broadcaster struct {
	node    *maelstrom.Node
	retrier *maelstrom.Retrier

	mu        sync.Mutex
	neighbors []string
	messages  []json.RawMessage
}

type syncPayload struct {
	Message json.RawMessage `json:"message"`
	Seen    map[string]bool `json:"seen"`
}

func newBroadcaster(node *maelstrom.Node, retrier *maelstrom.Retrier) *broadcaster {
	b := &broadcaster{
		node:    node,
		retrier: retrier,
	}
	if retrier != nil {
		go retrier.Retry(context.TODO())
	}
	return b
}

func (b *broadcaster) HandleBroadcast(n *maelstrom.Node, msg maelstrom.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var body map[string]json.RawMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("error: broadcast: unmarshal: %v", err)
		return
	}

	b.messages = append(b.messages, body["message"])

	if _, err := n.Reply(msg, "broadcast_ok", nil); err != nil {
		log.Printf("error: broadcast: reply: %v", err)
		return
	}

	payload := syncPayload{
		Message: body["message"],
		Seen:    map[string]bool{n.ID(): true},
	}
	if err := b.propagate(payload); err != nil {
		log.Printf("error: broadcast: propagate: %v", err)
	}
}

func (b *broadcaster) HandleRead(n *maelstrom.Node, msg maelstrom.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	payload := map[string][]json.RawMessage{
		"messages": b.messages,
	}
	if _, err := n.Reply(msg, "read_ok", payload); err != nil {
		log.Printf("error: read: reply: %v", err)
	}
}

func (b *broadcaster) HandleTopology(n *maelstrom.Node, msg maelstrom.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("error: topology: unmarshal: %v", err)
		return
	}

	neighbors, ok := body.Topology[n.ID()]
	if !ok {
		log.Printf("error: topology: unknown node: %v", n.ID())
		return
	}
	b.neighbors = neighbors

	if _, err := n.Reply(msg, "topology_ok", nil); err != nil {
		log.Printf("error: topology: reply: %v", err)
	}
}

func (b *broadcaster) HandleSync(n *maelstrom.Node, msg maelstrom.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var payload syncPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		log.Printf("error: sync: unmarshal: %v", err)
		return
	}
	b.messages = append(b.messages, payload.Message)

	if _, err := n.Reply(msg, "sync_ok", nil); err != nil {
		log.Printf("error: sync: reply: %v", err)
	}

	payload.Seen[n.ID()] = true
	if err := b.propagate(payload); err != nil {
		log.Printf("error: sync: propagate: %v", err)
	}
}

func (b *broadcaster) HandleSyncOk(n *maelstrom.Node, resp maelstrom.Message) {
	if err := b.retrier.Remove(resp); err != nil {
		log.Printf("error: sync_ok: remove retry: %v", err)
	}
}

func (b *broadcaster) propagate(payload syncPayload) error {
	for _, neighbor := range b.neighbors {
		if payload.Seen[neighbor] {
			continue
		}
		req, err := b.node.RPC(neighbor, "sync", payload, maelstrom.HandlerFunc(b.HandleSyncOk))
		if err != nil {
			return fmt.Errorf("RPC: %w", err)
		}
		if err := b.retrier.Add(req); err != nil {
			return fmt.Errorf("Handle: %w", err)
		}
	}
	return nil
}
