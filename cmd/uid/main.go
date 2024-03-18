// Copyright 2024 Roi Martin

// Uid solves Fly.io [challenge 2: Unique ID Generation].
//
// [challenge 2: Unique ID Generation]: https://fly.io/dist-sys/2/
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jroimartin/maelstrom"
)

func main() {
	n := maelstrom.NewNode(os.Stdin, os.Stdout)
	n.Handle("generate", &uidGenerator{})
	log.Fatalln(n.Serve())
}

type uidGenerator struct {
	mu          sync.Mutex
	initialized bool
	nodeNum     uint64
	ctr         uint64
}

func (gen *uidGenerator) uid(nodeID string) (string, error) {
	gen.mu.Lock()
	defer gen.mu.Unlock()

	if !gen.initialized {
		after, found := strings.CutPrefix(nodeID, "n")
		if !found {
			return "", fmt.Errorf("invalid node ID: %v", nodeID)
		}
		num, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid node number: %v", num)
		}
		gen.nodeNum = num
		gen.initialized = true
	}

	now := uint64(time.Now().UnixNano())

	ts := now & ((1 << 42) - 1)
	num := gen.nodeNum & ((1 << 10) - 1)
	ctr := gen.ctr & ((1 << 12) - 1)
	uid := (ts << 22) | (num << 12) | ctr

	gen.ctr++

	return strconv.FormatUint(uid, 10), nil
}

func (gen *uidGenerator) ServeMessage(n *maelstrom.Node, msg maelstrom.Message) {
	uid, err := gen.uid(n.ID())
	if err != nil {
		log.Printf("error: generate: uid: %v", err)
		return
	}

	payload := map[string]string{"id": uid}
	if _, err := n.Reply(msg, "generate_ok", payload); err != nil {
		log.Printf("error: generate: reply: %v", err)
	}
}
