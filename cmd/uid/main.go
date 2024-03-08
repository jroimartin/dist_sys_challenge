// Uid solves [challenge 2: Unique ID Generation].
//
// [challenge 2: Unique ID Generation]: https://fly.io/dist-sys/2/
package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jroimartin/maelstrom"
)

func main() {
	n, err := maelstrom.NewNode()
	if err != nil {
		log.Fatalf("new node: %v", err)
	}
	gen, err := newUIDGenerator(n.ID())
	if err != nil {
		log.Printf("handler: new UID generator: %v", err)
	}
	n.HandleFunc("generate", func(msg maelstrom.Message) {
		payload := map[string]string{"id": gen.uid()}
		if err := n.Reply(msg, "generate_ok", payload); err != nil {
			log.Printf("generate: reply: %v", err)
		}
	})
	log.Fatalln(n.Serve())
}

type uidGenerator struct {
	nodeNum uint64

	mu  sync.Mutex
	ctr uint64
}

func newUIDGenerator(nodeID string) (*uidGenerator, error) {
	after, found := strings.CutPrefix(nodeID, "n")
	if !found {
		return nil, fmt.Errorf("invalid node ID: %v", nodeID)
	}
	num, err := strconv.ParseUint(after, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node number: %v", num)
	}
	gen := &uidGenerator{
		nodeNum: num,
	}
	return gen, nil
}

func (gen *uidGenerator) uid() string {
	gen.mu.Lock()
	defer gen.mu.Unlock()

	now := uint64(time.Now().UnixNano())

	ts := now & ((1 << 42) - 1)
	num := gen.nodeNum & ((1 << 10) - 1)
	ctr := gen.ctr & ((1 << 12) - 1)
	uid := (ts << 22) | (num << 12) | ctr

	gen.ctr++

	return strconv.FormatUint(uid, 10)
}
