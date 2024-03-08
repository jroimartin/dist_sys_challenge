// Echo solves [challenge 1: Echo].
//
// [challenge 1: Echo]: https://fly.io/dist-sys/1/
package main

import (
	"log"

	maelstrom "github.com/jroimartin/maelstrom"
)

func main() {
	n, err := maelstrom.NewNode()
	if err != nil {
		log.Fatalf("new node: %v", err)
	}
	n.HandleFunc("echo", func(req maelstrom.Message) {
		if err := n.Reply(req, "echo_ok", req.Body); err != nil {
			log.Printf("handler: reply: %v", err)
		}
	})
	log.Fatalln(n.Serve())
}
