// Copyright 2024 Roi Martin

// Echo solves Fly.io [challenge 1: Echo].
//
// [challenge 1: Echo]: https://fly.io/dist-sys/1/
package main

import (
	"log"
	"os"

	maelstrom "github.com/jroimartin/maelstrom"
)

func main() {
	n := maelstrom.NewNode(os.Stdin, os.Stdout)
	n.HandleFunc("echo", func(n *maelstrom.Node, req maelstrom.Message) {
		if err := n.Reply(req, "echo_ok", req.Body); err != nil {
			log.Printf("error: echo: reply: %v", err)
		}
	})
	log.Fatalln(n.Serve())
}
