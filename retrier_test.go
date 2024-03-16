// Copyright 2024 Roi Martin

package maelstrom

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestRetrier(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124}}
		{"src": "c1", "dest": "n1", "body": {"type": "t2_ok", "msg_id": 125, "in_reply_to": 2}}
	`)
	want := []string{
		`{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
	}

	stdin := bytes.NewBufferString(input)
	stdout := new(bytes.Buffer)

	node := NewNode(stdin, stdout)
	retrier := NewRetrier(node, 500*time.Millisecond, 250*time.Millisecond)

	cont := make(chan bool)
	done := make(chan bool)

	respHandler := func(_ *Node, msg Message) {
		if err := retrier.Remove(msg); err != nil {
			t.Errorf("response handler: retrier remove error: %v", err)
		}
		done <- true
	}
	handler := func(n *Node, _ Message) {
		msg, err := n.RPC("c1", "t2", map[string]string{"k1": "v1"}, HandlerFunc(respHandler))
		if err != nil {
			t.Fatalf("handler: RPC error: %v", err)
		}
		if err := retrier.Add(msg); err != nil {
			t.Errorf("handler: retrier add error: %v", err)
		}
		cont <- true
	}
	testHookNodeServe = func(_ *Node, msg Message) {
		common, err := msg.CommonBody()
		if err != nil {
			t.Fatalf("hook: common body error: %v", err)
		}
		if common.Type == "t2_ok" {
			<-cont
			time.Sleep(1100 * time.Millisecond)
		}
	}

	node.HandleFunc("t1", handler)

	go retrier.Retry()
	if err := node.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	<-done

	if len(node.respHandlers) > 0 {
		t.Errorf("unexpected number of response handlers: %v", len(node.respHandlers))
	}

	if len(retrier.pending) > 0 {
		t.Errorf("unexpected number of pending retries: %v", len(retrier.pending))
	}

	got := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	less := func(a, b Message) bool { return a.String() < b.String() }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestRetrier_Add_error(t *testing.T) {
	retrier := NewRetrier(nil, 0, 0)

	malformedMsg := Message{
		Src:  "n1",
		Dest: "c1",
		Body: json.RawMessage(`{`),
	}
	if err := retrier.Add(malformedMsg); err == nil {
		t.Error("expected add error when payload is invalid")
	}

	noMsgID := Message{
		Src:  "n1",
		Dest: "c1",
		Body: json.RawMessage(`{"type": "t1"}`),
	}
	if err := retrier.Add(noMsgID); err == nil {
		t.Error("expected add error when no msg_id")
	}
}

func TestRetrier_Remove_error(t *testing.T) {
	retrier := NewRetrier(nil, 0, 0)

	malformedMsg := Message{
		Src:  "c1",
		Dest: "n1",
		Body: json.RawMessage(`{`),
	}
	if err := retrier.Remove(malformedMsg); err == nil {
		t.Error("expected remove error when payload is invalid")
	}

	noMsgID := Message{
		Src:  "c1",
		Dest: "n1",
		Body: json.RawMessage(`{"type": "t1", "msg_id": 123}`),
	}
	if err := retrier.Remove(noMsgID); err == nil {
		t.Error("expected remove error when no in_reply_to")
	}
}
