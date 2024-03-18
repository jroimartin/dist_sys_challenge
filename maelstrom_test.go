// Copyright 2024 Roi Martin

package maelstrom

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewMessage(t *testing.T) {
	tests := []struct {
		name             string
		dest, src, typ   string
		payload          any
		msgID, inReplyTo *uint64
		want             Message
		wantNilErr       bool
	}{
		{
			name: "multiple fields",
			dest: "dest",
			src:  "src",
			typ:  "typ",
			payload: map[string]any{
				"k1": "v1",
				"k2": "v2",
			},
			msgID:     ptr(uint64(123)),
			inReplyTo: ptr(uint64(122)),
			want: Message{
				Src:  "src",
				Dest: "dest",
				Body: json.RawMessage(`{"type": "typ", "msg_id": 123, "in_reply_to": 122, "k1": "v1", "k2": "v2"}`),
			},
			wantNilErr: true,
		},
		{
			name: "overlapped fields",
			dest: "dest",
			src:  "src",
			typ:  "typ",
			payload: map[string]any{
				"k1":     "v1",
				"msg_id": "555",
			},
			msgID:     ptr(uint64(123)),
			inReplyTo: ptr(uint64(122)),
			want: Message{
				Src:  "src",
				Dest: "dest",
				Body: json.RawMessage(`{"type": "typ", "msg_id": 123, "in_reply_to": 122, "k1": "v1"}`),
			},
			wantNilErr: true,
		},
		{
			name:      "zero payload",
			dest:      "dest",
			src:       "src",
			typ:       "typ",
			payload:   map[string]any{},
			msgID:     ptr(uint64(123)),
			inReplyTo: ptr(uint64(122)),
			want: Message{
				Src:  "src",
				Dest: "dest",
				Body: json.RawMessage(`{"type": "typ", "msg_id": 123, "in_reply_to": 122}`),
			},
			wantNilErr: true,
		},
		{
			name:      "nil payload",
			dest:      "dest",
			src:       "src",
			typ:       "typ",
			payload:   nil,
			msgID:     ptr(uint64(123)),
			inReplyTo: ptr(uint64(122)),
			want: Message{
				Src:  "src",
				Dest: "dest",
				Body: json.RawMessage(`{"type": "typ", "msg_id": 123, "in_reply_to": 122}`),
			},
			wantNilErr: true,
		},
		{
			name:       "invalid payload",
			dest:       "dest",
			src:        "src",
			typ:        "typ",
			payload:    "invalid",
			msgID:      ptr(uint64(123)),
			inReplyTo:  ptr(uint64(122)),
			want:       Message{},
			wantNilErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMessage(tt.dest, tt.src, tt.typ, tt.payload, tt.msgID, tt.inReplyTo)

			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: %v", err)
			}

			if !cmpMessage(got, tt.want) {
				t.Errorf("message mismatch:\ngot: %v\nwant: %v", got, tt.want)
			}
		})
	}
}

func TestMessage_CommonBody(t *testing.T) {
	tests := []struct {
		name       string
		msg        Message
		want       CommonBody
		wantNilErr bool
	}{
		{
			name: "full",
			msg: Message{
				Body: json.RawMessage(`{"type": "init", "msg_id": 123, "in_reply_to": 122}`),
			},
			want: CommonBody{
				Type:      "init",
				MsgID:     ptr(uint64(123)),
				InReplyTo: ptr(uint64(122)),
			},
			wantNilErr: true,
		},
		{
			name: "mandatory",
			msg: Message{
				Body: json.RawMessage(`{"type": "init"}`),
			},
			want: CommonBody{
				Type: "init",
			},
			wantNilErr: true,
		},
		{
			name: "no in_reply_to",
			msg: Message{
				Body: json.RawMessage(`{"type": "init", "msg_id": 123}`),
			},
			want: CommonBody{
				Type:  "init",
				MsgID: ptr(uint64(123)),
			},
			wantNilErr: true,
		},
		{
			name: "no msg_id",
			msg: Message{
				Body: json.RawMessage(`{"type": "init", "in_reply_to": 122}`),
			},
			want: CommonBody{
				Type:      "init",
				InReplyTo: ptr(uint64(122)),
			},
			wantNilErr: true,
		},
		{
			name: "extra",
			msg: Message{
				Body: json.RawMessage(`{"type": "init", "msg_id": 123, "in_reply_to": 122, "foo": "bar"}`),
			},
			want: CommonBody{
				Type:      "init",
				MsgID:     ptr(uint64(123)),
				InReplyTo: ptr(uint64(122)),
			},
			wantNilErr: true,
		},
		{
			name: "malformed",
			msg: Message{
				Body: json.RawMessage(`{"type": "init", "msg_id": 123, "in_reply_to": 122"`),
			},
			want:       CommonBody{},
			wantNilErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			common, err := tt.msg.CommonBody()

			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(common, tt.want); diff != "" {
				t.Errorf("unexpected common body (-got +want):\n%v", diff)
			}
		})
	}
}

func TestNode_init(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		want       string
		wantNilErr bool
	}{
		{
			name:       "valid",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}`,
			want:       `{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
			wantNilErr: true,
		},
		{
			name:       "invalid type",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "invalid", "msg_id": 123}}`,
			wantNilErr: false,
		},
		{
			name:       "dest and node_id mismatch",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n2", "node_ids": ["n1", "n2", "n3"]}}`,
			wantNilErr: false,
		},
		{
			name:       "no node_id",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_ids": ["n1", "n2", "n3"]}}`,
			wantNilErr: false,
		},
		{
			name:       "no node_ids",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1"}}`,
			wantNilErr: false,
		},
		{
			name:       "no messages",
			input:      "",
			wantNilErr: false,
		},
		{
			name:       "malformed init message",
			input:      `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}`,
			wantNilErr: false,
		},
		{
			name:       "invalid init body",
			input:      `{"src": "c1", "dest": "n1", "body": "invalid"}`,
			wantNilErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdin := bytes.NewBufferString(tt.input)
			stdout := &bytes.Buffer{}

			n := NewNode(stdin, stdout)

			err := n.init()

			if (err == nil) != tt.wantNilErr {
				t.Errorf("unexpected error: %v", err)
			}

			got := strings.TrimSpace(stdout.String())
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("unexpected cluster (-got +want):\n%v", diff)
			}
		})
	}
}

func TestNode_ID(t *testing.T) {
	const (
		input = `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}`
		want  = "n1"
	)

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	if err := n.init(); err != nil {
		t.Fatalf("init error: %v", err)
	}

	if got := n.ID(); got != want {
		t.Errorf("unexpected ID value: got: %v, want: %v", got, want)
	}
}

func TestNode_Cluster(t *testing.T) {
	const input = `{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}`
	want := []string{"n1", "n2", "n3"}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	if err := n.init(); err != nil {
		t.Fatalf("init error: %v", err)
	}

	got := n.Cluster()

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestNode_Serve_init_error(t *testing.T) {
	const input = `{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124, "k1": "v1"}}`

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	if err := n.Serve(); err == nil {
		t.Error("expected serve error")
	}
}

func TestNode_Serve_HandleFunc(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124, "k1": "v1"}}
		{"src": "c2", "dest": "n1", "body": {"type": "t2", "msg_id": 125, "k2": "v2"}}
	`)
	want := []string{
		"handler1: t1 k1 v1",
		"handler2: t2 k2 v2",
	}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		got []string
	)

	handler := func(name, key string) func(*Node, Message) {
		return func(_ *Node, msg Message) {
			defer wg.Done()

			mu.Lock()
			defer mu.Unlock()

			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				t.Fatalf("%v: unmarshal error: %v", name, err)
			}

			got = append(got, fmt.Sprintf("%v: %v %v %v", name, body["type"], key, body[key]))
		}
	}
	n.HandleFunc("t1", handler("handler1", "k1"))
	n.HandleFunc("t2", handler("handler2", "k2"))

	// Wait for:
	//
	//   - t1 handler
	//   - t2 handler
	wg.Add(2)

	if err := n.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	wg.Wait()

	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected results (-got +want):\n%v", diff)
	}
}

func TestNode_Send(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124}}
	`)
	want := []string{
		`{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","type":"t1_ok"}}`,
	}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	var wg sync.WaitGroup

	n.HandleFunc("t1", func(n *Node, _ Message) {
		defer wg.Done()

		if _, err := n.Send("c1", "t1_ok", map[string]string{"k1": "v1"}); err != nil {
			t.Errorf("handler: send error: %v", err)
		}
	})

	// Wait for t1 handler.
	wg.Add(1)

	if err := n.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	wg.Wait()

	got := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	less := func(a, b Message) bool { return a.String() < b.String() }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestNode_Send_error(t *testing.T) {
	n := NewNode(nil, nil)

	if _, err := n.Send("c1", "t1", "invalid"); err == nil {
		t.Error("expected send error when payload is invalid")
	}
}

func TestNode_Reply(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124}}
	`)
	want := []string{
		`{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
		`{"src":"n1","dest":"c1","body":{"in_reply_to":124,"k1":"v1","msg_id":2,"type":"t1_ok"}}`,
	}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	var wg sync.WaitGroup

	n.HandleFunc("t1", func(n *Node, msg Message) {
		defer wg.Done()

		if _, err := n.Reply(msg, "t1_ok", map[string]string{"k1": "v1"}); err != nil {
			t.Errorf("handler: reply error: %v", err)
		}
	})

	// Wait for t1 handler.
	wg.Add(1)

	if err := n.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	wg.Wait()

	got := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	less := func(a, b Message) bool { return a.String() < b.String() }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestNode_Reply_error(t *testing.T) {
	n := NewNode(nil, nil)

	validMsg := Message{
		Src:  "c1",
		Dest: "n1",
		Body: json.RawMessage(`{"type": "t1", "msg_id": 123}`),
	}
	if _, err := n.Reply(validMsg, "t1_ok", "invalid"); err == nil {
		t.Error("expected reply error when payload is invalid")
	}

	malformedMsg := Message{
		Src:  "c1",
		Dest: "n1",
		Body: json.RawMessage(`{`),
	}
	if _, err := n.Reply(malformedMsg, "t1_ok", map[string]string{"foo": "bar"}); err == nil {
		t.Error("expected reply error when msg is invalid")
	}
}

func TestNode_RPC(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124}}
		{"src": "c1", "dest": "n1", "body": {"type": "t2_ok", "msg_id": 125, "in_reply_to": 2}}
	`)
	want := []string{
		`{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
		`{"src":"n1","dest":"c1","body":{"type":"t3"}}`,
	}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	var wg sync.WaitGroup

	respHandler := func(n *Node, _ Message) {
		defer wg.Done()

		if _, err := n.Send("c1", "t3", nil); err != nil {
			t.Errorf("respHandler: send error: %v", err)
		}
	}

	cont := make(chan bool)

	n.HandleFunc("t1", func(n *Node, _ Message) {
		defer wg.Done()

		if _, err := n.RPC("c1", "t2", map[string]string{"k1": "v1"}, HandlerFunc(respHandler)); err != nil {
			t.Errorf("handler: RPC error: %v", err)
		}
		cont <- true
	})

	testHookNodeServe = func(_ *Node, msg Message) {
		common, err := msg.CommonBody()
		if err != nil {
			t.Fatalf("hook: common body error: %v", err)
		}
		if common.Type == "t2_ok" {
			<-cont
		}
	}

	// Wait for:
	//
	//   - t1 handler
	//   - t2 response handler
	wg.Add(2)

	if err := n.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	wg.Wait()

	if len(n.respHandlers) > 0 {
		t.Errorf("unexpected number of response handlers: %v", len(n.respHandlers))
	}

	got := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	less := func(a, b Message) bool { return a.String() < b.String() }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestNode_RPC_nil_handler(t *testing.T) {
	input := strings.TrimSpace(`
		{"src": "c1", "dest": "n1", "body": {"type": "init", "msg_id": 123, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
		{"src": "c1", "dest": "n1", "body": {"type": "t1", "msg_id": 124}}
		{"src": "c1", "dest": "n1", "body": {"type": "t2_ok", "msg_id": 125, "in_reply_to": 2}}
	`)
	want := []string{
		`{"src":"n1","dest":"c1","body":{"in_reply_to":123,"msg_id":1,"type":"init_ok"}}`,
		`{"src":"n1","dest":"c1","body":{"k1":"v1","msg_id":2,"type":"t2"}}`,
	}

	stdin := bytes.NewBufferString(input)
	stdout := &bytes.Buffer{}

	n := NewNode(stdin, stdout)

	var wg sync.WaitGroup

	cont := make(chan bool)

	n.HandleFunc("t1", func(n *Node, _ Message) {
		defer wg.Done()

		if _, err := n.RPC("c1", "t2", map[string]string{"k1": "v1"}, nil); err != nil {
			t.Errorf("handler: RPC error: %v", err)
		}
		cont <- true
	})

	testHookNodeServe = func(_ *Node, msg Message) {
		common, err := msg.CommonBody()
		if err != nil {
			t.Fatalf("hook: common body error: %v", err)
		}
		if common.Type == "t2_ok" {
			<-cont
		}
	}

	// Wait for t1 handler.
	wg.Add(1)

	if err := n.Serve(); err != nil {
		t.Fatalf("serve error: %v", err)
	}
	wg.Wait()

	if len(n.respHandlers) > 0 {
		t.Errorf("unexpected number of response handlers: %v", len(n.respHandlers))
	}

	got := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	less := func(a, b Message) bool { return a.String() < b.String() }
	if diff := cmp.Diff(got, want, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("unexpected cluster (-got +want):\n%v", diff)
	}
}

func TestNode_RPC_error(t *testing.T) {
	n := NewNode(nil, nil)

	if _, err := n.RPC("c1", "t1", "invalid", nil); err == nil {
		t.Error("expected RPC error when payload is invalid")
	}
}

// cmpMessage compares two messages.
func cmpMessage(a, b Message) bool {
	if a.Src != b.Src {
		return false
	}

	if a.Dest != b.Dest {
		return false
	}

	ma, err := convToMap(a.Body)
	if err != nil {
		return false
	}

	mb, err := convToMap(b.Body)
	if err != nil {
		return false
	}

	return maps.Equal(ma, mb)
}

// ptr returns a pointer to v. It helps to get a pointerq from a
// literal.
func ptr[T any](v T) *T {
	return &v
}
