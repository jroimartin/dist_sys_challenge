// Copyright 2024 Roi Martin

// Package maelstrom implements the [Maelstrom] protocol.
//
// [Maelstrom]: https://github.com/jepsen-io/maelstrom/
package maelstrom

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"sync/atomic"
)

// A Handler responds to a Maelstrom message.
type Handler interface {
	ServeMessage(*Node, Message)
}

// The HandlerFunc type is an adapter to allow the use of ordinary
// functions as Maelstrom handlers. If f is a function with the
// appropriate signature, HandlerFunc(f) is a [Handler] that calls f.
type HandlerFunc func(*Node, Message)

// ServeMessage calls f(msg).
func (f HandlerFunc) ServeMessage(n *Node, msg Message) {
	f(n, msg)
}

// Message represents a message exchanged between clients and nodes in
// the Maelstrom cluster.
type Message struct {
	// Src identifies the node this message came from.
	Src string `json:"src"`

	// Dest identifies the node this message is to.
	Dest string `json:"dest"`

	// Body is the payload of the message.
	Body json.RawMessage `json:"body"`
}

// CommonBody contains the fields present in all messages.
type CommonBody struct {
	// Type identifies the message type.
	Type string `json:"type"`

	// MsgID is a unique integer identifier.
	MsgID *uint64 `json:"msg_id,omitempty"`

	// InReplyTo is the identifier of the request in a
	// request/response context.
	InReplyTo *uint64 `json:"in_reply_to,omitempty"`
}

// ErrorBody represents the body of an "error" message.
type ErrorBody struct {
	CommonBody

	// Code indicates the type of error which occurred.
	Code uint64 `json:"code"`

	// Text is optional and may contain an explanatory message.
	Text *string `json:"text,omitempty"`
}

// initBody represents the body of an "init" message.
type initBody struct {
	CommonBody

	// NodeID is the identifier assigned to the node.
	NodeID string `json:"node_id"`

	// NodeIDs contains the identifiers of all the nodes in the
	// cluster.
	NodeIDs []string `json:"node_ids"`
}

// NewMessage creates a new message. msgID and inReplyTo are optional
// fields.
func NewMessage(dest, src, typ string, payload any, msgID, inReplyTo *uint64) (Message, error) {
	common := CommonBody{
		Type:      typ,
		MsgID:     msgID,
		InReplyTo: inReplyTo,
	}
	body, err := flatten(payload, common)
	if err != nil {
		return Message{}, fmt.Errorf("flatten: %w", err)
	}
	msg := Message{
		Src:  src,
		Dest: dest,
		Body: body,
	}
	return msg, nil
}

// CommonBody returns the fields present in all message types.
func (msg Message) CommonBody() (CommonBody, error) {
	var body CommonBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return CommonBody{}, fmt.Errorf("unmarshal: %w", err)
	}
	return body, nil
}

// String returns the string representation of the message.
func (msg Message) String() string {
	return fmt.Sprintf(`{"src": %q, "dest": %q, body: %q}`, msg.Src, msg.Dest, msg.Body)
}

// Node represents a cluster node.
type Node struct {
	// id is the identifier assigned to the node.
	id string

	// cluster contains the identifiers of all the nodes in the
	// cluster.
	cluster []string

	// handlers contains the registered message handlers.
	handlers map[string]Handler

	// scanner is used to read Maelstrom messages.
	scanner *bufio.Scanner

	// msgID is the identifier of the next message. It is
	// monotonically increased.
	msgID atomic.Uint64

	// mu protects the fields below.
	mu *sync.Mutex

	// enc is used to encode Maelstrom messages.
	enc *json.Encoder

	// respHandlers contains the registered RPC response handlers.
	respHandlers map[uint64]Handler
}

// NewNode initializes a new cluster node. r and w are used to receive
// and send Maelstrom messages.
func NewNode(r io.Reader, w io.Writer) *Node {
	return &Node{
		handlers:     make(map[string]Handler),
		scanner:      bufio.NewScanner(r),
		mu:           new(sync.Mutex),
		enc:          json.NewEncoder(w),
		respHandlers: make(map[uint64]Handler),
	}
}

// ID returns the identifier of the node. It returns an empty string
// if the node has not been initialized.
func (n *Node) ID() string {
	return n.id
}

// Cluster returns the identifiers of all the nodes in the cluster. It
// returns nil if the node has not been initialized.
func (n *Node) Cluster() []string {
	return n.cluster
}

// Handle registers the handler for messages of the specified type.
func (n *Node) Handle(typ string, handler Handler) {
	n.handlers[typ] = handler
}

// HandleFunc registers the handler function for messages of the
// specified type.
func (n *Node) HandleFunc(typ string, handler HandlerFunc) {
	n.Handle(typ, handler)
}

// testHookNodeServe is executed after every message received by the
// Node if not nil. It is set by tests.
var testHookNodeServe func(*Node, Message)

// Serve starts serving messages.
func (n *Node) Serve() error {
	if err := n.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	for n.scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(n.scanner.Bytes(), &msg); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}

		if testHookNodeServe != nil {
			testHookNodeServe(n, msg)
		}

		common, err := msg.CommonBody()
		if err != nil {
			return fmt.Errorf("common body: %w", err)
		}
		if common.InReplyTo != nil {
			n.handleResp(*common.InReplyTo, msg)
			continue
		}
		if h, ok := n.handlers[common.Type]; ok && h != nil {
			go h.ServeMessage(n, msg)
		}
	}
	if err := n.scanner.Err(); err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	return nil
}

// init initializes the node.
func (n *Node) init() error {
	// Receive init message.
	if !n.scanner.Scan() {
		if err := n.scanner.Err(); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		return errors.New("scan: no more messages")
	}

	var req Message
	if err := json.Unmarshal(n.scanner.Bytes(), &req); err != nil {
		return fmt.Errorf("unmarshal request: %w", err)
	}

	var reqBody initBody
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return fmt.Errorf("unmarshal response body: %w", err)
	}

	if reqBody.Type != "init" {
		return fmt.Errorf("unexpected message type: %v", reqBody.Type)
	}

	if reqBody.NodeID == "" {
		return fmt.Errorf("missing node_id")
	}

	if reqBody.NodeIDs == nil {
		return fmt.Errorf("missing node_ids")
	}

	if req.Dest != reqBody.NodeID {
		return fmt.Errorf("dest and node_id mismatch")
	}

	// Send init_ok message.
	msgID := n.msgID.Add(1)
	resp, err := NewMessage(req.Src, reqBody.NodeID, "init_ok", nil, &msgID, reqBody.MsgID)
	if err != nil {
		return fmt.Errorf("new response: %w", err)
	}
	if err := n.enc.Encode(resp); err != nil {
		return fmt.Errorf("encode response: %w", err)
	}

	n.id = reqBody.NodeID
	n.cluster = reqBody.NodeIDs
	return nil
}

// handleResp handles the response corresponding to the specified
// request identifier.
func (n *Node) handleResp(reqID uint64, resp Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	h, ok := n.respHandlers[reqID]
	if !ok {
		return
	}
	delete(n.respHandlers, reqID)

	if h != nil {
		go h.ServeMessage(n, resp)
	}
}

// Send sends a fire-and-forget message (without identifier) to dest
// with the specified type and payload. It returns the message that
// has been sent. Send is safe to call concurrently.
func (n *Node) Send(dest, typ string, payload any) (Message, error) {
	msg, err := NewMessage(dest, n.id, typ, payload, nil, nil)
	if err != nil {
		return Message{}, fmt.Errorf("new message: %w", err)
	}
	if err := n.Do(msg); err != nil {
		return Message{}, fmt.Errorf("do: %w", err)
	}
	return msg, nil
}

// Reply replies to the specified request with a message with the
// provided type and payload. It returns the reply that has been
// sent. Reply is safe to call concurrently.
func (n *Node) Reply(req Message, typ string, payload any) (Message, error) {
	common, err := req.CommonBody()
	if err != nil {
		return Message{}, fmt.Errorf("common body: %w", err)
	}
	msgID := n.msgID.Add(1)
	msg, err := NewMessage(req.Src, n.id, typ, payload, &msgID, common.MsgID)
	if err != nil {
		return Message{}, fmt.Errorf("new message: %w", err)
	}
	if err := n.Do(msg); err != nil {
		return Message{}, fmt.Errorf("do: %w", err)
	}
	return msg, nil
}

// RPC sends an RPC request to dest with the specified type and
// payload. The provided handler is executed when the RPC response is
// received. It returns the RPC request that has been sent. RPC is
// safe to call concurrently.
func (n *Node) RPC(dest, typ string, payload any, handler Handler) (Message, error) {
	msgID := n.msgID.Add(1)
	msg, err := NewMessage(dest, n.id, typ, payload, &msgID, nil)
	if err != nil {
		return Message{}, fmt.Errorf("new message: %w", err)
	}
	if err := n.Do(msg); err != nil {
		return Message{}, fmt.Errorf("do: %w", err)
	}

	n.mu.Lock()
	n.respHandlers[msgID] = handler
	n.mu.Unlock()

	return msg, nil
}

// Do sends a message. Do is safe to call concurrently.
func (n *Node) Do(msg Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.enc.Encode(msg); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return nil
}

// convToMap uses JSON encoding to convert the provided value to
// map[string]any.
func convToMap(v any) (map[string]any, error) {
	if v == nil {
		return map[string]any{}, nil
	}

	if m, ok := v.(map[string]any); ok {
		return m, nil
	}

	jsonV, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(jsonV, &m); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return m, nil
}

// flatten marshals the provided values after flattening them using
// JSON encoding. When a key in src is already present in dst, the
// value in dst will be overwritten by the value associated with the
// key in src.
func flatten(dst, src any) (json.RawMessage, error) {
	mdst, err := convToMap(dst)
	if err != nil {
		return nil, fmt.Errorf("convert dst to map: %w", err)
	}
	msrc, err := convToMap(src)
	if err != nil {
		return nil, fmt.Errorf("convert src to map: %w", err)
	}
	maps.Copy(mdst, msrc)
	raw, err := json.Marshal(mdst)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return raw, nil
}
