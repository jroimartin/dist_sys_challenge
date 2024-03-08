// Package maelstrom implements the [Maelstrom] protocol.
//
// [Maelstrom]: https://github.com/jepsen-io/maelstrom/
package maelstrom

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// A Handler responds to a Maelstrom message.
type Handler interface {
	ServeMessage(Message)
}

// The HandlerFunc type is an adapter to allow the use of ordinary
// functions as Maelstrom handlers. If f is a function with the
// appropriate signature, HandlerFunc(f) is a Handler that calls f.
type HandlerFunc func(Message)

// ServeMessage calls f(msg).
func (f HandlerFunc) ServeMessage(msg Message) {
	f(msg)
}

// Messages are exchanged between clients and nodes in the Maelstrom
// cluster.
type Message struct {
	// Src identifies the node this message came from.
	Src string `json:"src"`

	// Dest identifies the node this message is to.
	Dest string `json:"dest"`

	// Body is the payload of the message.
	Body json.RawMessage `json:"body"`
}

// CommonBody contains the fields common to all messages.
type CommonBody struct {
	// Type identifies message type.
	Type string `json:"type"`

	// MsgID is a unique integer identifier.
	MsgID *uint64 `json:"msg_id,omitempty"`

	// InReplyTo is the msg_id of the request in a req/response
	// context.
	InReplyTo *uint64 `json:"in_reply_to,omitempty"`
}

// ErrorBody represents the body of an error message.
type ErrorBody struct {
	CommonBody

	// Code indicates the type of error which occurred.
	Code uint64 `json:"code"`

	// Text is optional and may contain an explanatory message.
	Text *string `json:"text"`
}

// InitBody represents the body of an init message.
type InitBody struct {
	CommonBody

	// NodeID is the identifier assigned to the node.
	NodeID string `json:"node_id"`

	// NodeIDs contains the identifiers of all the node in the
	// cluster.
	NodeIDs []string `json:"node_ids"`
}

// CommonBody returns the fields that are common to all message types.
func (msg Message) CommonBody() (CommonBody, error) {
	var body CommonBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return CommonBody{}, fmt.Errorf("JSON unmarshal: %w", err)
	}
	return body, nil
}

// Recv receives a message.
func Recv() (Message, error) {
	var msg Message
	if err := json.NewDecoder(os.Stdin).Decode(&msg); err != nil {
		return Message{}, fmt.Errorf("JSON decode: %w", err)
	}
	return msg, nil
}

// Send sends a message.
func Send(msg Message) error {
	if err := json.NewEncoder(os.Stdout).Encode(msg); err != nil {
		return fmt.Errorf("JSON encode: %w", err)
	}
	return nil
}

// Node represents a node of the cluster.
type Node struct {
	// id is the identifier assigned to the node.
	id string

	// cluster contains the identifiers of all the nodes in the
	// cluster.
	cluster []string

	// handlers contains the registered message handlers.
	handlers map[string]Handler

	// mu protects the fields below.
	mu sync.Mutex

	// msgID is the identifier of the next message sent by the
	// node. It is monotonically increased.
	msgID uint64
}

// NewNode initializes a new node in the cluster.
func NewNode() (*Node, error) {
	// Wait for init message.
	req, err := Recv()
	if err != nil {
		return nil, fmt.Errorf("recv: %w", err)
	}

	var reqBody InitBody
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return nil, fmt.Errorf("received message before init: %v", req)
	}

	// Reply with init_ok message.
	msgID := uint64(0)
	respBody := CommonBody{
		Type:      "init_ok",
		MsgID:     &msgID,
		InReplyTo: reqBody.MsgID,
	}
	jsonRespBody, err := json.Marshal(respBody)
	if err != nil {
		return nil, fmt.Errorf("JSON marshal: %w", err)
	}
	resp := Message{
		Src:  reqBody.NodeID,
		Dest: req.Src,
		Body: jsonRespBody,
	}
	if err := Send(resp); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	n := &Node{
		id:       reqBody.NodeID,
		cluster:  reqBody.NodeIDs,
		msgID:    1,
		handlers: make(map[string]Handler),
	}
	return n, nil
}

// ID returns the identifier of the node.
func (n *Node) ID() string {
	return n.id
}

// Cluster returns the identifiers of all the nodes in the cluster.
func (n *Node) Cluster() []string {
	return n.cluster
}

// Send sends a message to dest with the specified type and payload.
func (n *Node) Send(dest, typ string, payload any) error {
	_, err := n.do(dest, typ, payload, nil)
	return err
}

// Reply replies to the specified request with a message of the
// provided type and payload.
func (n *Node) Reply(req Message, typ string, payload any) error {
	common, err := req.CommonBody()
	if err != nil {
		return fmt.Errorf("decode common body: %w", err)
	}
	_, err = n.do(req.Src, typ, payload, common.MsgID)
	return err
}

// do sends a message to dest with the specified type, payload and
// "in_reply_to" field. It returns the message identifier of the
// request.
func (n *Node) do(dest, typ string, payload any, inReplyTo *uint64) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode payload: %w", err)
	}

	msgID := n.msgID
	var body map[string]any
	if err := json.Unmarshal(jsonPayload, &body); err != nil {
		return 0, fmt.Errorf("decode body: %w", err)
	}
	body["type"] = typ
	body["in_reply_to"] = inReplyTo
	body["msg_id"] = msgID
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("encode updated body: %w", err)
	}

	msg := Message{
		Src:  n.id,
		Dest: dest,
		Body: jsonBody,
	}
	if err := Send(msg); err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}
	n.msgID++

	return msgID, nil
}

// Handle registers the handler for messages of specified type. If typ
// is an empty string, all message types are matched.
func (n *Node) Handle(typ string, handler Handler) {
	n.handlers[typ] = handler
}

// HandleFunc registers the handler function for messages of specified
// type. If typ is an empty string, all message types are matched.
func (n *Node) HandleFunc(typ string, handler HandlerFunc) {
	n.handlers[typ] = handler
}

// Serve starts serving messages.
func (n *Node) Serve() error {
	for {
		msg, err := Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}

		if h, ok := n.handlers[""]; ok {
			go h.ServeMessage(msg)
			continue
		}

		common, err := msg.CommonBody()
		if err != nil {
			return fmt.Errorf("decode common body: %w", err)
		}
		if h, ok := n.handlers[common.Type]; ok {
			go h.ServeMessage(msg)
		}
	}
}
