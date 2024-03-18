// Copyright 2024 Roi Martin

package maelstrom

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// A Retrier resends requests.
type Retrier struct {
	// node is the cluster node tracking the requests.
	node *Node

	// waitTime specifies how much time to wait before the first
	// retry.
	waitTime time.Duration

	// retryTime specifies how much time to wait between retries.
	retryTime time.Duration

	// mu protects the fields below.
	mu sync.Mutex

	// pending contains the requests waiting for response.
	pending map[uint64]pendingReq
}

// pendingReq represents a request waiting for response.
type pendingReq struct {
	// msg is the original message.
	msg Message

	// sentAt specifies when this request was sent.
	sentAt time.Time
}

// NewRetrier returns a new [Retrier]. It resends the requests using
// the provided node. It waits at least wt before sending the first
// retry. Then, it waits rt between retries.
func NewRetrier(node *Node, wt, rt time.Duration) *Retrier {
	return &Retrier{
		node:      node,
		waitTime:  wt,
		retryTime: rt,
		pending:   make(map[uint64]pendingReq),
	}
}

// Retry starts handling pending requests.
func (r *Retrier) Retry(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.retryTime):
			if err := r.retryReqs(); err != nil {
				log.Printf("maelstrom: retry error: %v", err)
			}
		}
	}
}

// retryReqs retries pending requests.
func (r *Retrier) retryReqs() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, req := range r.pending {
		if time.Since(req.sentAt) < r.waitTime {
			continue
		}
		if err := r.node.Do(req.msg); err != nil {
			return fmt.Errorf("do: %w", err)
		}
	}
	return nil
}

// Add registers a new pending request. Add is safe to call
// concurrently.
func (r *Retrier) Add(req Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	common, err := req.CommonBody()
	if err != nil {
		return fmt.Errorf("common body: %w", err)
	}

	if common.MsgID == nil {
		return errors.New("missing message identifier")
	}

	r.pending[*common.MsgID] = pendingReq{
		msg:    req,
		sentAt: time.Now(),
	}
	return nil
}

// Remove removes the request corresponding to the provided response
// from the list of pending requests. Remove is safe to call
// concurrently.
func (r *Retrier) Remove(resp Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	common, err := resp.CommonBody()
	if err != nil {
		return fmt.Errorf("common body: %w", err)
	}

	if common.InReplyTo == nil {
		return errors.New("missing in-reply-to identifier")
	}

	delete(r.pending, *common.InReplyTo)
	return nil
}
