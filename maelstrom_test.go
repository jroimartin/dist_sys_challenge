// Copyright 2024 Roi Martin

package maelstrom

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

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

func ptr[T any](v T) *T {
	return &v
}
