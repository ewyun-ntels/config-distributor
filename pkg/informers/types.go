package informers

import "github.com/nats-io/nats.go"

type EventType string

const (
	EventPut    EventType = "put"
	EventDelete EventType = "delete"
	EventPurge  EventType = "purge"
)

type Event struct {
	Namespace string
	Kind      string
	Name      string
	Revision  uint64
	Value     []byte
	Type      EventType
	Op        nats.KeyValueOp
}
