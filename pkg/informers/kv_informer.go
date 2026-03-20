package informers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
)

type KVInformer struct {
	kv        nats.KeyValue
	namespace string
	kind      string
	prefix    string

	out  chan Event
	once sync.Once
	wch  nats.KeyWatcher
}

func NewConfigMapInformer(kv nats.KeyValue, namespace string) *KVInformer {
	return newKVInformer(kv, namespace, "configmap")
}

func NewSecretInformer(kv nats.KeyValue, namespace string) *KVInformer {
	return newKVInformer(kv, namespace, "secret")
}

func newKVInformer(kv nats.KeyValue, namespace, kind string) *KVInformer {
	prefix := fmt.Sprintf("namespaces/%s/%s/", namespace, kind)
	return &KVInformer{
		kv:        kv,
		namespace: namespace,
		kind:      kind,
		prefix:    prefix,
		out:       make(chan Event, 128),
	}
}

// Start begins watching KV changes and emits events to Events().
// It returns when the watch is successfully started.
func (i *KVInformer) Start(ctx context.Context) error {
	wch, err := i.kv.Watch(i.prefix, nats.Context(ctx))
	if err != nil {
		return err
	}
	i.wch = wch

	go func() {
		defer i.closeOut()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-wch.Updates():
				if !ok {
					return
				}
				if entry == nil {
					// End of initial snapshot.
					continue
				}
				name := strings.TrimPrefix(entry.Key(), i.prefix)
				evt := Event{
					Namespace: i.namespace,
					Kind:      i.kind,
					Name:      name,
					Revision:  entry.Revision(),
					Value:     entry.Value(),
					Op:        entry.Operation(),
					Type:      mapOp(entry.Operation()),
				}
				select {
				case i.out <- evt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return nil
}

func (i *KVInformer) Events() <-chan Event {
	return i.out
}

// Stop halts the watcher and closes the Events channel.
func (i *KVInformer) Stop() {
	if i.wch != nil {
		i.wch.Stop()
	}
	i.closeOut()
}

func (i *KVInformer) closeOut() {
	i.once.Do(func() {
		close(i.out)
	})
}

func mapOp(op nats.KeyValueOp) EventType {
	switch op {
	case nats.KeyValuePut:
		return EventPut
	case nats.KeyValueDelete:
		return EventDelete
	case nats.KeyValuePurge:
		return EventPurge
	default:
		return EventPut
	}
}
