package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/nats-io/nats.go"
)

const (
	defaultBucket      = "UPM_CONFIG"
	defaultWatchPrefix = "namespaces/default/configmap/*"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	natsURL := getenv("NATS_URL", nats.DefaultURL)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		slog.Error("connect", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream", "err", err)
		os.Exit(1)
	}

	bucket := getenv("KV_BUCKET", defaultBucket)
	watchPrefix := getenv("WATCH_PREFIX", defaultWatchPrefix)
	slog.Info("config", "nats_url", natsURL, "bucket", bucket, "watch_prefix", watchPrefix)

	kv, err := ensureKV(js, bucket)
	if err != nil {
		slog.Error("ensure kv", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Get current snapshot immediately (prefix filtered)
	matchPrefix := normalizeWatchPrefix(watchPrefix)
	keys, err := kv.Keys()
	if err == nil {
		for _, k := range keys {
			if matchPrefix != "" && !strings.HasPrefix(k, matchPrefix) {
				continue
			}
			e, err := kv.Get(k)
			if err != nil {
				continue
			}
			logEntry("snapshot", e.Key(), e.Revision(), "", e.Value())
		}
	}

	// Watch ongoing changes. For "/"-separated keys, NATS subject wildcards won't match as expected.
	// If a wildcard is used, fall back to WatchAll and filter by prefix.
	var wch nats.KeyWatcher
	if strings.ContainsAny(watchPrefix, "*>") {
		wch, err = kv.WatchAll()
	} else {
		wch, err = kv.Watch(watchPrefix)
	}
	if err != nil {
		slog.Error("watch", "err", err)
		os.Exit(1)
	}
	defer wch.Stop()

	slog.Info("watching", "prefix", watchPrefix, "bucket", bucket)
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown")
			return
		case e := <-wch.Updates():
			if e == nil {
				continue
			}
			if matchPrefix != "" && !strings.HasPrefix(e.Key(), matchPrefix) {
				continue
			}
			logEntry("update", e.Key(), e.Revision(), e.Operation().String(), e.Value())
		}
	}
}

func logEntry(msg, key string, rev uint64, op string, value []byte) {
	args := []any{"key", key, "rev", rev}
	if op != "" {
		args = append(args, "op", op)
	}
	slog.Info(msg, args...)
	rendered := renderValue(key, value)
	if rendered == "" {
		return
	}
	fmt.Println(rendered)
}

func ensureKV(js nats.JetStreamContext, bucket string) (nats.KeyValue, error) {
	if kv, err := js.KeyValue(bucket); err == nil {
		return kv, nil
	}
	return js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucket,
		History: 5,
	})
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func normalizeWatchPrefix(prefix string) string {
	if strings.HasSuffix(prefix, ">") {
		prefix = strings.TrimSuffix(prefix, ">")
	}
	if strings.HasSuffix(prefix, "*") {
		prefix = strings.TrimSuffix(prefix, "*")
	}
	return prefix
}

func renderValue(key string, value []byte) string {
	if len(value) == 0 {
		return ""
	}

	switch kindFromKey(key) {
	case "configmap":
		return renderConfigMapValue(value)
	case "secret":
		return renderSecretValue(value)
	default:
		return string(value)
	}
}

func kindFromKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return ""
	}
	return parts[2]
}

func renderConfigMapValue(value []byte) string {
	value = extractStoredData(value)

	type configMapEnvelope struct {
		Format string            `json:"format,omitempty"`
		Data   map[string]string `json:"data,omitempty"`
	}

	var env configMapEnvelope
	if err := json.Unmarshal(value, &env); err == nil && len(env.Data) > 0 {
		return renderStringMap(env.Data)
	}

	var legacy map[string]string
	if err := json.Unmarshal(value, &legacy); err == nil && len(legacy) > 0 {
		return renderStringMap(legacy)
	}

	return string(value)
}

func renderSecretValue(value []byte) string {
	value = extractStoredData(value)

	type secretEnvelope struct {
		Format     string            `json:"format,omitempty"`
		StringData map[string]string `json:"stringData,omitempty"`
		BinaryData map[string]string `json:"binaryData,omitempty"`
	}

	var env secretEnvelope
	if err := json.Unmarshal(value, &env); err == nil && (len(env.StringData) > 0 || len(env.BinaryData) > 0) {
		parts := make([]string, 0, len(env.StringData)+len(env.BinaryData))
		if len(env.StringData) > 0 {
			parts = append(parts, renderStringMap(env.StringData))
		}
		if len(env.BinaryData) > 0 {
			parts = append(parts, renderBinaryMap(env.BinaryData))
		}
		return strings.Join(parts, "\n")
	}

	var legacy map[string]string
	if err := json.Unmarshal(value, &legacy); err == nil && len(legacy) > 0 {
		return renderStringMap(legacy)
	}

	return string(value)
}

func renderStringMap(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("%s: %s", key, data[key]))
	}
	return strings.Join(lines, "\n")
}

func renderBinaryMap(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, err := base64.StdEncoding.DecodeString(data[key]); err == nil {
			lines = append(lines, fmt.Sprintf("%s: !!binary %s", key, data[key]))
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s", key, data[key]))
	}
	return strings.Join(lines, "\n")
}

func extractStoredData(value []byte) []byte {
	type storedValueEnvelope struct {
		Data json.RawMessage `json:"data,omitempty"`
	}

	var env storedValueEnvelope
	if err := json.Unmarshal(value, &env); err == nil && len(env.Data) > 0 {
		return env.Data
	}
	return value
}
