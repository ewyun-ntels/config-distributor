package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
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
			slog.Info("snapshot", "key", e.Key(), "rev", e.Revision(), "value", string(e.Value()))
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
			slog.Info(
				"update",
				"key", e.Key(),
				"rev", e.Revision(),
				"op", e.Operation(),
				"value", string(e.Value()),
			)
		}
	}
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
