package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"ntels.com/upm/cfg-distributor/internal/kube"
	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/store"
)

const (
	resultSuccess  = "success"
	resultError    = "error"
	resultDisabled = "disabled"
)

type Reconciler struct {
	kv           nats.KeyValue
	managedLabel kube.ManagedLabel
	interval     time.Duration
	metrics      *metrics.Registry
}

func New(kv nats.KeyValue, managedLabel kube.ManagedLabel, interval time.Duration, registry *metrics.Registry) *Reconciler {
	return &Reconciler{
		kv:           kv,
		managedLabel: managedLabel,
		interval:     interval,
		metrics:      registry,
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	kubeClient, err := kube.NewClient(r.managedLabel)
	if err != nil {
		slog.Warn("reconciler disabled: kube client unavailable", "err", err)
		r.recordRun(resultDisabled)
		return
	}

	if r.interval <= 0 {
		r.interval = 5 * time.Minute
	}

	r.runOnce(ctx, kubeClient)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce(ctx, kubeClient)
		}
	}
}

func (r *Reconciler) runOnce(ctx context.Context, kubeClient *kube.Client) {
	keys, err := r.kv.Keys()
	if err == nats.ErrNoKeysFound {
		r.recordRun(resultSuccess)
		return
	}
	if err != nil {
		slog.Error("reconciler list kv keys failed", "err", err)
		r.recordRun(resultError)
		return
	}

	hasError := false
	for _, key := range keys {
		if key == store.BootstrapSentinelKey {
			continue
		}
		namespace, kind, name, ok := store.ParseKey(key)
		if !ok || kind != "configmap" {
			continue
		}

		entry, err := r.kv.Get(key)
		if err == nats.ErrKeyNotFound {
			continue
		}
		if err != nil {
			hasError = true
			slog.Error("reconciler get kv value failed", "key", key, "err", err)
			continue
		}

		action, err := kubeClient.UpsertConfigMapSingleKey(ctx, namespace, name, string(entry.Value()))
		if err != nil {
			hasError = true
			if action != "" {
				r.recordAction(namespace, string(action), resultError)
			}
			slog.Error("reconciler upsert configmap failed",
				"namespace", namespace,
				"name", name,
				"action", action,
				"err", err,
			)
			continue
		}
		r.recordAction(namespace, string(action), resultSuccess)
	}

	if hasError {
		r.recordRun(resultError)
		return
	}
	r.recordRun(resultSuccess)
}

func (r *Reconciler) recordRun(result string) {
	if r.metrics == nil {
		return
	}
	r.metrics.IncCounter(
		"cfg_distributor_reconciler_runs_total",
		"Total number of reconciler runs.",
		map[string]string{"result": result},
	)
}

func (r *Reconciler) recordAction(namespace, action, result string) {
	if r.metrics == nil {
		return
	}
	r.metrics.IncCounter(
		"cfg_distributor_reconciler_actions_total",
		"Total number of reconciler ConfigMap actions.",
		map[string]string{
			"namespace": namespace,
			"action":    action,
			"result":    result,
		},
	)
}
