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
	slog.Debug("reconciler started", "interval", r.interval)

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
	slog.Debug("reconciler run started")
	keys, err := r.kv.Keys()
	if err == nats.ErrNoKeysFound {
		r.recordRun(resultSuccess)
		slog.Debug("reconciler run completed", "result", resultSuccess, "keys", 0, "actions", 0)
		return
	}
	if err != nil {
		slog.Error("reconciler list kv keys failed", "err", err)
		r.recordRun(resultError)
		return
	}

	hasError := false
	actions := 0
	for _, key := range keys {
		if key == store.BootstrapSentinelKey {
			slog.Debug("reconciler skipped sentinel key", "key", key)
			continue
		}
		namespace, kind, name, ok := store.ParseKey(key)
		if !ok || kind != "configmap" {
			slog.Debug("reconciler skipped unsupported key", "key", key)
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
			if action != "" {
				r.recordAction(namespace, string(action), resultError)
				actions++
			}
			if action == kube.ConfigMapActionConflict {
				slog.Warn("reconciler skipped unmanaged configmap conflict",
					"namespace", namespace,
					"name", name,
					"err", err,
				)
				continue
			}
			hasError = true
			slog.Error("reconciler upsert configmap failed",
				"namespace", namespace,
				"name", name,
				"action", action,
				"err", err,
			)
			continue
		}
		r.recordAction(namespace, string(action), resultSuccess)
		actions++
		slog.Debug("reconciler applied configmap",
			"namespace", namespace,
			"name", name,
			"action", action,
			"revision", entry.Revision(),
		)
	}

	if hasError {
		r.recordRun(resultError)
		slog.Debug("reconciler run completed", "result", resultError, "keys", len(keys), "actions", actions)
		return
	}
	r.recordRun(resultSuccess)
	slog.Debug("reconciler run completed", "result", resultSuccess, "keys", len(keys), "actions", actions)
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
