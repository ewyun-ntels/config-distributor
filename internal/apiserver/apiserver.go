package apiserver

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	urlruntime "k8s.io/apimachinery/pkg/util/runtime"

	"ntels.com/upm/cfg-distributor/internal/api/resources/v1alpha1"
	"ntels.com/upm/cfg-distributor/internal/config"
	"ntels.com/upm/cfg-distributor/internal/kube"
	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/reconciler"
	"ntels.com/upm/cfg-distributor/internal/store"
)

const (
	resourceConfigMap = "configmap"

	resultSuccess = "success"
	resultError   = "error"

	reasonMultiKeyOrEmpty = "multi_key_or_empty"
)

type APIServer struct {
	container *restful.Container
	kv        nats.KeyValue
	nc        *nats.Conn
	cache     *store.ResourceCache
	metrics   *metrics.Registry

	port                int
	bootstrapNamespaces []string
	managedLabel        kube.ManagedLabel
	reconcilerEnabled   bool
	reconcilerInterval  time.Duration
}

func New(cfg *config.Config) (*APIServer, error) {
	natsURL := cfg.NATS.URL
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	kv, err := ensureKV(js, cfg.NATS.Bucket)
	if err != nil {
		return nil, err
	}

	slog.Info("init distributor", "nats_url", natsURL, "bucket", cfg.NATS.Bucket)

	s := &APIServer{
		container:           restful.NewContainer(),
		kv:                  kv,
		nc:                  nc,
		cache:               store.NewResourceCache(),
		metrics:             metrics.NewRegistry(),
		port:                cfg.Server.Port,
		bootstrapNamespaces: normalizeNamespaces(cfg.Bootstrap.Namespaces),
		managedLabel: kube.ManagedLabel{
			Key:   cfg.Filter.ManagedLabel.Key,
			Value: cfg.Filter.ManagedLabel.Value,
		},
		reconcilerEnabled:  cfg.Reconciler.Enabled,
		reconcilerInterval: time.Duration(cfg.Reconciler.IntervalSeconds) * time.Second,
	}
	return s, nil
}

func (s *APIServer) PrepareRun() error {
	s.container.Router(restful.CurlyRouter{})
	s.installAPIs()
	s.installHealthz()
	return nil
}

func (s *APIServer) installAPIs() {
	urlruntime.Must(v1alpha1.AddToContainer(s.container, s.kv, s.cache, s.metrics))
}

func (s *APIServer) installHealthz() {
	ws := new(restful.WebService)
	ws.Path("")
	healthHandler := func(_ *restful.Request, resp *restful.Response) {
		resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
		resp.WriteHeader(http.StatusOK)
		_, _ = resp.Write([]byte("ok\n"))
	}
	ws.Route(ws.GET("/healthz").To(healthHandler))
	ws.Route(ws.GET("/health").To(healthHandler))
	ws.Route(ws.GET("/metrics").To(func(req *restful.Request, resp *restful.Response) {
		s.metrics.Handler().ServeHTTP(resp.ResponseWriter, req.Request)
	}))
	s.container.Add(ws)
}

func (s *APIServer) Run(ctx context.Context) error {
	if err := s.PrepareRun(); err != nil {
		return err
	}
	slog.Info("start distributor", "bootstrap_namespaces", s.bootstrapNamespaces)

	if err := s.bootstrapIfNeeded(ctx); err != nil {
		return err
	}
	if err := s.preloadCacheFromKV(); err != nil {
		return err
	}
	s.startReconciler(ctx)

	addr := fmt.Sprintf(":%d", s.port)
	slog.Info("distributor api listening", "addr", addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           s.container,
		ReadHeaderTimeout: 30 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = s.nc.Drain()
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (s *APIServer) bootstrapIfNeeded(ctx context.Context) error {
	_, err := s.kv.Get(store.BootstrapSentinelKey)
	if err == nil {
		slog.Info("bootstrap skipped: sentinel exists", "key", store.BootstrapSentinelKey)
		return nil
	}
	if err != nats.ErrKeyNotFound {
		return fmt.Errorf("check bootstrap sentinel: %w", err)
	}

	hasNormalKeys, err := s.hasNormalKVKeys()
	if err != nil {
		return err
	}
	if hasNormalKeys {
		return fmt.Errorf("KV has data without bootstrap sentinel %q; manual migration required", store.BootstrapSentinelKey)
	}

	kubeClient, err := kube.NewClient(s.managedLabel)
	if err != nil {
		return fmt.Errorf("init kube client for bootstrap: %w", err)
	}

	for _, namespace := range s.bootstrapNamespaces {
		list, err := kubeClient.ClientSet().CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: kubeClient.ManagedLabelSelector(),
		})
		if err != nil {
			return fmt.Errorf("list bootstrap configmaps in namespace %q: %w", namespace, err)
		}

		for i := range list.Items {
			cm := &list.Items[i]
			value, ok := kube.ValueFromConfigMap(cm)
			if !ok {
				slog.Warn("skip bootstrap configmap with empty or multi-key data", "namespace", namespace, "name", cm.Name)
				s.recordBootstrapSkipped(namespace, reasonMultiKeyOrEmpty)
				continue
			}
			key := store.KeyFor(namespace, resourceConfigMap, cm.Name)
			if _, err := s.kv.Put(key, []byte(value)); err != nil {
				s.recordBootstrapSeeded(namespace, resultError)
				return fmt.Errorf("seed bootstrap configmap %q: %w", key, err)
			}
			s.recordBootstrapSeeded(namespace, resultSuccess)
			slog.Info("bootstrap configmap seeded", "key", key)
		}
	}

	if _, err := s.kv.Put(store.BootstrapSentinelKey, []byte(time.Now().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("write bootstrap sentinel: %w", err)
	}
	slog.Info("bootstrap completed", "sentinel", store.BootstrapSentinelKey)
	return nil
}

func (s *APIServer) hasNormalKVKeys() (bool, error) {
	keys, err := s.kv.Keys()
	if err == nats.ErrNoKeysFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("list kv keys for bootstrap state: %w", err)
	}
	for _, key := range keys {
		if key != store.BootstrapSentinelKey {
			return true, nil
		}
	}
	return false, nil
}

func (s *APIServer) preloadCacheFromKV() error {
	keys, err := s.kv.Keys()
	if err == nats.ErrNoKeysFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list kv keys for cache preload: %w", err)
	}

	loaded := 0
	for _, key := range keys {
		if key == store.BootstrapSentinelKey {
			continue
		}
		namespace, kind, name, ok := store.ParseKey(key)
		if !ok || kind != resourceConfigMap {
			continue
		}
		entry, err := s.kv.Get(key)
		if err == nats.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("get kv key %q for cache preload: %w", key, err)
		}
		s.cache.Upsert(namespace, kind, name, store.CachedValue{
			Revision: entry.Revision(),
			Value:    string(entry.Value()),
		})
		loaded++
	}
	slog.Info("cache preload completed", "items", loaded)
	return nil
}

func (s *APIServer) startReconciler(ctx context.Context) {
	if !s.reconcilerEnabled {
		slog.Info("reconciler disabled by config")
		return
	}
	r := reconciler.New(s.kv, s.managedLabel, s.reconcilerInterval, s.metrics)
	go r.Run(ctx)
}

func (s *APIServer) recordBootstrapSeeded(namespace, result string) {
	s.metrics.IncCounter(
		"cfg_distributor_bootstrap_seeded_total",
		"Total number of ConfigMaps seeded during bootstrap.",
		map[string]string{
			"namespace": namespace,
			"result":    result,
		},
	)
}

func (s *APIServer) recordBootstrapSkipped(namespace, reason string) {
	s.metrics.IncCounter(
		"cfg_distributor_bootstrap_skipped_total",
		"Total number of ConfigMaps skipped during bootstrap.",
		map[string]string{
			"namespace": namespace,
			"reason":    reason,
		},
	)
}

func normalizeNamespaces(namespaces []string) []string {
	out := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		v := strings.TrimSpace(namespace)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return []string{"default"}
	}
	return out
}

func ensureKV(js nats.JetStreamContext, bucketName string) (nats.KeyValue, error) {
	if kv, err := js.KeyValue(bucketName); err == nil {
		return kv, nil
	}
	return js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucketName,
		History: 5,
	})
}
