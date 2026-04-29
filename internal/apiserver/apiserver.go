package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	urlruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"ntels.com/upm/cfg-distributor/internal/api/resources/v1alpha1"
	"ntels.com/upm/cfg-distributor/internal/config"
	"ntels.com/upm/cfg-distributor/internal/kube"
	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/store"
)

type APIServer struct {
	container *restful.Container
	kv        nats.KeyValue
	nc        *nats.Conn
	kube      *kube.Client
	cache     *store.ResourceCache
	metrics   *metrics.Registry

	port            int
	watchNamespaces []string
	watchResources  []string // singular forms: "configmap", "secret"
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

	kubeClient, err := kube.NewClient(kube.ManagedLabel{
		Key:   cfg.Filter.ManagedLabel.Key,
		Value: cfg.Filter.ManagedLabel.Value,
	})
	if err != nil {
		return nil, err
	}

	slog.Info("init distributor", "nats_url", natsURL, "bucket", cfg.NATS.Bucket)

	s := &APIServer{
		container:       restful.NewContainer(),
		kv:              kv,
		nc:              nc,
		kube:            kubeClient,
		cache:           store.NewResourceCache(),
		metrics:         metrics.NewRegistry(),
		port:            cfg.Server.Port,
		watchNamespaces: normalizeNamespaces(cfg.Watch.Namespaces),
		watchResources:  watchResourcesFromConfig(cfg.Watch.Resources),
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
	urlruntime.Must(v1alpha1.AddToContainer(s.container, s.kv, s.kube, s.cache))
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
	slog.Info("start distributor", "namespaces", s.watchNamespaces, "resources", s.watchResources)
	if err := s.startKubeWatchers(ctx); err != nil {
		return err
	}

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
		return err
	}
}

func (s *APIServer) putIfChangedByRV(key string, value []byte, resourceVersion string) error {
	if resourceVersion != "" {
		entry, err := s.kv.Get(key)
		if err == nil {
			// API writes may already have stored the same Kubernetes state in KV.
			// Skip the informer-driven write when the observed resourceVersion is unchanged.
			if kube.ExtractResourceVersion(entry.Value()) == resourceVersion {
				return nil
			}
		} else if err != nats.ErrKeyNotFound {
			return err
		}
	} else {
		// Fallback when resourceVersion is unavailable.
		entry, err := s.kv.Get(key)
		if err == nil {
			if bytes.Equal(entry.Value(), value) {
				return nil
			}
		} else if err != nats.ErrKeyNotFound {
			return err
		}
	}

	rev, err := s.kv.Put(key, value)
	if err != nil {
		return err
	}
	s.updateCacheForKey(key, rev, string(value))
	return nil
}

func (s *APIServer) deleteIfExists(key string) error {
	_, err := s.kv.Get(key)
	if err == nats.ErrKeyNotFound {
		s.deleteCacheForKey(key)
		return nil
	}
	if err != nil {
		return err
	}

	if err := s.kv.Delete(key); err != nil && err != nats.ErrKeyNotFound {
		return err
	}
	s.deleteCacheForKey(key)
	return nil
}

func (s *APIServer) updateCacheForKey(key string, revision uint64, value string) {
	namespace, kind, name, ok := store.ParseKey(key)
	if !ok {
		return
	}
	s.cache.Upsert(namespace, kind, name, store.CachedValue{
		Revision: revision,
		Value:    value,
	})
}

func (s *APIServer) deleteCacheForKey(key string) {
	namespace, kind, name, ok := store.ParseKey(key)
	if !ok {
		return
	}
	s.cache.Delete(namespace, kind, name)
}

func (s *APIServer) startKubeWatchers(ctx context.Context) error {
	slog.Info("kube watch start")

	watchCM := containsString(s.watchResources, "configmap")
	watchSec := containsString(s.watchResources, "secret")

	desiredKeys := make(map[string]struct{})

	for _, ns := range s.watchNamespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(
			s.kube.ClientSet(),
			0,
			informers.WithNamespace(ns),
			informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
				lo.LabelSelector = s.kube.ManagedLabelSelector()
			}),
		)

		var syncChecks []cache.InformerSynced
		var cmInf, secInf cache.SharedIndexInformer

		if watchCM {
			cmInf = factory.Core().V1().ConfigMaps().Informer()
			cmInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					s.handleConfigMapUpsert("add", obj)
				},
				UpdateFunc: func(_, newObj interface{}) {
					s.handleConfigMapUpsert("update", newObj)
				},
				DeleteFunc: func(obj interface{}) {
					s.handleConfigMapDelete("delete", obj)
				},
			})
			syncChecks = append(syncChecks, cmInf.HasSynced)
		}

		if watchSec {
			secInf = factory.Core().V1().Secrets().Informer()
			secInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					s.handleSecretUpsert("add", obj)
				},
				UpdateFunc: func(_, newObj interface{}) {
					s.handleSecretUpsert("update", newObj)
				},
				DeleteFunc: func(obj interface{}) {
					s.handleSecretDelete("delete", obj)
				},
			})
			syncChecks = append(syncChecks, secInf.HasSynced)
		}

		factory.Start(ctx.Done())

		if ok := cache.WaitForCacheSync(ctx.Done(), syncChecks...); !ok {
			return fmt.Errorf("kube watch sync failed: namespace=%s", ns)
		}

		if err := s.reconcileNamespaceFromStore(ns, cmInf, secInf, desiredKeys); err != nil {
			return err
		}
		slog.Info("kube watch ready", "namespace", ns)
	}

	if err := s.deleteStaleKVKeys(desiredKeys); err != nil {
		return err
	}
	return nil
}

func (s *APIServer) reconcileNamespaceFromStore(
	namespace string,
	cmInf cache.SharedIndexInformer,
	secInf cache.SharedIndexInformer,
	desiredKeys map[string]struct{},
) error {
	slog.Info("startup reconcile begin", "namespace", namespace)

	if cmInf != nil {
		for _, obj := range cmInf.GetStore().List() {
			cm, ok := obj.(*corev1.ConfigMap)
			if !ok || cm == nil {
				continue
			}
			key := store.KeyFor(namespace, "configmap", cm.Name)
			value, err := kube.ValueFromConfigMap(cm)
			if err != nil {
				return err
			}
			if value == "" {
				continue
			}
			desiredKeys[key] = struct{}{}
			if err := s.putIfChangedByRV(key, []byte(value), cm.ResourceVersion); err != nil {
				return err
			}
		}
	}

	if secInf != nil {
		for _, obj := range secInf.GetStore().List() {
			sec, ok := obj.(*corev1.Secret)
			if !ok || sec == nil {
				continue
			}
			key := store.KeyFor(namespace, "secret", sec.Name)
			value, err := kube.ValueFromSecret(sec)
			if err != nil {
				return err
			}
			if value == "" {
				continue
			}
			desiredKeys[key] = struct{}{}
			if err := s.putIfChangedByRV(key, []byte(value), sec.ResourceVersion); err != nil {
				return err
			}
		}
	}

	slog.Info("startup reconcile done", "namespace", namespace)
	return nil
}

func (s *APIServer) deleteStaleKVKeys(desiredKeys map[string]struct{}) error {
	keys, err := s.kv.Keys()
	if err == nats.ErrNoKeysFound {
		return nil
	}
	if err != nil {
		return err
	}

	stale := make([]string, 0)
	for _, key := range keys {
		if !s.isWatchedKey(key) {
			continue
		}
		if _, ok := desiredKeys[key]; ok {
			continue
		}
		stale = append(stale, key)
	}

	sort.Strings(stale)
	for _, key := range stale {
		if err := s.deleteIfExists(key); err != nil {
			return err
		}
		slog.Info("startup reconcile delete stale kv", "key", key)
	}
	return nil
}

func (s *APIServer) isWatchedKey(key string) bool {
	namespace, kind, _, ok := store.ParseKey(key)
	if !ok {
		return false
	}
	if !containsString(s.watchNamespaces, namespace) {
		return false
	}
	return containsString(s.watchResources, kind)
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func (s *APIServer) handleConfigMapUpsert(action string, obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm == nil {
		slog.Info("kube configmap unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)

	value, err := kube.ValueFromConfigMap(cm)
	if err != nil {
		s.recordKVOperation("put", "error")
		s.recordKubeEvent("configmap", action, "error")
		slog.Error("kube configmap marshal failed", "action", action, "key", key, "err", err)
		return
	}
	if value == "" {
		slog.Info("kube configmap empty value, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			s.recordKVOperation("delete", "error")
			s.recordKubeEvent("configmap", action, "error")
			slog.Error("kube configmap delete kv failed", "action", action, "key", key, "err", err)
			return
		}
		s.recordKVOperation("delete", "success")
		s.recordKubeEvent("configmap", action, "success")
		return
	}

	if err := s.putIfChangedByRV(key, []byte(value), cm.ResourceVersion); err != nil {
		s.recordKVOperation("put", "error")
		s.recordKubeEvent("configmap", action, "error")
		slog.Error("kube configmap put kv failed", "action", action, "key", key, "err", err)
		return
	}
	s.recordKVOperation("put", "success")
	s.recordKubeEvent("configmap", action, "success")
	slog.Info("kube configmap kv put",
		"action", action,
		"key", key,
		"rv", cm.ResourceVersion,
		"value_len", len(value),
	)
}

func (s *APIServer) handleConfigMapDelete(action string, obj interface{}) {
	cm := extractConfigMap(obj)
	if cm == nil {
		s.recordKubeEvent("configmap", action, "error")
		slog.Info("kube configmap delete unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)
	if err := s.deleteIfExists(key); err != nil {
		s.recordKVOperation("delete", "error")
		s.recordKubeEvent("configmap", action, "error")
		slog.Error("kube configmap delete kv failed", "action", action, "key", key, "err", err)
		return
	}
	s.recordKVOperation("delete", "success")
	s.recordKubeEvent("configmap", action, "success")
	slog.Info("kube configmap delete kv", "action", action, "key", key)
}

func (s *APIServer) handleSecretUpsert(action string, obj interface{}) {
	sec, ok := obj.(*corev1.Secret)
	if !ok || sec == nil {
		slog.Info("kube secret unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)

	value, err := kube.ValueFromSecret(sec)
	if err != nil {
		s.recordKVOperation("put", "error")
		s.recordKubeEvent("secret", action, "error")
		slog.Error("kube secret marshal failed", "action", action, "key", key, "err", err)
		return
	}
	if value == "" {
		slog.Info("kube secret empty value, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			s.recordKVOperation("delete", "error")
			s.recordKubeEvent("secret", action, "error")
			slog.Error("kube secret delete kv failed", "action", action, "key", key, "err", err)
			return
		}
		s.recordKVOperation("delete", "success")
		s.recordKubeEvent("secret", action, "success")
		return
	}

	if err := s.putIfChangedByRV(key, []byte(value), sec.ResourceVersion); err != nil {
		s.recordKVOperation("put", "error")
		s.recordKubeEvent("secret", action, "error")
		slog.Error("kube secret put kv failed", "action", action, "key", key, "err", err)
		return
	}
	s.recordKVOperation("put", "success")
	s.recordKubeEvent("secret", action, "success")
	slog.Info("kube secret kv put",
		"action", action,
		"key", key,
		"rv", sec.ResourceVersion,
		"value_len", len(value),
	)
}

func (s *APIServer) handleSecretDelete(action string, obj interface{}) {
	sec := extractSecret(obj)
	if sec == nil {
		s.recordKubeEvent("secret", action, "error")
		slog.Info("kube secret delete unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)
	if err := s.deleteIfExists(key); err != nil {
		s.recordKVOperation("delete", "error")
		s.recordKubeEvent("secret", action, "error")
		slog.Error("kube secret delete kv failed", "action", action, "key", key, "err", err)
		return
	}
	s.recordKVOperation("delete", "success")
	s.recordKubeEvent("secret", action, "success")
	slog.Info("kube secret delete kv", "action", action, "key", key)
}

func (s *APIServer) recordKubeEvent(resource, action, result string) {
	s.metrics.IncCounter(
		"cfg_distributor_kube_events_total",
		"Total number of Kubernetes events processed by the distributor.",
		map[string]string{
			"resource": resource,
			"action":   action,
			"result":   result,
		},
	)
}

func (s *APIServer) recordKVOperation(operation, result string) {
	s.metrics.IncCounter(
		"cfg_distributor_kv_operations_total",
		"Total number of KV operations attempted by the distributor.",
		map[string]string{
			"operation": operation,
			"result":    result,
		},
	)
}

func extractConfigMap(obj interface{}) *corev1.ConfigMap {
	if cm, ok := obj.(*corev1.ConfigMap); ok {
		return cm
	}
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if cm, ok := tomb.Obj.(*corev1.ConfigMap); ok {
			return cm
		}
	}
	return nil
}

func extractSecret(obj interface{}) *corev1.Secret {
	if sec, ok := obj.(*corev1.Secret); ok {
		return sec
	}
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if sec, ok := tomb.Obj.(*corev1.Secret); ok {
			return sec
		}
	}
	return nil
}

// watchResourcesFromConfig normalizes plural resource names from config (e.g. "configmaps")
// to the singular forms used as KV key components (e.g. "configmap").
func watchResourcesFromConfig(resources []string) []string {
	singularOf := map[string]string{
		"configmaps": "configmap",
		"secrets":    "secret",
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(resources))
	for _, r := range resources {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		singular, ok := singularOf[r]
		if !ok {
			singular = strings.TrimSuffix(r, "s")
		}
		if _, dup := seen[singular]; !dup {
			out = append(out, singular)
			seen[singular] = struct{}{}
		}
	}
	if len(out) == 0 {
		return []string{"configmap", "secret"}
	}
	return out
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
