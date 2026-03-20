package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"
	corev1 "k8s.io/api/core/v1"
	urlruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"ntels.com/upm/cfg-distributor/pkg/api/resources/v1alpha1"
	kvinformers "ntels.com/upm/cfg-distributor/pkg/informers"
	"ntels.com/upm/cfg-distributor/pkg/kube"
	"ntels.com/upm/cfg-distributor/pkg/metrics"
	"ntels.com/upm/cfg-distributor/pkg/store"
)

const (
	bucket     = "UPM_CONFIG"
	metaBucket = "UPM_CONFIG_META"
)

type APIServer struct {
	container *restful.Container
	kv        nats.KeyValue
	metaKV    nats.KeyValue
	nc        *nats.Conn
	kube      *kube.Client
	cache     *store.ResourceCache
	metrics   *metrics.Registry

	natsURL         string
	watchNamespaces []string
	watchResources  []ResourceGroup
	kvInformers     []*kvinformers.KVInformer
}

func New() (*APIServer, error) {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	kv, err := ensureKV(js, bucket)
	if err != nil {
		return nil, err
	}

	kubeClient, err := kube.NewClient()
	if err != nil {
		return nil, err
	}

	slog.Info("init distributor", "nats_url", natsURL, "bucket", bucket, "meta_bucket", metaBucket)

	s := &APIServer{
		container:       restful.NewContainer(),
		kv:              kv,
		metaKV:          nil,
		nc:              nc,
		kube:            kubeClient,
		cache:           store.NewResourceCache(),
		metrics:         metrics.NewRegistry(),
		natsURL:         natsURL,
		watchNamespaces: parseNamespaces(getenv("WATCH_NAMESPACES", "default")),
		watchResources:  defaultWatchResources(),
	}
	s.setDependencyStatus("nats", nil, 1)

	metaKV, err := ensureKV(js, metaBucket)
	if err != nil {
		s.setDependencyStatus("nats", nil, 0)
		return nil, err
	}
	s.metaKV = metaKV

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
	ws.Route(ws.GET("/healthz").To(func(_ *restful.Request, resp *restful.Response) {
		_ = resp.WriteHeaderAndEntity(http.StatusOK, map[string]string{"status": "ok"})
	}))
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
	if err := s.startResourceSync(ctx); err != nil {
		return err
	}

	port := getenv("PORT", "8080")
	addr := ":" + port
	slog.Info("distributor api listening", "addr", addr)

	server := &http.Server{
		Addr:    addr,
		Handler: s.container,
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
		s.setDependencyStatus("nats", nil, 0)
		_ = s.nc.Drain()
		return nil
	case err := <-errCh:
		return err
	}
}

// startResourceSync starts KV watchers for resources declared in watchResources.
func (s *APIServer) startResourceSync(ctx context.Context) error {
	if err := s.startKubeWatchers(ctx); err != nil {
		return err
	}

	for _, ns := range s.watchNamespaces {
		for _, group := range s.watchResources {
			for _, res := range group.Resources {
				var inf *kvinformers.KVInformer
				switch res {
				case "configmaps":
					inf = kvinformers.NewConfigMapInformer(s.kv, ns)
				case "secrets":
					inf = kvinformers.NewSecretInformer(s.kv, ns)
				default:
					continue
				}
				if err := inf.Start(ctx); err != nil {
					s.setDependencyStatus("kv_watcher", map[string]string{
						"namespace": namespaceOrEmpty(ns),
						"resource":  strings.TrimSuffix(res, "s"),
					}, 0)
					return err
				}
				s.setDependencyStatus("kv_watcher", map[string]string{
					"namespace": namespaceOrEmpty(ns),
					"resource":  strings.TrimSuffix(res, "s"),
				}, 1)
				s.kvInformers = append(s.kvInformers, inf)

				// Minimal event loop (log only). Extend as needed.
				go func(informer *kvinformers.KVInformer, namespace, resource string) {
					defer s.setDependencyStatus("kv_watcher", map[string]string{
						"namespace": namespaceOrEmpty(namespace),
						"resource":  strings.TrimSuffix(resource, "s"),
					}, 0)
					for evt := range informer.Events() {
						switch evt.Type {
						case kvinformers.EventDelete, kvinformers.EventPurge:
							s.cache.Delete(namespace, strings.TrimSuffix(resource, "s"), evt.Name)
						default:
							s.cache.Upsert(namespace, strings.TrimSuffix(resource, "s"), evt.Name, store.CachedValue{
								Revision: evt.Revision,
								Value:    string(evt.Value),
							})
						}
						slog.Info("kv watch event",
							"namespace", namespace,
							"resource", resource,
							"type", evt.Type,
							"name", evt.Name,
							"rev", evt.Revision,
							"op", evt.Op,
							"value_len", len(evt.Value),
						)
					}
				}(inf, ns, res)
			}
		}
	}
	return nil
}

func (s *APIServer) putIfChangedByRV(key string, value []byte, resourceVersion string) error {
	if resourceVersion != "" {
		entry, err := s.metaKV.Get(key)
		if err == nil {
			if string(entry.Value()) == resourceVersion {
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
	if resourceVersion != "" {
		if _, err := s.metaKV.Put(key, []byte(resourceVersion)); err != nil {
			return err
		}
	}
	return nil
}

func (s *APIServer) deleteIfExists(key string) error {
	entry, err := s.kv.Get(key)
	if err == nats.ErrKeyNotFound {
		s.deleteCacheForKey(key)
		return nil
	}
	if err != nil {
		return err
	}
	if entry == nil {
		s.deleteCacheForKey(key)
		return nil
	}

	if err := s.kv.Delete(key); err != nil && err != nats.ErrKeyNotFound {
		return err
	}
	s.deleteCacheForKey(key)
	if err := s.metaKV.Delete(key); err != nil && err != nats.ErrKeyNotFound {
		return err
	}
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
	desiredKeys := make(map[string]struct{})
	for _, ns := range s.watchNamespaces {
		s.setDependencyStatus("kube_informer_synced", map[string]string{
			"namespace": namespaceOrEmpty(ns),
			"resource":  "configmap",
		}, 0)
		s.setDependencyStatus("kube_informer_synced", map[string]string{
			"namespace": namespaceOrEmpty(ns),
			"resource":  "secret",
		}, 0)
		factory := informers.NewSharedInformerFactoryWithOptions(
			s.kube.ClientSet(),
			0,
			informers.WithNamespace(ns),
		)

		cmInf := factory.Core().V1().ConfigMaps().Informer()
		secInf := factory.Core().V1().Secrets().Informer()

		cmInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				s.handleConfigMapUpsert("add", obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				s.handleConfigMapUpsert("update", newObj)
			},
			DeleteFunc: func(obj interface{}) {
				s.handleConfigMapDelete(obj)
			},
		})

		secInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				s.handleSecretUpsert("add", obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				s.handleSecretUpsert("update", newObj)
			},
			DeleteFunc: func(obj interface{}) {
				s.handleSecretDelete(obj)
			},
		})

		factory.Start(ctx.Done())
		if ok := cache.WaitForCacheSync(ctx.Done(), cmInf.HasSynced, secInf.HasSynced); !ok {
			s.setDependencyStatus("kube_informer_synced", map[string]string{
				"namespace": namespaceOrEmpty(ns),
				"resource":  "configmap",
			}, 0)
			s.setDependencyStatus("kube_informer_synced", map[string]string{
				"namespace": namespaceOrEmpty(ns),
				"resource":  "secret",
			}, 0)
			return fmt.Errorf("kube watch sync failed: namespace=%s", ns)
		}
		s.setDependencyStatus("kube_informer_synced", map[string]string{
			"namespace": namespaceOrEmpty(ns),
			"resource":  "configmap",
		}, 1)
		s.setDependencyStatus("kube_informer_synced", map[string]string{
			"namespace": namespaceOrEmpty(ns),
			"resource":  "secret",
		}, 1)

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

	for _, obj := range cmInf.GetStore().List() {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || cm == nil {
			continue
		}
		key := store.KeyFor(namespace, "configmap", cm.Name)
		if !kube.IsManagedConfigMap(cm) {
			continue
		}
		value := kube.ValueFromConfigMap(cm)
		if value == "" {
			continue
		}
		desiredKeys[key] = struct{}{}
		if err := s.putIfChangedByRV(key, []byte(value), cm.ResourceVersion); err != nil {
			return err
		}
	}

	for _, obj := range secInf.GetStore().List() {
		sec, ok := obj.(*corev1.Secret)
		if !ok || sec == nil {
			continue
		}
		key := store.KeyFor(namespace, "secret", sec.Name)
		if !kube.IsManagedSecret(sec) {
			continue
		}
		value := kube.ValueFromSecret(sec)
		if value == "" {
			continue
		}
		desiredKeys[key] = struct{}{}
		if err := s.putIfChangedByRV(key, []byte(value), sec.ResourceVersion); err != nil {
			return err
		}
	}

	slog.Info("startup reconcile done", "namespace", namespace)
	return nil
}

func (s *APIServer) deleteStaleKVKeys(desiredKeys map[string]struct{}) error {
	keys, err := s.kv.Keys()
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
	for _, group := range s.watchResources {
		for _, resource := range group.Resources {
			if strings.TrimSuffix(resource, "s") == kind {
				return true
			}
		}
	}
	return false
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
		s.recordKubeEvent("configmap", action, "error")
		slog.Info("kube configmap unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)

	if !kube.IsManagedConfigMap(cm) {
		slog.Info("kube configmap not managed, delete kv if exists", "action", action, "key", key)
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

	value := kube.ValueFromConfigMap(cm)
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

func (s *APIServer) handleConfigMapDelete(obj interface{}) {
	cm := extractConfigMap(obj)
	if cm == nil {
		s.recordKubeEvent("configmap", "delete", "error")
		slog.Info("kube configmap delete unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)
	if err := s.deleteIfExists(key); err != nil {
		s.recordKVOperation("delete", "error")
		s.recordKubeEvent("configmap", "delete", "error")
		slog.Error("kube configmap delete kv failed", "key", key, "err", err)
		return
	}
	s.recordKVOperation("delete", "success")
	s.recordKubeEvent("configmap", "delete", "success")
	slog.Info("kube configmap delete kv", "key", key)
}

func (s *APIServer) handleSecretUpsert(action string, obj interface{}) {
	sec, ok := obj.(*corev1.Secret)
	if !ok || sec == nil {
		s.recordKubeEvent("secret", action, "error")
		slog.Info("kube secret unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)

	if !kube.IsManagedSecret(sec) {
		slog.Info("kube secret not managed, delete kv if exists", "action", action, "key", key)
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

	value := kube.ValueFromSecret(sec)
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

func (s *APIServer) handleSecretDelete(obj interface{}) {
	sec := extractSecret(obj)
	if sec == nil {
		s.recordKubeEvent("secret", "delete", "error")
		slog.Info("kube secret delete unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)
	if err := s.deleteIfExists(key); err != nil {
		s.recordKVOperation("delete", "error")
		s.recordKubeEvent("secret", "delete", "error")
		slog.Error("kube secret delete kv failed", "key", key, "err", err)
		return
	}
	s.recordKVOperation("delete", "success")
	s.recordKubeEvent("secret", "delete", "success")
	slog.Info("kube secret delete kv", "key", key)
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

func (s *APIServer) setDependencyStatus(dependency string, labels map[string]string, value float64) {
	allLabels := map[string]string{
		"dependency": dependency,
	}
	for key, labelValue := range labels {
		allLabels[key] = labelValue
	}
	s.metrics.SetGauge(
		"cfg_distributor_dependency_status",
		"Dependency health status where 1 is healthy and 0 is unhealthy.",
		allLabels,
		value,
	)
}

func namespaceOrEmpty(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
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

type ResourceGroup struct {
	Group     string
	Version   string
	Resources []string
}

func defaultWatchResources() []ResourceGroup {
	return []ResourceGroup{
		{
			Group:   "",
			Version: "v1",
			Resources: []string{
				"configmaps",
				"secrets",
			},
		},
	}
}

func parseNamespaces(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
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

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
