package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
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
		natsURL:         natsURL,
		watchNamespaces: parseNamespaces(getenv("WATCH_NAMESPACES", "default")),
		watchResources:  defaultWatchResources(),
	}

	metaKV, err := ensureKV(js, metaBucket)
	if err != nil {
		return nil, err
	}
	s.metaKV = metaKV

	return s, nil
}

func (s *APIServer) PrepareRun() error {
	s.container.Router(restful.CurlyRouter{})
	s.installAPIs()
	return nil
}

func (s *APIServer) installAPIs() {
	urlruntime.Must(v1alpha1.AddToContainer(s.container, s.kv, s.kube))
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
		_ = s.nc.Drain()
		return nil
	case err := <-errCh:
		return err
	}
}

// startResourceSync starts KV watchers for resources declared in watchResources.
func (s *APIServer) startResourceSync(ctx context.Context) error {
	slog.Info("sync start: listing from kube")
	if err := s.syncFromKube(ctx); err != nil {
		return err
	}
	slog.Info("sync done: listing from kube")

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
					return err
				}
				s.kvInformers = append(s.kvInformers, inf)

				// Minimal event loop (log only). Extend as needed.
				go func(informer *kvinformers.KVInformer, namespace, resource string) {
					for evt := range informer.Events() {
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

func (s *APIServer) syncFromKube(ctx context.Context) error {
	for _, ns := range s.watchNamespaces {
		for _, group := range s.watchResources {
			for _, res := range group.Resources {
				switch res {
				case "configmaps":
					slog.Info("kube list configmaps", "namespace", ns)
					cms, err := s.kube.ListConfigMaps(ctx, ns)
					if err != nil {
						return err
					}
					slog.Info("kube list configmaps done", "namespace", ns, "count", len(cms))
					for _, cm := range cms {
						if !kube.IsManagedConfigMap(&cm) {
							slog.Info("kube configmap skip (not managed)", "namespace", cm.Namespace, "name", cm.Name)
							continue
						}
						value := kube.ValueFromConfigMap(&cm)
						if value == "" {
							slog.Info("kube configmap skip (empty value)", "namespace", cm.Namespace, "name", cm.Name)
							continue
						}
						if err := s.putIfChangedByRV(store.KeyFor(ns, "configmap", cm.Name), []byte(value), cm.ResourceVersion); err != nil {
							return err
						}
						slog.Info("kube configmap synced",
							"namespace", cm.Namespace,
							"name", cm.Name,
							"rv", cm.ResourceVersion,
							"value_len", len(value),
						)
					}
				case "secrets":
					slog.Info("kube list secrets", "namespace", ns)
					secs, err := s.kube.ListSecrets(ctx, ns)
					if err != nil {
						return err
					}
					slog.Info("kube list secrets done", "namespace", ns, "count", len(secs))
					for _, sec := range secs {
						if !kube.IsManagedSecret(&sec) {
							slog.Info("kube secret skip (not managed)", "namespace", sec.Namespace, "name", sec.Name)
							continue
						}
						value := kube.ValueFromSecret(&sec)
						if value == "" {
							slog.Info("kube secret skip (empty value)", "namespace", sec.Namespace, "name", sec.Name)
							continue
						}
						if err := s.putIfChangedByRV(store.KeyFor(ns, "secret", sec.Name), []byte(value), sec.ResourceVersion); err != nil {
							return err
						}
						slog.Info("kube secret synced",
							"namespace", sec.Namespace,
							"name", sec.Name,
							"rv", sec.ResourceVersion,
							"value_len", len(value),
						)
					}
				default:
					continue
				}
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

	if _, err := s.kv.Put(key, value); err != nil {
		return err
	}
	if resourceVersion != "" {
		if _, err := s.metaKV.Put(key, []byte(resourceVersion)); err != nil {
			return err
		}
	}
	return nil
}

func (s *APIServer) deleteIfExists(key string) error {
	if err := s.kv.Delete(key); err != nil && err != nats.ErrKeyNotFound {
		return err
	}
	if err := s.metaKV.Delete(key); err != nil && err != nats.ErrKeyNotFound {
		return err
	}
	return nil
}

func (s *APIServer) startKubeWatchers(ctx context.Context) error {
	slog.Info("kube watch start")
	for _, ns := range s.watchNamespaces {
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
			return fmt.Errorf("kube watch sync failed: namespace=%s", ns)
		}
		slog.Info("kube watch ready", "namespace", ns)
	}
	return nil
}

func (s *APIServer) handleConfigMapUpsert(action string, obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm == nil {
		slog.Info("kube configmap unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)

	if !kube.IsManagedConfigMap(cm) {
		slog.Info("kube configmap not managed, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			slog.Error("kube configmap delete kv failed", "action", action, "key", key, "err", err)
		}
		return
	}

	value := kube.ValueFromConfigMap(cm)
	if value == "" {
		slog.Info("kube configmap empty value, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			slog.Error("kube configmap delete kv failed", "action", action, "key", key, "err", err)
		}
		return
	}

	if err := s.putIfChangedByRV(key, []byte(value), cm.ResourceVersion); err != nil {
		slog.Error("kube configmap put kv failed", "action", action, "key", key, "err", err)
		return
	}
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
		slog.Info("kube configmap delete unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(cm.Namespace, "configmap", cm.Name)
	if err := s.deleteIfExists(key); err != nil {
		slog.Error("kube configmap delete kv failed", "key", key, "err", err)
		return
	}
	slog.Info("kube configmap delete kv", "key", key)
}

func (s *APIServer) handleSecretUpsert(action string, obj interface{}) {
	sec, ok := obj.(*corev1.Secret)
	if !ok || sec == nil {
		slog.Info("kube secret unexpected object", "action", action, "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)

	if !kube.IsManagedSecret(sec) {
		slog.Info("kube secret not managed, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			slog.Error("kube secret delete kv failed", "action", action, "key", key, "err", err)
		}
		return
	}

	value := kube.ValueFromSecret(sec)
	if value == "" {
		slog.Info("kube secret empty value, delete kv if exists", "action", action, "key", key)
		if err := s.deleteIfExists(key); err != nil {
			slog.Error("kube secret delete kv failed", "action", action, "key", key, "err", err)
		}
		return
	}

	if err := s.putIfChangedByRV(key, []byte(value), sec.ResourceVersion); err != nil {
		slog.Error("kube secret put kv failed", "action", action, "key", key, "err", err)
		return
	}
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
		slog.Info("kube secret delete unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}
	key := store.KeyFor(sec.Namespace, "secret", sec.Name)
	if err := s.deleteIfExists(key); err != nil {
		slog.Error("kube secret delete kv failed", "key", key, "err", err)
		return
	}
	slog.Info("kube secret delete kv", "key", key)
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
