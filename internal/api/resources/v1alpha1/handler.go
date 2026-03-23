package v1alpha1

import (
	"context"
	"fmt"
	"io"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"ntels.com/upm/cfg-distributor/internal/kube"
	"ntels.com/upm/cfg-distributor/internal/store"
)

type Handler struct {
	kv    nats.KeyValue
	kube  *kube.Client
	cache *store.ResourceCache
}

func NewHandlerWithDeps(kv nats.KeyValue, kubeClient *kube.Client, cache *store.ResourceCache) *Handler {
	return &Handler{kv: kv, kube: kubeClient, cache: cache}
}

func (h *Handler) listConfigMaps(req *restful.Request, resp *restful.Response) {
	h.listByPrefix(req, resp, "configmap")
}

func (h *Handler) listSecrets(req *restful.Request, resp *restful.Response) {
	h.listByPrefix(req, resp, "secret")
}

func (h *Handler) listByPrefix(req *restful.Request, resp *restful.Response, kind string) {
	ns := req.PathParameter("namespace")

	items := make([]itemResponse, 0)
	for _, item := range h.cache.List(ns, kind) {
		items = append(items, itemResponse{
			Namespace: ns,
			Kind:      kind,
			Name:      item.Name,
			Revision:  item.Revision,
			Value:     item.Value,
		})
	}

	resp.WriteHeaderAndEntity(http.StatusOK, listResponse{
		Namespace: ns,
		Kind:      kind,
		Items:     items,
	})
}

func (h *Handler) getConfigMap(req *restful.Request, resp *restful.Response) {
	h.getItem(req, resp, "configmap")
}

func (h *Handler) getSecret(req *restful.Request, resp *restful.Response) {
	h.getItem(req, resp, "secret")
}

func (h *Handler) getItem(req *restful.Request, resp *restful.Response, kind string) {
	ns := req.PathParameter("namespace")
	name := req.PathParameter("name")
	if name == "" {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	item, ok := h.cache.Get(ns, kind, name)
	if ok {
		resp.WriteHeaderAndEntity(http.StatusOK, itemResponse{
			Namespace: ns,
			Kind:      kind,
			Name:      name,
			Revision:  item.Revision,
			Value:     item.Value,
		})
		return
	}

	entry, err := h.kv.Get(store.KeyFor(ns, kind, name))
	if err != nil {
		if err == nats.ErrKeyNotFound {
			writeError(resp, http.StatusNotFound, err)
			return
		}
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteHeaderAndEntity(http.StatusOK, itemResponse{
		Namespace: ns,
		Kind:      kind,
		Name:      name,
		Revision:  entry.Revision(),
		Value:     string(entry.Value()),
	})
}

func (h *Handler) putConfigMap(req *restful.Request, resp *restful.Response) {
	h.putItem(req, resp, "configmap")
}

func (h *Handler) putSecret(req *restful.Request, resp *restful.Response) {
	h.putItem(req, resp, "secret")
}

func (h *Handler) putItem(req *restful.Request, resp *restful.Response, kind string) {
	ns := req.PathParameter("namespace")
	name := req.PathParameter("name")
	if name == "" {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	body := req.Request.Body
	if body == nil {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("body is required"))
		return
	}

	data, err := ioReadAllLimit(body, 4<<20)
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	storedValue, k8sRev, err := h.applyToKube(req.Request.Context(), ns, kind, name, data)
	if err != nil {
		if apierrors.IsNotFound(err) {
			writeError(resp, http.StatusNotFound, err)
			return
		}
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	// Update KV/cache in the request path so the API can return the KV revision
	// immediately and subsequent reads in this process observe the new value
	// without waiting for the asynchronous informer event.
	rev, err := h.kv.Put(store.KeyFor(ns, kind, name), []byte(storedValue))
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}
	h.cache.Upsert(ns, kind, name, store.CachedValue{
		Revision: rev,
		Value:    storedValue,
	})

	resp.WriteHeaderAndEntity(http.StatusOK, map[string]any{
		"namespace":        ns,
		"kind":             kind,
		"name":             name,
		"revision":         rev,
		"k8sResourceVer":   k8sRev,
		"k8sResourceKind":  kind,
		"k8sResourceName":  name,
		"k8sResourceScope": "namespaced",
	})
}

func (h *Handler) deleteConfigMap(req *restful.Request, resp *restful.Response) {
	h.deleteItem(req, resp, "configmap")
}

func (h *Handler) deleteSecret(req *restful.Request, resp *restful.Response) {
	h.deleteItem(req, resp, "secret")
}

func (h *Handler) deleteItem(req *restful.Request, resp *restful.Response, kind string) {
	ns := req.PathParameter("namespace")
	name := req.PathParameter("name")
	if name == "" {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	if err := h.deleteFromKube(req.Request.Context(), ns, kind, name); err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	if err := h.kv.Delete(store.KeyFor(ns, kind, name)); err != nil {
		if err != nats.ErrKeyNotFound {
			writeError(resp, http.StatusInternalServerError, err)
			return
		}
	}
	h.cache.Delete(ns, kind, name)

	resp.WriteHeader(http.StatusNoContent)
}

func (h *Handler) applyToKube(ctx context.Context, namespace, kind, name string, data []byte) (string, string, error) {
	if h.kube == nil {
		return "", "", fmt.Errorf("kube client not configured")
	}
	value := string(data)
	switch kind {
	case "configmap":
		cm, err := h.kube.UpsertConfigMap(ctx, namespace, name, value)
		if err != nil {
			return "", "", err
		}
		return kube.ValueFromConfigMap(cm), cm.ResourceVersion, nil
	case "secret":
		sec, err := h.kube.UpsertSecret(ctx, namespace, name, value)
		if err != nil {
			return "", "", err
		}
		return kube.ValueFromSecret(sec), sec.ResourceVersion, nil
	default:
		return "", "", fmt.Errorf("unsupported kind: %s", kind)
	}
}

func (h *Handler) deleteFromKube(ctx context.Context, namespace, kind, name string) error {
	if h.kube == nil {
		return fmt.Errorf("kube client not configured")
	}
	switch kind {
	case "configmap":
		return h.kube.DeleteConfigMap(ctx, namespace, name)
	case "secret":
		return h.kube.DeleteSecret(ctx, namespace, name)
	default:
		return fmt.Errorf("unsupported kind: %s", kind)
	}
}

func writeError(resp *restful.Response, code int, err error) {
	_ = resp.WriteHeaderAndEntity(code, map[string]string{
		"error": err.Error(),
	})
}

func ioReadAllLimit(r io.ReadCloser, limit int64) ([]byte, error) {
	defer r.Close()
	limited := &io.LimitedReader{R: r, N: limit + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("payload too large")
	}
	return data, nil
}
