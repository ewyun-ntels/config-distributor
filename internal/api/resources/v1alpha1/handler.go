package v1alpha1

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"

	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/store"
)

const (
	resourceConfigMap = "configmap"
	namespaceAll      = "all"

	actionPut    = "put"
	actionDelete = "delete"

	resultSuccess = "success"
	resultError   = "error"

	reasonNone          = "none"
	reasonStorageFailed = "storage_failed"
)

type Handler struct {
	kv      nats.KeyValue
	cache   *store.ResourceCache
	metrics *metrics.Registry
}

func NewHandlerWithDeps(kv nats.KeyValue, cache *store.ResourceCache, registry *metrics.Registry) *Handler {
	return &Handler{kv: kv, cache: cache, metrics: registry}
}

func (h *Handler) listConfigMaps(req *restful.Request, resp *restful.Response) {
	h.listByPrefix(req, resp, resourceConfigMap)
}

func (h *Handler) listAllConfigMaps(req *restful.Request, resp *restful.Response) {
	ns := req.PathParameter("namespace")
	if ns != namespaceAll {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("namespace must be %q for aggregate list", namespaceAll))
		return
	}
	h.listAllByKind(resp, resourceConfigMap)
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
	slog.Debug("list resources from cache", "namespace", ns, "kind", kind, "items", len(items))

	resp.WriteHeaderAndEntity(http.StatusOK, listResponse{
		Namespace: ns,
		Kind:      kind,
		Items:     items,
	})
}

func (h *Handler) listAllByKind(resp *restful.Response, kind string) {
	items := make([]itemResponse, 0)
	for _, item := range h.cache.ListAll(kind) {
		items = append(items, itemResponse{
			Namespace: item.Namespace,
			Kind:      kind,
			Name:      item.Name,
			Revision:  item.Revision,
			Value:     item.Value,
		})
	}
	slog.Debug("list all resources from cache", "kind", kind, "items", len(items))

	resp.WriteHeaderAndEntity(http.StatusOK, listResponse{
		Kind:  kind,
		Items: items,
	})
}

func (h *Handler) getConfigMap(req *restful.Request, resp *restful.Response) {
	h.getItem(req, resp, resourceConfigMap)
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
		slog.Debug("get resource from cache", "namespace", ns, "kind", kind, "name", name, "revision", item.Revision)
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
			slog.Debug("resource not found in kv", "namespace", ns, "kind", kind, "name", name)
			writeError(resp, http.StatusNotFound, err)
			return
		}
		slog.Debug("get resource from kv failed", "namespace", ns, "kind", kind, "name", name, "err", err)
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	slog.Debug("get resource from kv", "namespace", ns, "kind", kind, "name", name, "revision", entry.Revision())
	resp.WriteHeaderAndEntity(http.StatusOK, itemResponse{
		Namespace: ns,
		Kind:      kind,
		Name:      name,
		Revision:  entry.Revision(),
		Value:     string(entry.Value()),
	})
}

func (h *Handler) putConfigMap(req *restful.Request, resp *restful.Response) {
	h.putItem(req, resp, resourceConfigMap)
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

	rev, err := h.kv.Put(store.KeyFor(ns, kind, name), data)
	if err != nil {
		h.recordKVOperation(ns, kind, name, actionPut, resultError, reasonStorageFailed)
		slog.Debug("put resource failed", "namespace", ns, "kind", kind, "name", name, "bytes", len(data), "err", err)
		writeError(resp, http.StatusInternalServerError, err)
		return
	}
	value := string(data)
	h.cache.Upsert(ns, kind, name, store.CachedValue{
		Revision: rev,
		Value:    value,
	})
	h.recordKVOperation(ns, kind, name, actionPut, resultSuccess, reasonNone)
	slog.Debug("put resource", "namespace", ns, "kind", kind, "name", name, "revision", rev, "bytes", len(data))

	resp.WriteHeaderAndEntity(http.StatusOK, map[string]any{
		"namespace": ns,
		"kind":      kind,
		"name":      name,
		"revision":  rev,
	})
}

func (h *Handler) deleteConfigMap(req *restful.Request, resp *restful.Response) {
	h.deleteItem(req, resp, resourceConfigMap)
}

func (h *Handler) deleteItem(req *restful.Request, resp *restful.Response, kind string) {
	ns := req.PathParameter("namespace")
	name := req.PathParameter("name")
	if name == "" {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	if err := h.kv.Delete(store.KeyFor(ns, kind, name)); err != nil {
		if err != nats.ErrKeyNotFound {
			h.recordKVOperation(ns, kind, name, actionDelete, resultError, reasonStorageFailed)
			slog.Debug("delete resource failed", "namespace", ns, "kind", kind, "name", name, "err", err)
			writeError(resp, http.StatusInternalServerError, err)
			return
		}
		slog.Debug("delete resource ignored missing key", "namespace", ns, "kind", kind, "name", name)
	}
	h.cache.Delete(ns, kind, name)
	h.recordKVOperation(ns, kind, name, actionDelete, resultSuccess, reasonNone)
	slog.Debug("delete resource", "namespace", ns, "kind", kind, "name", name)

	resp.WriteHeader(http.StatusNoContent)
}

func (h *Handler) recordKVOperation(namespace, resource, name, action, result, reason string) {
	if h.metrics == nil {
		return
	}
	h.metrics.IncCounter(
		"cfg_distributor_kv_operations_total",
		"Total number of KV operations attempted by the distributor.",
		map[string]string{
			"namespace": namespace,
			"resource":  resource,
			"name":      name,
			"action":    action,
			"result":    result,
			"reason":    reason,
		},
	)
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
