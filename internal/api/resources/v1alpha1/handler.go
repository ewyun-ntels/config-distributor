package v1alpha1

import (
	"fmt"
	"io"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"

	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/store"
)

const (
	resourceConfigMap = "configmap"

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
		writeError(resp, http.StatusInternalServerError, err)
		return
	}
	value := string(data)
	h.cache.Upsert(ns, kind, name, store.CachedValue{
		Revision: rev,
		Value:    value,
	})
	h.recordKVOperation(ns, kind, name, actionPut, resultSuccess, reasonNone)

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
			writeError(resp, http.StatusInternalServerError, err)
			return
		}
	}
	h.cache.Delete(ns, kind, name)
	h.recordKVOperation(ns, kind, name, actionDelete, resultSuccess, reasonNone)

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
