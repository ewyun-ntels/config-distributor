package v1alpha1

import (
	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"

	"ntels.com/upm/cfg-distributor/internal/metrics"
	"ntels.com/upm/cfg-distributor/internal/store"
)

// Register wires API routes for v1alpha1 resources.
func Register(ws *restful.WebService, h *Handler) {
	ws.Path("/namespaces/{namespace}").
		Consumes(restful.MIME_JSON, "text/plain").
		Produces(restful.MIME_JSON, "text/plain")

	ws.Route(ws.GET("/configmap").To(h.listConfigMaps))
	ws.Route(ws.GET("/configmaps").To(h.listAllConfigMaps))
	ws.Route(ws.GET("/configmap/{name}").To(h.getConfigMap))
	ws.Route(ws.POST("/configmap/{name}").To(h.putConfigMap))
	ws.Route(ws.PUT("/configmap/{name}").To(h.putConfigMap))
	ws.Route(ws.DELETE("/configmap/{name}").To(h.deleteConfigMap))
}

// AddToContainer registers v1alpha1 resources into the container.
func AddToContainer(container *restful.Container, kv nats.KeyValue, cache *store.ResourceCache, registry *metrics.Registry) error {
	ws := new(restful.WebService)
	Register(ws, NewHandlerWithDeps(kv, cache, registry))
	container.Add(ws)
	return nil
}
