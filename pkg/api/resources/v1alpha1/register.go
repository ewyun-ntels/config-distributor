package v1alpha1

import (
	restful "github.com/emicklei/go-restful/v3"
	"github.com/nats-io/nats.go"

	"ntels.com/upm/cfg-distributor/pkg/kube"
	"ntels.com/upm/cfg-distributor/pkg/store"
)

// Register wires API routes for v1alpha1 resources.
func Register(ws *restful.WebService, h *Handler) {
	ws.Path("/namespaces/{namespace}").
		Consumes(restful.MIME_JSON, "text/plain").
		Produces(restful.MIME_JSON, "text/plain")

	ws.Route(ws.GET("/configmap").To(h.listConfigMaps))
	ws.Route(ws.GET("/configmap/{name}").To(h.getConfigMap))
	ws.Route(ws.POST("/configmap/{name}").To(h.putConfigMap))
	ws.Route(ws.PUT("/configmap/{name}").To(h.putConfigMap))
	ws.Route(ws.DELETE("/configmap/{name}").To(h.deleteConfigMap))

	ws.Route(ws.GET("/secret").To(h.listSecrets))
	ws.Route(ws.GET("/secret/{name}").To(h.getSecret))
	ws.Route(ws.POST("/secret/{name}").To(h.putSecret))
	ws.Route(ws.PUT("/secret/{name}").To(h.putSecret))
	ws.Route(ws.DELETE("/secret/{name}").To(h.deleteSecret))
}

// AddToContainer registers v1alpha1 resources into the container.
func AddToContainer(container *restful.Container, kv nats.KeyValue, kubeClient *kube.Client, cache *store.ResourceCache) error {
	ws := new(restful.WebService)
	Register(ws, NewHandlerWithDeps(kv, kubeClient, cache))
	container.Add(ws)
	return nil
}
