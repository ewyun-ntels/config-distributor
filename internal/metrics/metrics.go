package metrics

import (
	"net/http"
	"slices"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	mu       sync.Mutex
	registry *prometheus.Registry
	counters map[string]*counterMetric
}

type counterMetric struct {
	labels []string
	vec    *prometheus.CounterVec
}

func NewRegistry() *Registry {
	registry := prometheus.NewRegistry()
	return &Registry{
		registry: registry,
		counters: make(map[string]*counterMetric),
	}
}

func (r *Registry) IncCounter(name, help string, labels map[string]string) {
	r.AddCounter(name, help, labels, 1)
}

func (r *Registry) AddCounter(name, help string, labels map[string]string, delta uint64) {
	labelNames, labelValues := orderedLabels(labels)
	counter := r.counter(name, help, labelNames)
	counter.WithLabelValues(labelValues...).Add(float64(delta))
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

func (r *Registry) counter(name, help string, labelNames []string) *prometheus.CounterVec {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.counters[name]; ok {
		if !slices.Equal(metric.labels, labelNames) {
			panic("metric label mismatch: " + name)
		}
		return metric.vec
	}

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: help,
		},
		labelNames,
	)
	r.registry.MustRegister(counter)
	r.counters[name] = &counterMetric{
		labels: labelNames,
		vec:    counter,
	}
	return counter
}

func orderedLabels(labels map[string]string) ([]string, []string) {
	if len(labels) == 0 {
		return nil, nil
	}

	labelNames := make([]string, 0, len(labels))
	for name := range labels {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	labelValues := make([]string, 0, len(labelNames))
	for _, name := range labelNames {
		labelValues = append(labelValues, labels[name])
	}
	return labelNames, labelValues
}
