package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Registry struct {
	mu       sync.RWMutex
	counters map[string]*counterMetric
	gauges   map[string]*gaugeMetric
}

type counterMetric struct {
	help   string
	values map[string]uint64
}

type gaugeMetric struct {
	help   string
	values map[string]float64
}

func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*counterMetric),
		gauges:   make(map[string]*gaugeMetric),
	}
}

func (r *Registry) IncCounter(name, help string, labels map[string]string) {
	r.AddCounter(name, help, labels, 1)
}

func (r *Registry) AddCounter(name, help string, labels map[string]string, delta uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric, ok := r.counters[name]
	if !ok {
		metric = &counterMetric{
			help:   help,
			values: make(map[string]uint64),
		}
		r.counters[name] = metric
	}
	metric.values[encodeLabels(labels)] += delta
}

func (r *Registry) SetGauge(name, help string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric, ok := r.gauges[name]
	if !ok {
		metric = &gaugeMetric{
			help:   help,
			values: make(map[string]float64),
		}
		r.gauges[name] = metric
	}
	metric.values[encodeLabels(labels)] = value
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(r.Render()))
	})
}

func (r *Registry) Render() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var b strings.Builder

	counterNames := make([]string, 0, len(r.counters))
	for name := range r.counters {
		counterNames = append(counterNames, name)
	}
	sort.Strings(counterNames)

	for _, name := range counterNames {
		metric := r.counters[name]
		writeMeta(&b, name, metric.help, "counter")
		writeSamples(&b, name, metric.values)
	}

	gaugeNames := make([]string, 0, len(r.gauges))
	for name := range r.gauges {
		gaugeNames = append(gaugeNames, name)
	}
	sort.Strings(gaugeNames)

	for _, name := range gaugeNames {
		metric := r.gauges[name]
		writeMeta(&b, name, metric.help, "gauge")
		writeSamples(&b, name, metric.values)
	}

	return b.String()
}

func writeMeta(b *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, escapeHelp(help))
	fmt.Fprintf(b, "# TYPE %s %s\n", name, metricType)
}

func writeSamples[T ~uint64 | ~float64](b *strings.Builder, name string, values map[string]T) {
	labelKeys := make([]string, 0, len(values))
	for labelKey := range values {
		labelKeys = append(labelKeys, labelKey)
	}
	sort.Strings(labelKeys)

	for _, labelKey := range labelKeys {
		switch value := any(values[labelKey]).(type) {
		case uint64:
			fmt.Fprintf(b, "%s%s %d\n", name, labelKey, value)
		case float64:
			fmt.Fprintf(b, "%s%s %s\n", name, labelKey, strconv.FormatFloat(value, 'f', -1, 64))
		}
	}
}

func encodeLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, key, escapeLabelValue(labels[key])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeHelp(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return v
}
