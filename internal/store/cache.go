package store

import (
	"slices"
	"strings"
	"sync"
)

type CachedValue struct {
	Revision uint64
	Value    string
}

type NamedValue struct {
	Name string
	CachedValue
}

type ResourceValue struct {
	Namespace string
	Name      string
	CachedValue
}

type ResourceCache struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string]CachedValue
}

func NewResourceCache() *ResourceCache {
	return &ResourceCache{
		data: make(map[string]map[string]map[string]CachedValue),
	}
}

func (c *ResourceCache) Upsert(namespace, kind, name string, value CachedValue) {
	c.mu.Lock()
	defer c.mu.Unlock()

	kindMap, ok := c.data[namespace]
	if !ok {
		kindMap = make(map[string]map[string]CachedValue)
		c.data[namespace] = kindMap
	}

	nameMap, ok := kindMap[kind]
	if !ok {
		nameMap = make(map[string]CachedValue)
		kindMap[kind] = nameMap
	}

	nameMap[name] = value
}

func (c *ResourceCache) Delete(namespace, kind, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	kindMap, ok := c.data[namespace]
	if !ok {
		return
	}
	nameMap, ok := kindMap[kind]
	if !ok {
		return
	}

	delete(nameMap, name)
	if len(nameMap) == 0 {
		delete(kindMap, kind)
	}
	if len(kindMap) == 0 {
		delete(c.data, namespace)
	}
}

func (c *ResourceCache) Get(namespace, kind, name string) (CachedValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	kindMap, ok := c.data[namespace]
	if !ok {
		return CachedValue{}, false
	}
	nameMap, ok := kindMap[kind]
	if !ok {
		return CachedValue{}, false
	}
	value, ok := nameMap[name]
	return value, ok
}

func (c *ResourceCache) List(namespace, kind string) []NamedValue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	kindMap, ok := c.data[namespace]
	if !ok {
		return nil
	}
	nameMap, ok := kindMap[kind]
	if !ok {
		return nil
	}

	items := make([]NamedValue, 0, len(nameMap))
	for name, value := range nameMap {
		items = append(items, NamedValue{
			Name:        name,
			CachedValue: value,
		})
	}
	slices.SortFunc(items, func(a, b NamedValue) int {
		return strings.Compare(a.Name, b.Name)
	})
	return items
}

func (c *ResourceCache) ListAll(kind string) []ResourceValue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	items := make([]ResourceValue, 0)
	for namespace, kindMap := range c.data {
		nameMap, ok := kindMap[kind]
		if !ok {
			continue
		}
		for name, value := range nameMap {
			items = append(items, ResourceValue{
				Namespace:   namespace,
				Name:        name,
				CachedValue: value,
			})
		}
	}
	slices.SortFunc(items, func(a, b ResourceValue) int {
		if cmp := strings.Compare(a.Namespace, b.Namespace); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Name, b.Name)
	})
	return items
}
