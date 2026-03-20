package store

import (
	"fmt"
	"strings"
)

func KeyPrefix(namespace, kind string) string {
	return fmt.Sprintf("namespaces/%s/%s/", namespace, kind)
}

func KeyFor(namespace, kind, name string) string {
	return KeyPrefix(namespace, kind) + name
}

func ParseKey(key string) (namespace, kind, name string, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "namespaces" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[3], true
}
