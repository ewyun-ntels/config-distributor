package store

import "fmt"

func KeyPrefix(namespace, kind string) string {
	return fmt.Sprintf("namespaces/%s/%s/", namespace, kind)
}

func KeyFor(namespace, kind, name string) string {
	return KeyPrefix(namespace, kind) + name
}
