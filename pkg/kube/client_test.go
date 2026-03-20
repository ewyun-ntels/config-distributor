package kube

import (
	"bytes"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func resetManagedLabelCache() {
	managedLabelKey = ""
	managedLabelValue = ""
	managedLabelOnce = sync.Once{}
}

func TestEnsureManagedLabels(t *testing.T) {
	t.Setenv("MANAGED_LABEL_KEY", "config.upm.io/managed")
	t.Setenv("MANAGED_LABEL_VALUE", "true")
	resetManagedLabelCache()

	meta := &metav1.ObjectMeta{}
	ensureManagedLabels(meta)

	if got := meta.Labels["config.upm.io/managed"]; got != "true" {
		t.Fatalf("expected managed label to be set, got %q", got)
	}
}

func TestConfigMapValueRoundTripPreservesKeys(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"app.yaml":  "port: 8080\n",
			"log.level": "info",
		},
	}

	value := ValueFromConfigMap(cm)
	got, err := parseConfigMapValue(value)
	if err != nil {
		t.Fatalf("parse configmap value: %v", err)
	}

	if got["app.yaml"] != "port: 8080\n" || got["log.level"] != "info" || len(got) != 2 {
		t.Fatalf("unexpected parsed configmap data: %#v", got)
	}
}

func TestConfigMapPlainValueRemainsCompatible(t *testing.T) {
	got, err := parseConfigMapValue("plain-text")
	if err != nil {
		t.Fatalf("parse plain configmap value: %v", err)
	}

	if got["value"] != "plain-text" || len(got) != 1 {
		t.Fatalf("unexpected parsed configmap data: %#v", got)
	}
}

func TestSecretValueRoundTripPreservesTextAndBinary(t *testing.T) {
	sec := &corev1.Secret{
		Data: map[string][]byte{
			"username": []byte("admin"),
			"cert":     []byte{0x00, 0x01, 0x02, 0xff},
		},
	}

	value := ValueFromSecret(sec)
	got, err := parseSecretValue(value)
	if err != nil {
		t.Fatalf("parse secret value: %v", err)
	}

	if !bytes.Equal(got.Data["username"], []byte("admin")) {
		t.Fatalf("unexpected username data: %q", got.Data["username"])
	}
	if !bytes.Equal(got.Data["cert"], []byte{0x00, 0x01, 0x02, 0xff}) {
		t.Fatalf("unexpected cert data: %v", got.Data["cert"])
	}
	if len(got.Data) != 2 {
		t.Fatalf("unexpected parsed secret size: %d", len(got.Data))
	}
}

func TestSecretLegacyJSONMapKeepsKeyShape(t *testing.T) {
	got, err := parseSecretValue(`{"username":"admin","password":"secret"}`)
	if err != nil {
		t.Fatalf("parse legacy secret value: %v", err)
	}

	if !bytes.Equal(got.Data["username"], []byte("admin")) || !bytes.Equal(got.Data["password"], []byte("secret")) {
		t.Fatalf("unexpected parsed secret data: %#v", got.Data)
	}
}
