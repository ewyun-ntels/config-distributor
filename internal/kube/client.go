package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Client struct {
	cs kubernetes.Interface
}

type configMapEnvelope struct {
	Format string            `json:"format,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}

type storedMetadata struct {
	Name      string            `json:"name,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type storedValueEnvelope struct {
	Data            json.RawMessage `json:"data,omitempty"`
	Metadata        *storedMetadata `json:"metadata,omitempty"`
	ResourceVersion string          `json:"resourceVersion,omitempty"`
}

type secretEnvelope struct {
	Format     string            `json:"format,omitempty"`
	StringData map[string]string `json:"stringData,omitempty"`
	BinaryData map[string]string `json:"binaryData,omitempty"`
}

type secretValues struct {
	Data map[string][]byte
}

const (
	defaultManagedLabelKey   = "config.upm.io/managed"
	defaultManagedLabelValue = "true"
)

var (
	managedLabelKey   string
	managedLabelValue string
	managedLabelOnce  sync.Once
)

func managedLabel() (string, string) {
	managedLabelOnce.Do(func() {
		managedLabelKey = getenv("MANAGED_LABEL_KEY", defaultManagedLabelKey)
		managedLabelValue = getenv("MANAGED_LABEL_VALUE", defaultManagedLabelValue)
	})
	return managedLabelKey, managedLabelValue
}

func NewClient() (*Client, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{cs: cs}, nil
}

func (c *Client) ClientSet() kubernetes.Interface {
	return c.cs
}

func loadConfig() (*rest.Config, error) {
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	home := homedir.HomeDir()
	if home == "" {
		return nil, fmt.Errorf("cannot resolve kubeconfig: set KUBECONFIG or run in cluster")
	}
	return clientcmd.BuildConfigFromFlags("", filepath.Join(home, ".kube", "config"))
}

func (c *Client) ListConfigMaps(ctx context.Context, namespace string) ([]corev1.ConfigMap, error) {
	list, err := c.cs.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) ListSecrets(ctx context.Context, namespace string) ([]corev1.Secret, error) {
	list, err := c.cs.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) UpsertConfigMap(ctx context.Context, namespace, name, value string) (*corev1.ConfigMap, error) {
	data, err := parseConfigMapValue(value)
	if err != nil {
		return nil, err
	}

	cm, err := c.cs.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Data: data,
		}
		ensureManagedLabels(&cm.ObjectMeta)
		return c.cs.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	cm.Data = data
	ensureManagedLabels(&cm.ObjectMeta)
	return c.cs.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
}

func (c *Client) UpsertSecret(ctx context.Context, namespace, name, value string) (*corev1.Secret, error) {
	values, err := parseSecretValue(value)
	if err != nil {
		return nil, err
	}

	sec, err := c.cs.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Data: values.Data,
			Type: corev1.SecretTypeOpaque,
		}
		ensureManagedLabels(&sec.ObjectMeta)
		return c.cs.CoreV1().Secrets(namespace).Create(ctx, sec, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if sec.Type == "" {
		sec.Type = corev1.SecretTypeOpaque
	}
	sec.Data = values.Data
	sec.StringData = nil
	ensureManagedLabels(&sec.ObjectMeta)
	return c.cs.CoreV1().Secrets(namespace).Update(ctx, sec, metav1.UpdateOptions{})
}

func (c *Client) DeleteConfigMap(ctx context.Context, namespace, name string) error {
	err := c.cs.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *Client) DeleteSecret(ctx context.Context, namespace, name string) error {
	err := c.cs.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func IsManagedConfigMap(cm *corev1.ConfigMap) bool {
	if cm == nil || cm.Labels == nil {
		return false
	}
	key, value := managedLabel()
	return cm.Labels[key] == value
}

func IsManagedSecret(sec *corev1.Secret) bool {
	if sec == nil || sec.Labels == nil {
		return false
	}
	key, value := managedLabel()
	return sec.Labels[key] == value
}

func ensureManagedLabels(meta *metav1.ObjectMeta) {
	if meta == nil {
		return
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	key, value := managedLabel()
	meta.Labels[key] = value
}

func ValueFromConfigMap(cm *corev1.ConfigMap) string {
	if cm == nil {
		return ""
	}
	if len(cm.Data) == 0 {
		return ""
	}
	return wrapStoredValue(cm.ObjectMeta, cm.Data, cm.ResourceVersion)
}

func ValueFromSecret(sec *corev1.Secret) string {
	if sec == nil {
		return ""
	}
	if len(sec.Data) == 0 {
		return ""
	}
	env := secretEnvelope{
		Format:     "secret/v1",
		StringData: map[string]string{},
		BinaryData: map[string]string{},
	}
	for k, v := range sec.Data {
		if utf8.Valid(v) {
			env.StringData[k] = string(v)
		} else {
			env.BinaryData[k] = base64.StdEncoding.EncodeToString(v)
		}
	}
	if len(env.StringData) == 0 {
		env.StringData = nil
	}
	if len(env.BinaryData) == 0 {
		env.BinaryData = nil
	}
	return wrapStoredValue(sec.ObjectMeta, env, sec.ResourceVersion)
}

func parseConfigMapValue(value string) (map[string]string, error) {
	payload := extractStoredPayload([]byte(value))

	var env configMapEnvelope
	if err := json.Unmarshal(payload, &env); err == nil && len(env.Data) > 0 {
		return env.Data, nil
	}

	var legacy map[string]string
	if err := json.Unmarshal(payload, &legacy); err == nil && len(legacy) > 0 {
		return legacy, nil
	}

	return map[string]string{"value": string(payload)}, nil
}

func parseSecretValue(value string) (secretValues, error) {
	payload := extractStoredPayload([]byte(value))

	var env secretEnvelope
	if err := json.Unmarshal(payload, &env); err == nil && (len(env.StringData) > 0 || len(env.BinaryData) > 0) {
		data := make(map[string][]byte, len(env.StringData)+len(env.BinaryData))
		for k, v := range env.StringData {
			data[k] = []byte(v)
		}
		for k, v := range env.BinaryData {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return secretValues{}, fmt.Errorf("decode binary secret field %q: %w", k, err)
			}
			data[k] = decoded
		}
		return secretValues{Data: data}, nil
	}

	var legacy map[string]string
	if err := json.Unmarshal(payload, &legacy); err == nil && len(legacy) > 0 {
		data := make(map[string][]byte, len(legacy))
		for k, v := range legacy {
			data[k] = []byte(v)
		}
		return secretValues{Data: data}, nil
	}

	return secretValues{
		Data: map[string][]byte{"value": payload},
	}, nil
}

func ExtractResourceVersion(value []byte) string {
	var env storedValueEnvelope
	if err := json.Unmarshal(value, &env); err == nil {
		return env.ResourceVersion
	}
	return ""
}

func ExtractData(value []byte) []byte {
	return extractStoredPayload(value)
}

func wrapStoredValue(meta metav1.ObjectMeta, payload any, resourceVersion string) string {
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	value, err := json.Marshal(storedValueEnvelope{
		Data: data,
		Metadata: &storedMetadata{
			Name:      meta.Name,
			Namespace: meta.Namespace,
			Labels:    meta.Labels,
		},
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return ""
	}
	return string(value)
}

func extractStoredPayload(value []byte) []byte {
	var env storedValueEnvelope
	if err := json.Unmarshal(value, &env); err == nil && len(env.Data) > 0 {
		return env.Data
	}
	return value
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
