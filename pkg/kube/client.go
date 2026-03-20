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
	cm, err := c.cs.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Data: map[string]string{"value": value},
		}
		return c.cs.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["value"] = value
	return c.cs.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
}

func (c *Client) UpsertSecret(ctx context.Context, namespace, name, value string) (*corev1.Secret, error) {
	sec, err := c.cs.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			StringData: map[string]string{"value": value},
			Type:       corev1.SecretTypeOpaque,
		}
		return c.cs.CoreV1().Secrets(namespace).Create(ctx, sec, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if sec.StringData == nil {
		sec.StringData = map[string]string{}
	}
	sec.StringData["value"] = value
	return c.cs.CoreV1().Secrets(namespace).Update(ctx, sec, metav1.UpdateOptions{})
}

func (c *Client) DeleteConfigMap(ctx context.Context, namespace, name string) error {
	return c.cs.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *Client) DeleteSecret(ctx context.Context, namespace, name string) error {
	return c.cs.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
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

func ValueFromConfigMap(cm *corev1.ConfigMap) string {
	if cm == nil {
		return ""
	}
	if v, ok := cm.Data["value"]; ok {
		return v
	}
	if len(cm.Data) == 1 {
		for _, v := range cm.Data {
			return v
		}
	}
	if len(cm.Data) > 0 {
		if b, err := json.Marshal(cm.Data); err == nil {
			return string(b)
		}
	}
	return ""
}

func ValueFromSecret(sec *corev1.Secret) string {
	if sec == nil {
		return ""
	}
	if v, ok := sec.Data["value"]; ok {
		if utf8.Valid(v) {
			return string(v)
		}
		return base64.StdEncoding.EncodeToString(v)
	}
	if len(sec.Data) == 1 {
		for _, v := range sec.Data {
			if utf8.Valid(v) {
				return string(v)
			}
			return base64.StdEncoding.EncodeToString(v)
		}
	}
	if len(sec.Data) > 0 {
		m := make(map[string]string, len(sec.Data))
		for k, v := range sec.Data {
			if utf8.Valid(v) {
				m[k] = string(v)
			} else {
				m[k] = base64.StdEncoding.EncodeToString(v)
			}
		}
		if b, err := json.Marshal(m); err == nil {
			return string(b)
		}
	}
	return ""
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
