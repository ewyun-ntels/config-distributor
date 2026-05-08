package kube

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const ConfigMapValueKey = "value"

type ConfigMapUpsertAction string

const (
	ConfigMapActionCreate   ConfigMapUpsertAction = "create"
	ConfigMapActionUpdate   ConfigMapUpsertAction = "update"
	ConfigMapActionNoop     ConfigMapUpsertAction = "noop"
	ConfigMapActionConflict ConfigMapUpsertAction = "conflict"
)

type Client struct {
	cs           kubernetes.Interface
	managedLabel ManagedLabel
}

type ManagedLabel struct {
	Key   string
	Value string
}

func NewClient(managedLabel ManagedLabel) (*Client, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{cs: cs, managedLabel: managedLabel}, nil
}

func (c *Client) ClientSet() kubernetes.Interface {
	return c.cs
}

func (c *Client) ManagedLabelSelector() string {
	return fmt.Sprintf("%s=%s", c.managedLabel.Key, c.managedLabel.Value)
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

func (c *Client) UpsertConfigMapSingleKey(ctx context.Context, namespace, name, value string) (ConfigMapUpsertAction, error) {
	desiredData := map[string]string{ConfigMapValueKey: value}

	cm, err := c.cs.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Data: desiredData,
		}
		c.ensureManagedLabels(&cm.ObjectMeta)
		_, err := c.cs.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
		return ConfigMapActionCreate, err
	}
	if err != nil {
		return "", err
	}

	if !c.hasManagedLabel(&cm.ObjectMeta) {
		return ConfigMapActionConflict, fmt.Errorf("configmap %s/%s exists without managed label", namespace, name)
	}

	if hasSingleValue(cm.Data, value) {
		return ConfigMapActionNoop, nil
	}

	cm.Data = desiredData
	c.ensureManagedLabels(&cm.ObjectMeta)
	_, err = c.cs.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return ConfigMapActionUpdate, err
}

func (c *Client) hasManagedLabel(meta *metav1.ObjectMeta) bool {
	if meta == nil || meta.Labels == nil {
		return false
	}
	return meta.Labels[c.managedLabel.Key] == c.managedLabel.Value
}

func (c *Client) ensureManagedLabels(meta *metav1.ObjectMeta) {
	if meta == nil {
		return
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	meta.Labels[c.managedLabel.Key] = c.managedLabel.Value
}

func ValueFromConfigMap(cm *corev1.ConfigMap) (string, bool) {
	if cm == nil || len(cm.Data) != 1 {
		return "", false
	}
	for _, value := range cm.Data {
		return value, true
	}
	return "", false
}

func hasSingleValue(data map[string]string, value string) bool {
	return len(data) == 1 && data[ConfigMapValueKey] == value
}
