package v1alpha1

import "k8s.io/apimachinery/pkg/runtime/schema"

const (
	GroupName = "resources.cfg-distributor.io"
	Version   = "v1alpha1"
)

var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}
