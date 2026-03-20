package v1alpha1

type itemResponse struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Revision  uint64 `json:"revision"`
	Value     string `json:"value"`
}

type listResponse struct {
	Namespace string         `json:"namespace"`
	Kind      string         `json:"kind"`
	Items     []itemResponse `json:"items"`
}
