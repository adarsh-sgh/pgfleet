package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Compute is the CPU and memory of one Postgres container. Requests equal limits (Guaranteed QoS),
// which in-place resize requires to stay constant.
type Compute struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
}

// DeletionPolicy decides what happens to the data volume when an instance is deleted.
// +kubebuilder:validation:Enum=Delete;Retain
type DeletionPolicy string

const (
	DeletionPolicyDelete DeletionPolicy = "Delete"
	DeletionPolicyRetain DeletionPolicy = "Retain"
)

// PostgresInstanceSpec is the desired state of one Postgres server.
// +kubebuilder:validation:XValidation:rule="has(self.storage) == has(oldSelf.storage)",message="storage cannot be added or removed"
type PostgresInstanceSpec struct {
	// Postgres major version; selects image postgres:<version>-alpine unless Image is set.
	// +kubebuilder:default="17"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="version is immutable"
	Version string `json:"version,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// Changed in place on the running pod (no reschedule).
	Resources Compute `json:"resources"`
	// Size of the data volume, grow-only. Unset means ephemeral (emptyDir), e.g. preview branches.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// WarmPool to claim a pre-initialized pod from. Falls back to a cold create when the pool
	// is empty or incompatible.
	// +optional
	PoolRef string `json:"poolRef,omitempty"`
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

// Source is how an instance got its pod.
type Source string

const (
	SourceWarm Source = "warm"
	SourceCold Source = "cold"
)

// Condition types on PostgresInstance.
const (
	CondReady          = "Ready"          // pod ready, service up, credentials owned by this instance
	CondComputeResized = "ComputeResized" // running container resources match spec
	CondStorageResized = "StorageResized" // volume capacity matches spec
	CondCredentials    = "CredentialsRotated"
)

type PostgresInstanceStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Source Source `json:"source,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	PVCName string `json:"pvcName,omitempty"`
	// Secret holding the superuser password (key "password").
	// +optional
	SecretName string `json:"secretName,omitempty"`
	// host:port of the instance's Service.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgi
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.status.source`
// +kubebuilder:printcolumn:name="CPU",type=string,JSONPath=`.spec.resources.cpu`
// +kubebuilder:printcolumn:name="Memory",type=string,JSONPath=`.spec.resources.memory`
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.spec.storage`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PostgresInstance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresInstanceSpec   `json:"spec"`
	Status PostgresInstanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PostgresInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresInstance `json:"items"`
}

// WarmPoolSpec keeps Size pods booted, initdb'd and passing readiness, ready to be claimed.
type WarmPoolSpec struct {
	// +kubebuilder:validation:Minimum=0
	Size int32 `json:"size"`
	// +kubebuilder:default="17"
	Version string `json:"version,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// Claims with different resources are resized in place after the claim.
	Resources Compute `json:"resources"`
	// Unset means ephemeral pods. Claims asking for more storage expand the volume after the claim.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

type WarmPoolStatus struct {
	// Warm pods passing readiness.
	// +optional
	Ready int32 `json:"ready,omitempty"`
	// Warm pods still booting.
	// +optional
	Warming int32 `json:"warming,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Warming",type=integer,JSONPath=`.status.warming`
type WarmPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WarmPoolSpec   `json:"spec"`
	Status WarmPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type WarmPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WarmPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresInstance{}, &PostgresInstanceList{}, &WarmPool{}, &WarmPoolList{})
}

// ImageFor resolves the container image for a version/image pair.
func ImageFor(version, image string) string {
	if image != "" {
		return image
	}
	if version == "" {
		version = "17"
	}
	return "postgres:" + version + "-alpine"
}
