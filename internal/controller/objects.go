package controller

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

const (
	LabelInstance = "pgfleet.dev/instance"
	LabelPool     = "pgfleet.dev/pool"
	LabelState    = "pgfleet.dev/state" // warm | claimed
	LabelTemplate = "pgfleet.dev/template-hash"

	StateWarm    = "warm"
	StateClaimed = "claimed"

	Finalizer     = "pgfleet.dev/finalizer"
	containerName = "postgres"
	dataDir       = "/var/lib/postgresql/data"
)

// template is what a pod is built from; a pool's and an instance's spec both reduce to it.
type template struct {
	Image            string
	Resources        pgv1.Compute
	Storage          *resource.Quantity
	StorageClassName *string
}

func poolTemplate(p *pgv1.WarmPool) template {
	return template{pgv1.ImageFor(p.Spec.Version, p.Spec.Image), p.Spec.Resources, p.Spec.Storage, p.Spec.StorageClassName}
}

func instanceTemplate(in *pgv1.PostgresInstance) template {
	return template{pgv1.ImageFor(in.Spec.Version, in.Spec.Image), in.Spec.Resources, in.Spec.Storage, in.Spec.StorageClassName}
}

func (t template) hash() string {
	b, _ := json.Marshal(t)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:5])
}

func (t template) resourceList() corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: t.Resources.CPU, corev1.ResourceMemory: t.Resources.Memory}
}

func buildPod(name, ns string, t template, pvc, secret string, labels map[string]string, owner metav1.OwnerReference) *corev1.Pod {
	vol := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	if pvc != "" {
		vol = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: withManaged(labels), OwnerReferences: []metav1.OwnerReference{owner}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  containerName,
				Image: t.Image,
				Env: []corev1.EnvVar{
					{Name: "PGDATA", Value: dataDir + "/pgdata"},
					{Name: "POSTGRES_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: "password"}}},
				},
				Ports:     []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}},
				Resources: corev1.ResourceRequirements{Requests: t.resourceList(), Limits: t.resourceList()},
				// CPU changes apply live; memory restarts Postgres so shared_buffers sizing can follow.
				ResizePolicy: []corev1.ContainerResizePolicy{
					{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired},
					{ResourceName: corev1.ResourceMemory, RestartPolicy: corev1.RestartContainer},
				},
				// TCP, not the socket: the image's initdb phase runs a socket-only temp server.
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-h", "127.0.0.1", "-U", "postgres"}}},
					PeriodSeconds: 1, FailureThreshold: 3,
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: dataDir}},
			}},
			Volumes:                       []corev1.Volume{{Name: "data", VolumeSource: vol}},
			TerminationGracePeriodSeconds: ptr(int64(10)),
		},
	}
}

func buildPVC(name, ns string, t template, labels map[string]string, owner metav1.OwnerReference) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: withManaged(labels), OwnerReferences: []metav1.OwnerReference{owner}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: t.StorageClassName,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: *t.Storage}},
		},
	}
}

func buildService(in *pgv1.PostgresInstance, owner metav1.OwnerReference) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: in.Name, Namespace: in.Namespace,
			Labels: withManaged(map[string]string{LabelInstance: in.Name}), OwnerReferences: []metav1.OwnerReference{owner}},
		// Headless: DNS resolves straight to the pod. A ClusterIP per instance cost ~200ms of IP
		// allocation per create in the load test and a kube-proxy rule set per instance on every node.
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  map[string]string{LabelInstance: in.Name},
			Ports:     []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)}},
		},
	}
}

func buildSecret(name, ns string, labels map[string]string, owner metav1.OwnerReference) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: withManaged(labels), OwnerReferences: []metav1.OwnerReference{owner}},
		StringData: map[string]string{"password": randomPassword()},
	}
}

func randomPassword() string {
	const alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 24)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b[i] = alphabet[n.Int64()]
	}
	return string(b)
}

func ownerRef(obj metav1.Object, kind string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: pgv1.GroupVersion.String(), Kind: kind, Name: obj.GetName(), UID: obj.GetUID(),
		Controller: ptr(true), BlockOwnerDeletion: ptr(true),
	}
}

func podReady(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func pvcOf(p *corev1.Pod) string {
	for _, v := range p.Spec.Volumes {
		if v.Name == "data" && v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

func container(p *corev1.Pod) *corev1.Container {
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].Name == containerName {
			return &p.Spec.Containers[i]
		}
	}
	return nil
}

func ptr[T any](v T) *T { return &v }
