package controller

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// CredentialSetter sets the superuser password inside a running pod. Warm pods are initdb'd with
// the pool's bootstrap password, so a claim must replace it before the instance is handed out.
type CredentialSetter interface {
	SetPassword(ctx context.Context, pod *corev1.Pod, password string) error
}

// ExecSetter runs psql in the pod over the exec subresource. The password goes over stdin so it
// never appears in the pod spec, the exec URL or apiserver audit logs.
type ExecSetter struct {
	Config *rest.Config
	Client kubernetes.Interface
}

func (e *ExecSetter) SetPassword(ctx context.Context, pod *corev1.Pod, password string) error {
	req := e.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).
		SubResource("exec").VersionedParams(&corev1.PodExecOptions{
		Container: containerName,
		Command:   []string{"psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-q"},
		Stdin:     true, Stdout: true, Stderr: true,
	}, scheme.ParameterCodec)

	ws, err := remotecommand.NewWebSocketExecutor(e.Config, "GET", req.URL().String())
	if err != nil {
		return err
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.Config, "POST", req.URL())
	if err != nil {
		return err
	}
	exec, err := remotecommand.NewFallbackExecutor(ws, spdy, httpstream.IsUpgradeFailure)
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	// randomPassword's alphabet needs no SQL quoting.
	stdin := bytes.NewBufferString(fmt.Sprintf("ALTER ROLE postgres PASSWORD '%s';\n", password))
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: &bytes.Buffer{}, Stderr: &stderr}); err != nil {
		return fmt.Errorf("psql in %s: %w: %s", pod.Name, err, stderr.String())
	}
	return nil
}
