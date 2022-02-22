package server

import (
	"github.com/containerd/containerd/errdefs"
	"golang.org/x/net/context"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

func (c *criService) PodSandboxStats(ctx context.Context, req *runtime.PodSandboxStatsRequest) (*runtime.PodSandboxStatsResponse, error) {
	return nil, errdefs.ErrNotImplemented
}

func (c *criService) ListPodSandboxStats(ctx context.Context, req *runtime.ListPodSandboxStatsRequest) (*runtime.ListPodSandboxStatsResponse, error) {
	return nil, errdefs.ErrNotImplemented
}
