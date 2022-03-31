/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	api "github.com/containerd/cri/pkg/api/v1"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// ResetPodSandbox resets resources and state of the sandbox and its associated containers
func (c *criService) ResetPodSandbox(ctx context.Context, r *api.ResetPodSandboxRequest) (_ *api.ResetPodSandboxResponse, retErr error) {
	id := r.GetPodSandboxId()
	sandbox, err := c.sandboxStore.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "an error occurred when try to find sandbox %q", id)
	}

	if err := c.resetSandbox(ctx, sandbox); err != nil {
		return nil, err
	}

	return &api.ResetPodSandboxResponse{}, nil
}
