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
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"

	api "github.com/containerd/cri/pkg/api/v1"
	containerstore "github.com/containerd/cri/pkg/store/container"
)

// ResetContainer resets the container back to a created state and recreates associated resources
func (c *criService) ResetContainer(ctx context.Context, r *api.ResetContainerRequest) (retRes *api.ResetContainerResponse, retErr error) {
	cntr, err := c.containerStore.Get(r.GetContainerId())
	if err != nil {
		return nil, errors.Wrapf(err, "an error occurred when try to find container %q", r.GetContainerId())
	}

	if err := c.resetContainer(ctx, cntr); err != nil {
		return nil, err
	}

	return &api.ResetContainerResponse{}, nil
}

// setContainerResetting sets the container into the resetting state, so it will not
// be removed, started, or reset again.
func setContainerResetting(container containerstore.Container) error {
	return container.Status.Update(func(status containerstore.Status) (containerstore.Status, error) {
		// Return error if container is not in exited state
		if status.State() != runtime.ContainerState_CONTAINER_EXITED {
			return status, errors.New("container has not exited, stop first")
		}

		// Do not start the container when there is a removal in progress.
		if status.Removing {
			return status, errors.New("container is in removing state, can't be reset")
		}
		if status.Resetting {
			return status, errors.New("container is already resetting state")
		}
		if status.Starting {
			return status, errors.New("container is in starting state, cant be reset")
		}
		status.Resetting = true
		return status, nil
	})
}

// resetContainerState resets the container status from exited to starting
// this should only be done after the container is put in the resetting state
func resetContainerStatus(container containerstore.Container) error {
	return container.Status.Update(func(status containerstore.Status) (containerstore.Status, error) {
		// retain original CreatedAt time, since resetting is not recreation
		status.Message = "resetting"
		// reset state
		status.Pid = 0
		status.ExitCode = 0
		status.StartedAt = 0
		status.FinishedAt = 0
		return status, nil
	})
}

// resetContainerStarting resets the container resetting state
func resetContainerResetting(container containerstore.Container) error {
	return container.Status.Update(func(status containerstore.Status) (containerstore.Status, error) {
		status.Resetting = false
		return status, nil
	})
}
