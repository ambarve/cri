//go:build windows
// +build windows

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
	"encoding/json"
	"net/url"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/oci"
	cio "github.com/containerd/cri/pkg/server/io"
	"github.com/containerd/cri/pkg/store"
	containerstore "github.com/containerd/cri/pkg/store/container"
	sandboxstore "github.com/containerd/cri/pkg/store/sandbox"
	"github.com/containerd/typeurl"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

func (c *criService) resetContainer(ctx context.Context, container containerstore.Container) (retErr error) {
	var (
		id     = container.ID
		meta   = container.Metadata
		config = meta.Config
		entity = log.G(ctx).WithField("containerID", id)
	)

	// Return error if container is neither created nor exited
	switch state := container.Status.Get().State(); state {
	case runtime.ContainerState_CONTAINER_CREATED:
		entity.Debugf("container is already in created state")
		return nil
	case runtime.ContainerState_CONTAINER_EXITED:
	default:
		return errors.Errorf("container is in invalid state %q", criContainerStateToString(state))
	}

	// Get sandbox config from sandbox store.
	sandbox, err := c.sandboxStore.Get(meta.SandboxID)
	if err != nil {
		return errors.Wrapf(err, "sandbox %q not found", meta.SandboxID)
	}
	if sandbox.Status.Get().State != sandboxstore.StateReady {
		return errors.Errorf("sandbox container %q is not running", meta.SandboxID)
	}

	// Set resetting state to prevent other start/remove/reset operations against this container
	// while it's being reset.
	if err := setContainerResetting(container); err != nil {
		return errors.Wrapf(err, "failed to set resetting state for container %q", id)
	}
	defer func() {
		if retErr != nil {
			// Set container to exited if fail to start.
			if err := container.Status.UpdateSync(func(status containerstore.Status) (containerstore.Status, error) {
				status.Pid = 0
				status.FinishedAt = time.Now().UnixNano()
				status.ExitCode = errorStartExitCode
				status.Reason = errorResetReason
				status.Message = retErr.Error()
				return status, nil
			}); err != nil {
				entity.WithError(err).Errorf("failed to set reset failure state for container %q", id)
			}
		}
		if err := resetContainerResetting(container); err != nil {
			entity.WithError(err).Errorf("failed to reset resetting state for container %q", id)
		}
	}()

	// Root directory should already exist if container exists, however, volatile directory is
	// not guaranteed to persist across (system or containerd) restarts, so recreate it.
	// `CreateContainer` removes (volatile) container directories if container creation fails and
	// otherwise assumes directories exist if the container exits; therefore no need to defer
	// removal if restart fails.
	volatileContainerRootDir := c.getVolatileContainerRootDir(id)
	if err := c.os.MkdirAll(volatileContainerRootDir, 0755); err != nil {
		return errors.Wrapf(err, "failed to create volatile container root directory %q",
			volatileContainerRootDir)
	}

	// recreate the container IO, since it should have been closed on exit/stop
	entity.Debug("recreating container IO")
	cioOpts := cio.WithNewFIFOs(volatileContainerRootDir, config.GetTty(), config.GetStdin())

	// Get container log path.
	if config.GetLogPath() != "" {
		u, err := url.Parse(config.GetLogPath())
		if err == nil && u.Scheme == "binary" {
			cioOpts = cio.WithBinaryFIFOs(config.GetLogPath())
		}
	}

	containerIO, err := cio.NewContainerIO(id, cioOpts)
	if err != nil {
		return errors.Wrap(err, "failed to recreate container io")
	}
	defer func() {
		if retErr != nil {
			if err := containerIO.Close(); err != nil {
				log.G(ctx).WithError(err).Errorf("Failed to close container io %q", id)
			}
		}
	}()

	entity.Debug("updating container store entry")
	container, err = c.containerStore.Update(id,
		func(ctr containerstore.Container) (containerstore.Container, error) {
			ctr.IO = containerIO
			ctr.StopCh = store.NewStopCh()
			return ctr, nil
		})
	if err != nil {
		return errors.Wrapf(err, "failed to update CRI container store entry sandbox %q", id)
	}

	// container spec wont differentiate WCOW hypervisor vs process isolated, but should be fine
	// to distinguish LCOW
	s, err := container.Container.Spec(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get container %q OCI spec", id)
	}
	if s.Linux != nil {
		entity.Debug("updating containerd OCI spec with new linux namespaces")

		sb, err := sandbox.Container.Task(ctx, nil)
		if err != nil {
			return errors.Wrap(err, "failed to get sandbox container task")
		}

		err = container.Container.Update(ctx,
			func(ctx context.Context, client *containerd.Client, cc *containers.Container) error {
				// load and modify container OCI spec in one operation to prevent intermediary updates
				// between read and write
				var spec oci.Spec
				if err := json.Unmarshal(cc.Spec.Value, &spec); err != nil {
					return err
				}

				g := newSpecGenerator(&spec)
				// this will modify the underlying oci spec
				setOCINamespaces(&g, config.GetLinux().GetSecurityContext().GetNamespaceOptions(), sb.Pid())

				cc.Spec, err = typeurl.MarshalAny(&spec)
				return err
			})
		if err != nil {
			return errors.Wrapf(err, "failed to update containerd entry for container %q", id)
		}
	}

	entity.Debug("resetting container status back to created")
	// Reset container state back to created
	if err := resetContainerStatus(container); err != nil {
		return errors.Wrapf(err, "failed to update container %q state", id)
	}

	return nil
}
