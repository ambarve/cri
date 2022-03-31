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
	"github.com/containerd/containerd"
	containerdio "github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/snapshots"
	customopts "github.com/containerd/cri/pkg/containerd/opts"
	ctrdutil "github.com/containerd/cri/pkg/containerd/util"
	"github.com/containerd/cri/pkg/netns"
	"github.com/containerd/cri/pkg/store"
	sandboxstore "github.com/containerd/cri/pkg/store/sandbox"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// ResetPodSandbox resets resources and state of the sandbox and its associated containers
func (c *criService) resetSandbox(ctx context.Context, sandbox sandboxstore.Sandbox) (retErr error) {
	var (
		id     = sandbox.ID
		entity = log.G(ctx).WithField("sandboxID", id)
	)

	// Return error if sandbox is in running in unknown state
	state := sandbox.Status.Get().State
	switch state {
	case sandboxstore.StateReady:
		entity.Debugf("sandbox is already running")
		return nil
	case sandboxstore.StateNotReady:
	default:
		return errors.Errorf("sandbox %q is in invalid state %q", id, state)
	}

	netnsPath := sandbox.NetNS.GetPath()
	entity.WithField("netNSPath", netnsPath).Debugf("recreating HCN network namespace")
	// If containerd was closed while sandbox was running, HCN namespace may not have been
	// properly closed. Loading that namespace will cause `task.New` to error if the sandbox
	// task endpoint already exists and is attached to the switch.
	// So remove the namespace and then recreate it
	nns, err := netns.NewNetNSWithPath(netnsPath, true /*removeExisting*/)
	if err != nil {
		return errors.Wrapf(err, "failed to recreate network namespace for sandbox %q", id)
	}

	defer func() {
		if retErr != nil {
			if err := sandbox.NetNS.Remove(); err != nil {
				entity.WithError(err).WithField("netNS", sandbox.NetNSPath).
					Error("Failed to remove network namespace")
			}
		}
	}()

	// Setup network for sandbox.
	// Certain VM based solutions like clear containers (Issue containerd/cri-containerd#524)
	// rely on the assumption that CRI shim will not be querying the network namespace to check the
	// network states such as IP.
	// In future runtime implementation should avoid relying on CRI shim implementation details.
	// In this case however caching the IP will add a subtle performance enhancement by avoiding
	// calls to network namespace of the pod to query the IP of the veth interface on every
	// SandboxStatus request.
	config := sandbox.Config
	sip, cnires, err := c.setupPod(ctx, id, sandbox.NetNSPath, config)
	if err != nil {
		return errors.Wrapf(err, "failed to setup network for sandbox %q", id)
	}
	defer func() {
		if retErr != nil {
			// Teardown network if an error is returned.
			if err := c.teardownPod(id, sandbox.NetNSPath, config); err != nil {
				entity.WithError(err).Errorf("Failed to destroy sandbox network")
			}
		}
	}()

	// the containerd obj stored in bbolt
	container := sandbox.Container
	spec, err := container.Spec(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get sandbox %q OCI spec", id)
	}

	// uVMs are not terminated gracefully, which can cause cached file(system) changes to
	// fail to be flushed to the underlying scratch vhd. This can cause the SCSI mount points for
	// the container scratch spaces (under `C:\c\`) to remain on uVM restart, which then causes
	// container (re)starts to fail when (re)mounting the scratch space VHDs with a
	// "The directory is not empty" error.
	// Currently, HCS does not allow any way to flush filesystem buffers on the guest from the host
	// so alternatives are to either to:
	// (1) open the VHD and delete `C:\c`, if it exists;
	// (2) exec into the uVM during reset and delete `C:\c`, if it exists;
	// (3) create a process on the guest to flush all remaining before terminating the uVM; or
	// (4) recrete the uVM scratch vhd.
	//
	// Option (1) incurs a, potentially expensive, volume mount operation; would require
	// adding functionality to either hcsshim or this package to mount a VHD for R/W operations;
	// and risks corrupting the VHD if the reparse points are not removed properly.
	//
	// Option (2) would entail execing into the uVM when reseting it, potentially raising
	// concerns security boundaries; require that the uVM images come with `cmd` and `rmdir`;
	// and require that hcsshim expose either a generic uVM exec capability to containerd or
	// the ability to reset the uVMs scratch space.
	//
	// Option (3) requires sharing an executable that flushes remaining IO operations on the guest,
	// as `fsutil volume flush` does not work on the uVMs. This raises the same issues as (2), but
	// also introduces issues with win32 API stability across different uVM versions.
	//
	// Option (4) reuses functionality already available to CRI; and only entails copying the
	// pre-existing SystemTemplateBase vhd into a new snapshot folder.
	//
	// Option (4) is implemented here.
	//
	// see also https://docs.microsoft.com/en-us/windows/win32/fileio/file-caching
	if spec.Linux == nil && spec.Windows != nil && spec.Windows.HyperV != nil {
		sso := snapshots.WithLabels(snapshots.FilterInheritedLabels(config.Annotations))
		img, err := container.Image(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get sandbox container image")
		}

		err = container.Update(ctx,
			func(ctx context.Context, client *containerd.Client, cc *containers.Container) error {
				// manually call Opts to delete and create the snapshot

				// most opts, such as NewContainerOpts, take a pointer, but DeleteOpts do not
				err := containerd.WithSnapshotCleanup(ctx, client, *cc)
				if err != nil {
					return err
				}

				err = customopts.WithNewSnapshot(id, img, sso)(ctx, client, cc)
				if err != nil {
					return err
				}

				return nil
			})
		if err != nil {
			return errors.Wrapf(err, "failed to update containerd entry for sandbox container %q", id)
		}

		entity.Debug("created new pod scratch space")
	}

	entity.Debug("updating sandbox store entry")
	sandbox, err = c.sandboxStore.Update(id,
		func(sb sandboxstore.Sandbox) (_ sandboxstore.Sandbox, err error) {
			sb.NetNS = nns
			sb.NetNSPath = netnsPath
			sb.IP = sip
			sb.CNIResult = cnires
			// Add sandbox into sandbox store in INIT state.
			sb.Container = container
			// reset stop channel, so stopping does not return immediately
			sb.StopCh = store.NewStopCh()

			return sb, nil
		})
	if err != nil {
		return errors.Wrapf(err, "failed to update CRI sandbox store entry sandbox %q", id)
	}

	// Recreate the sandbox volatile directory (if it was removed during system restart)
	// Root directory should not have been removed
	// No need to defer removal, since the sandbox already exists
	volatileSandboxRootDir := c.getVolatileSandboxRootDir(id)
	if err := c.os.MkdirAll(volatileSandboxRootDir, 0755); err != nil {
		return errors.Wrapf(err, "failed to create volatile sandbox root directory %q",
			volatileSandboxRootDir)
	}

	// Update sandbox created timestamp.
	info, err := container.Info(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get sandbox container info")
	}

	// Create sandbox task in containerd.
	name := sandbox.Metadata.Name
	entity.Tracef("Create sandbox container (id=%q, name=%q).", id, name)

	// We don't need stdio for sandbox container.
	task, err := container.NewTask(ctx, containerdio.NullIO)
	if err != nil {
		return errors.Wrap(err, "failed to create containerd task")
	}
	defer func() {
		if retErr != nil {
			deferCtx, deferCancel := ctrdutil.DeferContext()
			defer deferCancel()
			// Cleanup the sandbox container if an error is returned.
			if _, err := task.Delete(deferCtx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				entity.WithError(err).Errorf("Failed to delete sandbox container")
			}
		}
	}()

	// wait is a long running background request, no timeout needed.
	exitCh, err := task.Wait(ctrdutil.NamespacedContext())
	if err != nil {
		return errors.Wrap(err, "failed to wait for sandbox container task")
	}

	if err := task.Start(ctx); err != nil {
		return errors.Wrapf(err, "failed to start sandbox container task %q", id)
	}

	entity.Debug("resetting sandbox status to ready")
	if err := sandbox.Status.Update(func(status sandboxstore.Status) (sandboxstore.Status, error) {
		// Set the pod sandbox as ready after successfully start sandbox container.
		status.Pid = task.Pid()
		status.State = sandboxstore.StateReady
		status.CreatedAt = info.CreatedAt
		return status, nil
	}); err != nil {
		return errors.Wrap(err, "failed to update sandbox status")
	}

	// start the monitor after adding sandbox into the store, this ensures
	// that sandbox is in the store, when event monitor receives the TaskExit event.
	//
	// TaskOOM from containerd may come before sandbox is added to store,
	// but we don't care about sandbox TaskOOM right now, so it is fine.
	c.eventMonitor.startExitMonitor(context.Background(), id, task.Pid(), exitCh)

	for _, ctr := range c.containerStore.List() {
		if ctr.SandboxID == id {
			entity.WithField("containerID", ctr.ID).Debugf("resetting container")
			if err = c.resetContainer(ctx, ctr); err != nil {
				return errors.Wrapf(err, "failed to reset container %q", ctr.ID)
			}
		}
	}
	return nil
}
