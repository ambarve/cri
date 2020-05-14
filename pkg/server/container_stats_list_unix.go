// +build !windows

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
	"github.com/containerd/cgroups"
	"github.com/containerd/containerd/api/types"
	containerstore "github.com/containerd/cri/pkg/store/container"
	"github.com/containerd/typeurl"
	"github.com/pkg/errors"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

func (c *criService) getContainerMetrics(
	meta containerstore.Metadata,
	stats *types.Metric,
) (*runtime.ContainerStats, error) {
	var cs runtime.ContainerStats
	var usedBytes, inodesUsed uint64
	sn, err := c.snapshotStore.Get(meta.ID)
	// If snapshotstore doesn't have cached snapshot information
	// set WritableLayer usage to zero
	if err == nil {
		usedBytes = sn.Size
		inodesUsed = sn.Inodes
	}
	cs.WritableLayer = &runtime.FilesystemUsage{
		Timestamp: sn.Timestamp,
		FsId: &runtime.FilesystemIdentifier{
			Mountpoint: c.imageFSPath,
		},
		UsedBytes:  &runtime.UInt64Value{Value: usedBytes},
		InodesUsed: &runtime.UInt64Value{Value: inodesUsed},
	}
	cs.Attributes = &runtime.ContainerAttributes{
		Id:          meta.ID,
		Metadata:    meta.Config.GetMetadata(),
		Labels:      meta.Config.GetLabels(),
		Annotations: meta.Config.GetAnnotations(),
	}

	if stats != nil {
		s, err := typeurl.UnmarshalAny(stats.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract container metrics")
		}
		metrics := s.(*cgroups.Metrics)
		if metrics.CPU != nil && metrics.CPU.Usage != nil {
			cs.Cpu = &runtime.CpuUsage{
				Timestamp:            stats.Timestamp.UnixNano(),
				UsageCoreNanoSeconds: &runtime.UInt64Value{Value: metrics.CPU.Usage.Total},
			}
		}
		if metrics.Memory != nil && metrics.Memory.Usage != nil {
			cs.Memory = &runtime.MemoryUsage{
				Timestamp: stats.Timestamp.UnixNano(),
				WorkingSetBytes: &runtime.UInt64Value{
					Value: getWorkingSet(metrics.Memory),
				},
			}
		}
	}

	return &cs, nil
}
