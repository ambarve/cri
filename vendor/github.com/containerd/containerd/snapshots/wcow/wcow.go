//go:build windows
// +build windows

/*
   Copyright The containerd Authors.

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

package wcow

import (
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots/windows"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type:   plugin.SnapshotPlugin,
		ID:     "windows",
		Config: &windows.WindowsSnapshotterConfig{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			return windows.NewWCOWSnapshotter(ic)
		},
	})

	plugin.Register(&plugin.Registration{
		Type:   plugin.SnapshotPlugin,
		ID:     "cimfs",
		Config: &windows.WindowsSnapshotterConfig{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			return windows.NewCimfsSnapshotter(ic)
		},
	})
}
