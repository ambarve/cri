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

package netns

import (
	"fmt"
	"sync"

	"github.com/Microsoft/hcsshim/hcn"
	"github.com/pkg/errors"
)

// ErrClosedNetNS is the error returned when network namespace is closed.
var ErrClosedNetNS = errors.New("network namespace is closed")

// NetNS holds network namespace for sandbox
type NetNS struct {
	sync.Mutex
	closed   bool
	restored bool
	path     string
}

// NewNetNS creates a network namespace for the sandbox
func NewNetNS() (*NetNS, error) {
	temp := hcn.HostComputeNamespace{}
	hcnNamespace, err := temp.Create()
	if err != nil {
		return nil, err
	}

	return &NetNS{path: string(hcnNamespace.Id)}, nil
}

// NewNetNSWithPath creates a network namespace for the sandbox with the given path.
// removeExisting toggles removing any prior existing namespaces with the same path.
func NewNetNSWithPath(path string, removeExisting bool) (*NetNS, error) {
	if removeExisting {
		if err := RemoveByPath(path); err != nil {
			return nil, err
		}
	}
	temp := hcn.HostComputeNamespace{Id: path}
	hcnNamespace, err := temp.Create()
	if err != nil {
		return nil, err
	}

	if hcnNamespace.Id != path {
		return nil, fmt.Errorf("recreated network namespace is %q, not %q", hcnNamespace.Id, path)
	}
	return &NetNS{path: string(hcnNamespace.Id)}, nil
}

// LoadNetNS loads existing network namespace. It returns ErrClosedNetNS
// if the network namespace has already been closed or not found.
func LoadNetNS(path string) *NetNS {
	_, err := hcn.GetNamespaceByID(path)
	if err != nil {
		// Todo: Check for NotFound error
		return &NetNS{closed: true, path: path}
	}

	return &NetNS{restored: true, path: path}
}

// Remove removes network namepace if it exists and not closed. Remove is idempotent,
// meaning it might be invoked multiple times and provides consistent result.
func (n *NetNS) Remove() error {
	n.Lock()
	defer n.Unlock()
	if !n.closed {
		if err := RemoveByPath(n.path); err != nil {
			return err
		}
		n.closed = true
	}
	if n.restored {
		n.restored = false
	}
	return nil
}

// RemoveByPath removes the network namepace if it exists and not closed.
// RemoveByPath is idempotent, and will not error if the path does not exist.
func RemoveByPath(path string) error {
	n, err := hcn.GetNamespaceByID(path)
	if err == nil {
		n.Delete()
	} else if !hcn.IsNotFoundError(err) {
		return errors.Wrap(err, "failed while attempting to get namespace")
	}
	return nil
}

// Closed checks whether the network namespace has been closed.
func (n *NetNS) Closed() (bool, error) {
	n.Lock()
	defer n.Unlock()
	return n.closed, nil
}

// GetPath returns network namespace path for sandbox container
func (n *NetNS) GetPath() string {
	n.Lock()
	defer n.Unlock()
	return n.path
}
