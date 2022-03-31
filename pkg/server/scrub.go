package server

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/opencontainers/runtime-spec/specs-go"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// scrubbing utilities to remove user information from arbitrary objects

const _scrubbedReplacement = "<scrubbed>"

// Scrub returns a fmt.Formatter to be used with fmt.Printf and related functions that prints a
// a copy of i with the fields containing user data (ie, Environment) set to `"<scrubbed>"`.
// Objects passed in should as copies of the original, and *NOT* be pointers
func (c *criService) Scrub(i interface{}) fmt.Formatter {
	if !c.config.ScrubLogs {
		return spew.NewFormatter(i)
	}

	switch o := i.(type) {
	case specs.Spec:
		p := *o.Process // make a copy of the process
		p.Env = []string{_scrubbedReplacement}
		o.Process = &p
		return spew.NewFormatter(o)
	case runtime.ContainerConfig:
		o.Envs = []*runtime.KeyValue{{Key: _scrubbedReplacement, Value: _scrubbedReplacement}}
		return spew.NewFormatter(o)
	}

	return spew.NewFormatter(i)
}
