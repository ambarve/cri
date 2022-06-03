package server

import (
	"fmt"
	"reflect"

	"github.com/davecgh/go-spew/spew"
	"github.com/opencontainers/runtime-spec/specs-go"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// scrubbing utilities to remove user information from arbitrary objects

const scrubbedReplacement = "<scrubbed>"

var (
	// Predeclared scrubbed blocks to be passed into the spew formatter; spew should not modify them
	// so there is minimal risk of mutated state and leaked values between `Scrub` calls.

	_scrubbedAnnotations = map[string]string{scrubbedReplacement: scrubbedReplacement}
	_scrubbedEnvList     = []string{scrubbedReplacement}
	_scrubbedEnvKV       = []*runtime.KeyValue{{Key: scrubbedReplacement, Value: scrubbedReplacement}}
)

// Scrub returns a scrubbed copy of the input as a `fmt.Formatter` that can to be used with
// `fmt.Printf` and related functions.
// Scrubbing sets potentially sensitive fields (eg, Annotations,
// Environment Variables) to `"<scrubbed>"`.
//
// Objects passed in by value (as a copy of the original), and *NOT* by reference (as a pointer)
func (c *criService) Scrub(i interface{}) fmt.Formatter {
	if !c.config.ScrubLogs {
		return spew.NewFormatter(i)
	}

	if r := reflect.ValueOf(i); r.Type().Kind() == reflect.Ptr {
		// if a pointer is passed, dereference it and scrub the copy
		i = r.Elem().Interface()
	}

	switch o := i.(type) {
	case specs.Spec:
		p := *o.Process // make a copy of the process
		p.Env = _scrubbedEnvList
		o.Process = &p
		o.Annotations = _scrubbedAnnotations
		return spew.NewFormatter(o)
	case runtime.ContainerConfig:
		o.Envs = _scrubbedEnvKV
		o.Annotations = _scrubbedAnnotations
		return spew.NewFormatter(o)
	case runtime.PodSandboxConfig:
		// scrub annotations but not Environment
		o.Annotations = _scrubbedAnnotations
		return spew.NewFormatter(o)

	}

	return spew.NewFormatter(i)
}
