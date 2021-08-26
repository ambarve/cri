# CRI Plugin Config Guide
This document provides the description of the CRI plugin configuration.

The explanation and default value of each configuration item are as follows:
```toml
# The "plugins.cri" table contains all of the server options.
[plugins.cri]

  # stream_server_address is the ip address streaming server is listening on.
  stream_server_address = "127.0.0.1"

  # stream_server_port is the port streaming server is listening on.
  stream_server_port = "0"

  # enable_selinux indicates to enable the selinux support.
  enable_selinux = false

  # sandbox_image is the image used by sandbox container.
  sandbox_image = "k8s.gcr.io/pause:3.2"

  # stats_collect_period is the period (in seconds) of snapshots stats collection.
  stats_collect_period = 10

  # systemd_cgroup enables systemd cgroup support. This only works for runtime
  # type "io.containerd.runtime.v1.linux".
  # DEPRECATED: use Runtime.Options for runtime specific config for shim v2 runtimes.
  #   For runtime "io.containerd.runc.v1", use the option `SystemdCgroup`.
  systemd_cgroup = false

  # enable_tls_streaming enables the TLS streaming support.
  # It generates a self-sign certificate unless the following x509_key_pair_streaming are both set.
  enable_tls_streaming = false

  # "plugins.cri.x509_key_pair_streaming" contains a x509 valid key pair to stream with tls.
  [plugins.cri.x509_key_pair_streaming]
    # tls_cert_file is the filepath to the certificate paired with the "tls_key_file"
    tls_cert_file = ""

    # tls_key_file is the filepath to the private key paired with the "tls_cert_file"
    tls_key_file = ""

  # max_container_log_line_size is the maximum log line size in bytes for a container.
  # Log line longer than the limit will be split into multiple lines. -1 means no
  # limit.
  max_container_log_line_size = 16384

  # disable_cgroup indicates to disable the cgroup support.
  # This is useful when the daemon does not have permission to access cgroup.
  disable_cgroup = false

  # disable_apparmor indicates to disable the apparmor support.
  # This is useful when the daemon does not have permission to access apparmor.
  disable_apparmor = false

  # restrict_oom_score_adj indicates to limit the lower bound of OOMScoreAdj to
  # the containerd's current OOMScoreAdj.
  # This is useful when the containerd does not have permission to decrease OOMScoreAdj.
  restrict_oom_score_adj = false

  # "plugins.cri.containerd" contains config related to containerd
  [plugins.cri.containerd]

    # snapshotter is the snapshotter used by containerd.
    snapshotter = "overlayfs"

    # no_pivot disables pivot-root (linux only), required when running a container in a RamDisk with runc.
    # This only works for runtime type "io.containerd.runtime.v1.linux".
    # DEPRECATED: use Runtime.Options for runtime specific config for shim v2 runtimes.
    #   For runtime "io.containerd.runc.v1", use the option `NoPivotRoot`.
    no_pivot = false

    # discard_unpacked_layers allows GC to remove layers from the content store after
    # successfully unpacking these layers to the snapshotter.
    discard_unpacked_layers = false

    # default_runtime_name is the default runtime name to use.
    default_runtime_name = "runc"

    # 'plugins."io.containerd.grpc.v1.cri".containerd.default_runtime' is the runtime to use in containerd.
    # DEPRECATED: use `default_runtime_name` and `plugins."io.containerd.grpc.v1.cri".runtimes` instead.
    # Remove in containerd 1.4.
    [plugins."io.containerd.grpc.v1.cri".containerd.default_runtime]

    # 'plugins."io.containerd.grpc.v1.cri".containerd.untrusted_workload_runtime' is a runtime to run untrusted workloads on it.
    # DEPRECATED: use `untrusted` runtime in `plugins."io.containerd.grpc.v1.cri".runtimes` instead.
    # Remove in containerd 1.4.
    [plugins."io.containerd.grpc.v1.cri".containerd.untrusted_workload_runtime]

    # 'plugins."io.containerd.grpc.v1.cri".containerd.runtimes' is a map from CRI RuntimeHandler strings, which specify types
    # of runtime configurations, to the matching configurations.
    # In this example, 'runc' is the RuntimeHandler string to match.
    [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
      # runtime_type is the runtime type to use in containerd.
      # The default value is "io.containerd.runc.v2" since containerd 1.4.
      # The default value was "io.containerd.runc.v1" in containerd 1.3, "io.containerd.runtime.v1.linux" in prior releases.
      runtime_type = "io.containerd.runc.v2"

      # pod_annotations is a list of pod annotations passed to both pod
      # sandbox as well as container OCI annotations. Pod_annotations also
      # supports golang path match pattern - https://golang.org/pkg/path/#Match.
      # e.g. ["runc.com.*"], ["*.runc.com"], ["runc.com/*"].
      #
      # For the naming convention of annotation keys, please reference:
      # * Kubernetes: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set
      # * OCI: https://github.com/opencontainers/image-spec/blob/master/annotations.md
      pod_annotations = []

      # container_annotations is a list of container annotations passed through to the OCI config of the containers.
      # Container annotations in CRI are usually generated by other Kubernetes node components (i.e., not users).
      # Currently, only device plugins populate the annotations.
      container_annotations = []

      # privileged_without_host_devices allows overloading the default behaviour of passing host
      # devices through to privileged containers. This is useful when using a runtime where it does
      # not make sense to pass host devices to the container when privileged. Defaults to false -
      # i.e pass host devices through to privileged containers.
      privileged_without_host_devices = false

      # base_runtime_spec is a file path to a JSON file with the OCI spec that will be used as the base spec that all
      # container's are created from.
      # Use containerd's `ctr oci spec > /etc/containerd/cri-base.json` to output initial spec file.
      # Spec files are loaded at launch, so containerd daemon must be restared on any changes to refresh default specs.
      # Still running containers and restarted containers will still be using the original spec from which that container was created.
      base_runtime_spec = ""

      # 'plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options' is options specific to
      # "io.containerd.runc.v1" and "io.containerd.runc.v2". Its corresponding options type is:
      #   https://github.com/containerd/containerd/blob/v1.3.2/runtime/v2/runc/options/oci.pb.go#L26 .
      [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
        # no_pivot_root disables pivot root when creating a container.
        no_pivot_root = false

        # no_new_keyring disables new keyring for the container.
        no_new_keyring = false

        # shim_cgroup places the shim in a cgroup.
        shim_cgroup = ""

        # io_uid sets the I/O's pipes uid.
        io_uid = 0

        # io_gid sets the I/O's pipes gid.
        io_gid = 0

        # binary_name is the binary name of the runc binary.
        binary_name = ""

        # root is the runc root directory.
        root = ""

        # criu_path is the criu binary path.
        criu_path = ""

        # systemd_cgroup enables systemd cgroups.
        systemd_cgroup = false

        # criu_image_path is the criu image path
        criu_image_path = ""

        # criu_work_path is the criu work path.
        criu_work_path = ""

  # 'plugins."io.containerd.grpc.v1.cri".cni' contains config related to cni
  [plugins."io.containerd.grpc.v1.cri".cni]
    # bin_dir is the directory in which the binaries for the plugin is kept.
    bin_dir = "/opt/cni/bin"

    # conf_dir is the directory in which the admin places a CNI conf.
    conf_dir = "/etc/cni/net.d"

    # conf_template is the file path of golang template used to generate
    # cni config.
    # If this is set, containerd will generate a cni config file from the
    # template. Otherwise, containerd will wait for the system admin or cni
    # daemon to drop the config file into the conf_dir.
    # This is a temporary backward-compatible solution for kubenet users
    # who don't have a cni daemonset in production yet.
    # This will be deprecated when kubenet is deprecated.
    conf_template = ""

  # "plugins.cri.registry" contains config related to the registry
  [plugins.cri.registry]

    # "plugins.cri.registry.mirrors" are namespace to mirror mapping for all namespaces.
    [plugins.cri.registry.mirrors]
      [plugins.cri.registry.mirrors."docker.io"]
        endpoint = ["https://registry-1.docker.io", ]
```
