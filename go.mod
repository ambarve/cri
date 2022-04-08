module github.com/containerd/cri

go 1.14

require (
	github.com/Microsoft/go-winio v0.5.1
	github.com/Microsoft/hcsshim v0.9.1
	github.com/containerd/cgroups v1.0.2
	github.com/containerd/containerd v1.5.9
	github.com/containerd/containerd/api v1.6.0-beta.3
	github.com/containerd/continuity v0.2.2-0.20211201162329-8e53e7cac79d
	github.com/containerd/fifo v1.0.0
	github.com/containerd/go-cni v1.1.1-0.20211026134925-aa8bf14323a5
	github.com/containerd/typeurl v1.0.2
	github.com/containernetworking/plugins v1.0.1
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker v17.12.0-ce-rc1.0.20200310163718-4634ce647cf2+incompatible
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/moby/sys/signal v0.6.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.0.2
	github.com/opencontainers/runc v1.0.3
	github.com/opencontainers/runtime-spec v1.0.3-0.20210326190908-1c3f411f0417
	github.com/opencontainers/runtime-tools v0.7.1-0.20181011054405-1d69bd0f9c39
	github.com/opencontainers/selinux v1.8.2
	github.com/pelletier/go-toml v1.9.3
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/gocapability v0.0.0-20200815063812-42c35b437635
	github.com/urfave/cli v1.22.2
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	golang.org/x/net v0.0.0-20211216030914-fe4d6282115f
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	google.golang.org/grpc v1.43.0
	k8s.io/apimachinery v0.22.1
	k8s.io/client-go v0.22.0
	k8s.io/cri-api v0.23.0-alpha.4
	k8s.io/klog v0.2.1-0.20190222023857-8145543d67ad
	k8s.io/utils v0.0.0-20210930125809-cb0fa318a74b
)

replace (
	github.com/containerd/containerd => github.com/kevpar/containerd v1.2.1-0.20220329205807-c612c11661fa
	github.com/opencontainers/image-spec => github.com/kevpar/image-spec v1.0.2-0.20201102000608-deb02d24daef // fork
	// replace genproto and grpc to prevent panic in ttrpc module
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20200224152610-e50cd9704f63
)
