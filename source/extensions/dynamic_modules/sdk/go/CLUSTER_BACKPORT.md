# Go cluster SDK backport

This branch adds the merged Go cluster lifecycle SDK to the Go module from Envoy
commit `9e5580933739775e8bce696a57a2e1bc99368f23`. It retains that module's existing
filter bindings and ABI header for consumers running Envoy 1.40.0.

The source is [envoyproxy/envoy#49522](https://github.com/envoyproxy/envoy/pull/49522),
merged as `bcad0e308cb04405128cfc6eb1d7c15956b7571c`. The complete newer module
also references filter callbacks absent from Envoy 1.40.0; consumers of this
backport must identify this branch's source commit rather than the upstream merge
commit as their dependency version.

The following files are copied unchanged from the merged upstream source:

- `abi/cluster.go` and `abi/handle.go`
- `internal/scheduler/scheduler.go` and `internal/scheduler/scheduler_test.go`
- `shared/cluster.go`, `shared/cluster_api.go`, and `shared/common.go`
- `shared/mocks/mock_cluster.go` and `shared/mocks/mock_cluster_api.go`

The only integrations into existing files are the upstream
`dymCommonHandle.GetServerConcurrency` method in `abi/common.go`, the cluster
configuration registry and its functions in `sdk.go`, and
`GetServerConcurrency` methods in the existing HTTP and network configuration
mocks. The nested module's `go.mod`, `go.sum`, ABI header, and filter-specific APIs
remain at the base commit.

The cluster API and scheduler behavior match the merged implementation. Acquire
the scheduler in `Cluster.OnInit`, configure a native load-balancing policy such
as `ROUND_ROBIN`, and set `dynamic_module_config.do_not_close` to true. The SDK
scheduler has no queue limit; callers must bound the tasks they submit.

From `source/extensions/dynamic_modules`, run `go test ./sdk/go/...` to check the
Go SDK, including scheduler shutdown and concurrent dispatch tests. Native load
and lifecycle validation must use the actual host binary consuming the module.
