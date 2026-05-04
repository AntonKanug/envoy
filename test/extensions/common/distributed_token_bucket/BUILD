load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_test",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_test(
    name = "distributed_token_bucket_test",
    srcs = ["distributed_token_bucket_test.cc"],
    deps = [
        "//source/common/stats:isolated_store_lib",
        "//source/extensions/common/distributed_token_bucket:distributed_token_bucket_lib",
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_interface",
        "//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "grpc_lease_fetcher_test",
    srcs = ["grpc_lease_fetcher_test.cc"],
    deps = [
        "//source/common/buffer:buffer_lib",
        "//source/common/grpc:common_lib",
        "//source/extensions/common/distributed_token_bucket:grpc_lease_fetcher_lib",
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_interface",
        "//test/mocks/event:event_mocks",
        "//test/mocks/grpc:grpc_mocks",
        "@envoy_api//envoy/extensions/distributed_token_bucket/v3:pkg_cc_proto",
    ],
)
