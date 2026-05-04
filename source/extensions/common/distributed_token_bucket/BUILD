load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_library(
    name = "lease_fetcher_interface",
    hdrs = ["lease_fetcher.h"],
    deps = [
        "//envoy/common:pure_lib",
    ],
)

envoy_cc_library(
    name = "distributed_token_bucket_lib",
    srcs = ["distributed_token_bucket.cc"],
    hdrs = ["distributed_token_bucket.h"],
    deps = [
        ":lease_fetcher_interface",
        "//envoy/common:time_interface",
        "//envoy/common:token_bucket_interface",
        "//envoy/stats:stats_interface",
        "//envoy/stats:stats_macros",
        "//source/common/common:minimal_logger_lib",
        "//source/common/common:token_bucket_impl_lib",
        "@abseil-cpp//absl/synchronization",
    ],
)

envoy_cc_library(
    name = "grpc_lease_fetcher_lib",
    srcs = ["grpc_lease_fetcher.cc"],
    hdrs = ["grpc_lease_fetcher.h"],
    deps = [
        ":lease_fetcher_interface",
        "//envoy/event:dispatcher_interface",
        "//envoy/grpc:async_client_interface",
        "//envoy/tracing:tracer_interface",
        "//source/common/common:assert_lib",
        "//source/common/common:minimal_logger_lib",
        "//source/common/grpc:typed_async_client_lib",
        "//source/common/tracing:null_span_lib",
        "@envoy_api//envoy/extensions/distributed_token_bucket/v3:pkg_cc_proto",
    ],
)
