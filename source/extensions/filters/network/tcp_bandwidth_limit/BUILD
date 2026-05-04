load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_cc_library(
    name = "tcp_bandwidth_limit",
    srcs = ["tcp_bandwidth_limit.cc"],
    hdrs = ["tcp_bandwidth_limit.h"],
    deps = [
        "//envoy/buffer:buffer_interface",
        "//envoy/common:exception_lib",
        "//envoy/common:time_interface",
        "//envoy/common:token_bucket_interface",
        "//envoy/event:dispatcher_interface",
        "//envoy/event:timer_interface",
        "//envoy/grpc:async_client_manager_interface",
        "//envoy/network:connection_interface",
        "//envoy/network:filter_interface",
        "//envoy/runtime:runtime_interface",
        "//envoy/server:factory_context_interface",
        "//envoy/stats:stats_interface",
        "//envoy/stats:stats_macros",
        "//source/common/buffer:buffer_lib",
        "//source/common/buffer:watermark_buffer_lib",
        "//source/common/common:assert_lib",
        "//source/common/common:logger_lib",
        "//source/common/common:shared_token_bucket_impl_lib",
        "//source/common/grpc:typed_async_client_lib",
        "//source/common/runtime:runtime_lib",
        "//source/extensions/common/distributed_token_bucket:distributed_token_bucket_lib",
        "//source/extensions/common/distributed_token_bucket:grpc_lease_fetcher_lib",
        "@envoy_api//envoy/extensions/filters/network/tcp_bandwidth_limit/v3:pkg_cc_proto",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":tcp_bandwidth_limit",
        "//envoy/registry",
        "//envoy/server:filter_config_interface",
        "//source/extensions/filters/network:well_known_names",
        "//source/extensions/filters/network/common:factory_base_lib",
        "@envoy_api//envoy/extensions/filters/network/tcp_bandwidth_limit/v3:pkg_cc_proto",
    ],
)
