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
        "//source/common/common:minimal_logger_lib",
        "//source/common/common:token_bucket_impl_lib",
        "@abseil-cpp//absl/synchronization",
    ],
)

envoy_cc_library(
    name = "lease_fetcher_factory_lib",
    srcs = ["lease_fetcher_factory.cc"],
    hdrs = ["lease_fetcher_factory.h"],
    deps = [
        ":lease_fetcher_interface",
        "//envoy/common:time_interface",
        "//envoy/event:dispatcher_interface",
        "//envoy/stats:stats_interface",
        "//envoy/upstream:cluster_manager_interface",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/synchronization",
    ],
)

envoy_cc_library(
    name = "redis_lease_fetcher_lib",
    srcs = ["redis_lease_fetcher.cc"],
    hdrs = ["redis_lease_fetcher.h"],
    deps = [
        ":lease_fetcher_factory_lib",
        ":lease_fetcher_interface",
        "//envoy/event:dispatcher_interface",
        "//envoy/network:connection_interface",
        "//envoy/upstream:cluster_manager_interface",
        "//envoy/upstream:load_balancer_interface",
        "//envoy/upstream:thread_local_cluster_interface",
        "//source/common/common:fmt_lib",
        "//source/common/common:minimal_logger_lib",
        "//source/extensions/filters/network/common/redis:client_lib",
        "//source/extensions/filters/network/common/redis:codec_lib",
        "//source/extensions/filters/network/common/redis:redis_command_stats_lib",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
    ],
)
