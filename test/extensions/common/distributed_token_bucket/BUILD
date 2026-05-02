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
        "//source/extensions/common/distributed_token_bucket:distributed_token_bucket_lib",
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_interface",
        "//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "lease_fetcher_factory_test",
    srcs = ["lease_fetcher_factory_test.cc"],
    deps = [
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_factory_lib",
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_interface",
        "//test/mocks/event:event_mocks",
        "//test/mocks/stats:stats_mocks",
        "//test/mocks/upstream:cluster_manager_mocks",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:utility_lib",
    ],
)

envoy_cc_test(
    name = "redis_lease_fetcher_test",
    srcs = ["redis_lease_fetcher_test.cc"],
    deps = [
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_factory_lib",
        "//source/extensions/common/distributed_token_bucket:lease_fetcher_interface",
        "//source/extensions/common/distributed_token_bucket:redis_lease_fetcher_lib",
        "//source/extensions/filters/network/common/redis:client_interface",
        "//test/extensions/filters/network/common/redis:redis_mocks",
        "//test/mocks/event:event_mocks",
        "//test/mocks/stats:stats_mocks",
        "//test/mocks/upstream:cluster_manager_mocks",
        "//test/mocks/upstream:load_balancer_mocks",
        "//test/mocks/upstream:thread_local_cluster_mocks",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:utility_lib",
    ],
)
