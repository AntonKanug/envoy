.. _config_network_filters_tcp_bandwidth_limit:

TCP bandwidth limit
===================

* This filter should be configured with the type URL ``type.googleapis.com/envoy.extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit``.
* :ref:`v3 API reference <envoy_v3_api_msg_extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit>`

Overview
--------

The TCP bandwidth limit filter is a network filter that limits the bandwidth on the downstream
connection. It can be configured to limit read and write bandwidth independently, using a token
bucket algorithm similar to the HTTP bandwidth limit filter.

- ``read_limit_kbps`` limits data read from the downstream connection.
- ``write_limit_kbps`` limits data written to the downstream connection.

The filter works by:

* Consuming tokens from a token bucket when data passes through
* Buffering data when insufficient tokens are available
* Using timers to refill tokens and resume data flow when bandwidth becomes available

Example configuration
---------------------

The following example configuration limits read bandwidth to 1 MiB/s and write bandwidth to 512 KiB/s:

.. code-block:: yaml

  name: envoy.filters.network.tcp_bandwidth_limit
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit
    stat_prefix: bandwidth_limiter
    read_limit_kbps: 1024   # 1 MiB/s
    write_limit_kbps: 512   # 512 KiB/s
    fill_interval:
      nanos: 50000000       # 50ms

Distributed mode
----------------

By default each Envoy enforces ``read_limit_kbps`` / ``write_limit_kbps``
independently in-process: with ``N`` Envoys you get ``N`` × the limit aggregate.
To share a single limit across all instances, set the
:ref:`distributed <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit.distributed>`
field. The filter then enforces the limit via a leased token bucket fetched
from an external **bandwidth quota gRPC service** shared across the fleet.

The service is a small standalone gRPC server implementing
``envoy.extensions.distributed_token_bucket.v3.BandwidthQuotaService.AcquireLease``;
the reference implementation backs the bucket with Redis. Envoy itself does
not speak to Redis directly — it talks gRPC to the service, the service talks
Redis. This matches the same shape as the rate-limit, ext-authz and ext-proc
data-plane callouts and inherits Envoy's standard gRPC client features (mTLS,
retries, outlier detection, cluster pooling).

How it works:

* Each Envoy keeps a small **local lease** of tokens. Reads/writes draw from
  this lease synchronously inside the worker thread.
* When the local lease drops below a threshold an asynchronous ``AcquireLease``
  RPC is dispatched against the configured
  :ref:`quota_service <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.quota_service>`.
  The service atomically advances the global bucket and returns the granted
  token count.
* Read and write directions automatically use **different bucket keys**: the
  ``key`` you configure is suffixed with ``:read`` and ``:write`` so the two
  directions track independent global pools and never collide on a single
  global counter.
* If
  :ref:`lease_kb <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.lease_kb>`
  is unset, an automatic value is derived from the configured rate (~ one
  ``fill_interval``'s worth of bandwidth) and clamped to ``[64 KiB, 16 MiB]``.
* If the quota service is unreachable and
  :ref:`fail_open <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.fail_open>`
  is ``true``, each Envoy falls back to a local-only token bucket at
  :ref:`fail_open_kbps <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.fail_open_kbps>`
  per instance. ``fail_open`` defaults to ``false`` (the safer choice for
  distributed semantics — when ``true``, an outage allows N × the configured
  fallback rate until recovery).

The bucket is approximate: with ``N`` instances and lease size ``L`` the total
delivery may exceed the global limit by up to ``N`` × ``L`` during refill bursts.
Smaller leases narrow the window at the cost of higher service QPS.

Example: 1 MiB/s shared across all Envoys keyed by tenant:

.. code-block:: yaml

  name: envoy.filters.network.tcp_bandwidth_limit
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit
    stat_prefix: bandwidth_limiter
    read_limit_kbps: 1024   # 1 MiB/s GLOBAL
    write_limit_kbps: 1024
    distributed:
      quota_service:
        envoy_grpc:
          cluster_name: bandwidth_quota_cluster
      key: tenant.acme.bandwidth
      fail_open: false

The ``quota_service`` is a standard
:ref:`GrpcService <envoy_v3_api_msg_config.core.v3.GrpcService>` reference, so
mTLS, retries and any other transport-socket / cluster configuration are owned
by the cluster.

In addition to the regular per-direction stats, distributed mode emits a set
of stats under ``<stat_prefix>.distributed_bandwidth_limit.``:

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  lease_requests_total, Counter, Number of AcquireLease calls dispatched
  lease_failures_total, Counter, Number of AcquireLease calls that failed (transport error or non-OK status)
  lease_granted_bytes_total, Counter, Sum of granted_tokens across successful calls
  fail_open_active, Gauge, 1 while the bucket is operating in fail-open fallback, 0 otherwise

Statistics
----------

The TCP bandwidth limit filter outputs statistics in the ``<stat_prefix>.`` namespace.

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  read_enabled, Counter, Total number of times the read limit was applied to incoming data
  write_enabled, Counter, Total number of times the write limit was applied to outgoing data
  read_throttled, Counter, Total number of times read data was throttled
  write_throttled, Counter, Total number of times write data was throttled
  read_total_bytes, Counter, Total bytes read
  write_total_bytes, Counter, Total bytes written
  read_bytes_buffered, Gauge, Current number of bytes buffered for read
  write_bytes_buffered, Gauge, Current number of bytes buffered for write
  read_rate_bps, Gauge, Current read rate in bytes per second
  write_rate_bps, Gauge, Current write rate in bytes per second

Runtime
-------

The TCP bandwidth limit filter can be runtime feature flagged via the :ref:`runtime_enabled
<envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit.runtime_enabled>`
configuration field.
