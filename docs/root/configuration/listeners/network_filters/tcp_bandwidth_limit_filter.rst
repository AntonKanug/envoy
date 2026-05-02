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

By default each Envoy enforces ``read_limit_kbps`` / ``write_limit_kbps`` independently
in-process: with ``N`` Envoys you get ``N`` × the limit aggregate. To share a single
limit across all instances, set the
:ref:`distributed <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit.distributed>`
field. The filter then enforces the limit via a leased token bucket backed by a
Redis-typed upstream cluster shared across the fleet.

How it works:

* Each Envoy keeps a small **local lease** of tokens (default 64 KiB, configurable
  via :ref:`lease_kb <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.lease_kb>`).
  Reads/writes draw from this lease synchronously.
* When the local lease drops below a threshold an asynchronous ``EVAL`` is dispatched
  against the configured Redis cluster to atomically advance a global counter for
  the bucket :ref:`key <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.key>`.
* Two Envoys configured with the same ``key`` against the same Redis share one
  bucket; ``read_limit_kbps`` / ``write_limit_kbps`` are interpreted as the
  *global* rate.
* If the Redis cluster is unreachable and
  :ref:`fail_open <envoy_v3_api_field_extensions.filters.network.tcp_bandwidth_limit.v3.DistributedBandwidthLimit.fail_open>`
  is ``true`` (the default), each Envoy falls back to the per-instance rate
  defined by ``read_limit_kbps`` / ``write_limit_kbps`` until Redis recovers.
  If ``fail_open`` is ``false`` traffic is buffered / backpressured.

The bucket is approximate: with ``N`` instances and lease size ``L`` the total
delivery may exceed the global limit by up to ``N`` × ``L`` during refill bursts.
Smaller leases narrow the window at the cost of higher Redis QPS.

Example: 1 MiB/s shared across all Envoys keyed by tenant:

.. code-block:: yaml

  name: envoy.filters.network.tcp_bandwidth_limit
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_bandwidth_limit.v3.TcpBandwidthLimit
    stat_prefix: bandwidth_limiter
    read_limit_kbps: 1024   # 1 MiB/s GLOBAL
    write_limit_kbps: 1024
    distributed:
      cluster: redis_bucket_cluster
      key: tenant.acme.bandwidth
      lease_kb: 64
      fail_open: true

The ``cluster`` must be an Envoy upstream cluster reachable from the main thread
dispatcher; standard Redis cluster types (``STATIC``, ``STRICT_DNS``, ``LOGICAL_DNS``)
all work. TLS, AWS IAM, and other auth modes follow the cluster's transport socket
configuration.

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
