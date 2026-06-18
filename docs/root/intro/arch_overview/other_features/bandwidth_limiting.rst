.. _arch_overview_bandwidth_limit:

Bandwidth limiting
===================

Envoy supports local (non-distributed) bandwidth limiting of HTTP requests and response via the
:ref:`HTTP bandwidth limit filter <config_http_filters_bandwidth_limit>`. This can be activated
globally at the listener level or at a more specific level (e.g.: the virtual host or route level).

For raw TCP traffic, the :ref:`TCP bandwidth limit filter <config_network_filters_tcp_bandwidth_limit>`
provides equivalent functionality at the network filter layer. It supports a
:ref:`distributed mode <config_network_filters_tcp_bandwidth_limit>` that shares a single bandwidth
limit across multiple Envoy instances via a Redis-backed leased token bucket.

