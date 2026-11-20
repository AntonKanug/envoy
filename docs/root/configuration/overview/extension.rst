.. _config_overview_extension_configuration:

Extension configuration
-----------------------

Each configuration resource in Envoy has a type URL in the ``typed_config``. This
type corresponds to a versioned schema. The type URL uniquely identifies an
extension capable of interpreting the configuration. The ``name`` field is
optional and can be used as an identifier or as an annotation for the
particular instance of the extension configuration. For example, the following
filter configuration snippet is permitted:

.. literalinclude:: _include/extensions/without-type-url.yaml
   :language: yaml
   :lines: 14-33
   :linenos:
   :lineno-start: 14
   :caption: :download:`extensions-demo.yaml <_include/extensions/without-type-url.yaml>`

In case the control plane lacks the schema definitions for an extension,
``xds.type.v3.TypedStruct`` should be used as a generic container. The type URL
inside it is then used by a client to convert the contents to a typed
configuration resource. For example, the above example could be written as
follows:

.. literalinclude:: _include/extensions/with-type-url.yaml
   :language: yaml
   :lines: 14-37
   :linenos:
   :lineno-start: 14
   :caption: :download:`extensions-demo-with-type-url.yaml <_include/extensions/with-type-url.yaml>`

.. _config_overview_extension_discovery:

Discovery service
^^^^^^^^^^^^^^^^^

Extension configuration can be supplied dynamically from an :ref:`xDS
management server<xds_protocol>` using :ref:`ExtensionConfiguration discovery
service<envoy_v3_api_file_envoy/service/extension/v3/config_discovery.proto>`.
The name field in the extension configuration acts as the resource identifier.

TCP listener filters
^^^^^^^^^^^^^^^^^^^^

For TCP listener filters, the discovery service configuration is: :ref:`dynamic listener filter
re-configuration<envoy_v3_api_field_config.listener.v3.ListenerFilter.config_discovery>`.
If the dynamic configuration is missing, the connection will be rejected until a valid config is updated.

Network filters
^^^^^^^^^^^^^^^

For downstream network filters, the discovery service configuration is: :ref:`dynamic filter
re-configuration<envoy_v3_api_field_config.listener.v3.Filter.config_discovery>`.
If the dynamic configuration is missing, the connection will be rejected until a valid config is updated.
When a filter configuration updates, the new configuration will only apply to new connections, existing connections
will keep using the older filter configuration.

HTTP filters
^^^^^^^^^^^^

For HTTP filters, HTTP connection manager supports :ref:`dynamic filter
re-configuration<envoy_v3_api_field_extensions.filters.network.http_connection_manager.v3.HttpFilter.config_discovery>`.
If the configuration is missing a local HTTP response with '500' status code will be returned.

UDP listener filters
^^^^^^^^^^^^^^^^^^^^

Raw UDP listeners support ECDS through :ref:`listener filter
configuration discovery<envoy_v3_api_field_config.listener.v3.ListenerFilter.config_discovery>`.
An update replaces that filter's instances on each worker without rebinding the listening socket
or recreating other filters. In particular, a static DNS fallback filter retains its pending
queries during an update. Pending work owned by the replaced filter ends with that instance;
filters must cancel their asynchronous work when destroyed.

Missing required configuration drops datagrams before any filter in the chain runs and increments
``listener.<stat_prefix>.udp.extension_config_missing``. A configured default is used after initial
fetch failure or resource removal. Without a default, packets remain blocked until valid
configuration arrives. Updates rejected during main-thread validation retain the last accepted
configuration. Delta ECDS supports explicit resource removal; an empty state-of-the-world response
is rejected and retains the current configuration.

An ECDS ACK confirms main-thread validation and queuing of worker updates. It does not guarantee
that every worker has applied the update: workers can briefly use different versions. The
``extension_config_discovery.udp_listener_filter.<resource_name>.config_reload`` counter advances
after all workers have processed the update, including failed construction attempts. Worker filter
construction must succeed for the configuration to be usable: a construction failure drops subsequent datagrams on that worker and
increments ``listener.<stat_prefix>.udp.extension_config_update_failed``. Recovery requires a changed
typed configuration, removal followed by readdition, or listener recreation. Changing only the xDS
version or nonce does not retry construction because identical typed configurations are deduplicated.
Listener warming queues the initial configuration before worker listener creation, so initial
reload completion can precede filter construction. On initial fetch failure, listener warming
completes but required missing configuration continues to block packets.

UDP Session filters
^^^^^^^^^^^^^^^^^^^

For UDP session filters, UDP proxy supports :ref:`dynamic filter
re-configuration<envoy_v3_api_field_extensions.filters.udp.udp_proxy.v3.UdpProxyConfig.SessionFilter.config_discovery>`.
If the configuration is missing the UDP session will be removed

Statistics
^^^^^^^^^^

In addition to the :ref:`statistics<subscription_statistics>` supported for xDS subscriptions, the following statistics
are supported for listener filters, downstream network filters, and HTTP filters, rooted at *extension_config_discovery.<stat_prefix>.<extension_config_name>*.

- For UDP listener filters, the value of *<stat_prefix>* is *udp_listener_filter*.
- For TCP listener filters, the value of *<stat_prefix>* is *tcp_listener_filter*.
- For downstream network filters, the value of *<stat_prefix>* is *network_filter*.
- For upstream network filters, the value of *<stat_prefix>* is *upstream_network_filter*.
- For downstream HTTP filters, the value of *<stat_prefix>* is *http_filter*.
- For upstream HTTP filters, the value of *<stat_prefix>* is *upstream_http_filter*.
- For UDP session filters the value of *<stat_prefix>* is *udp_session_filter*.

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  config_reload, Counter, Total number of accepted configuration updates processed by all workers; this does not guarantee that UDP listener filter construction succeeded
  config_fail, Counter, Total number of failed configuration updates
  config_conflict, Counter, Total number of conflicting applications of configuration updates; this may happen when a new listener cannot reuse a subscribed extension configuration due to an invalid type URL.

Additionally, the following statistics are supported to indicate that a connection was closed due to a missing
configuration, rooted at listener.<address> (or listener.<stat_prefix>. if stat_prefix is non-empty).

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  extension_config_missing, Counter, Total connections closed due to missing listener filter extension configuration
  network_extension_config_missing, Counter, Total connections closed due to missing network filter extension configuration
