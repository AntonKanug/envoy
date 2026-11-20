listener: added ECDS support for raw UDP listener filters. Updates replace worker filter instances
without recreating the listening socket or unaffected static filters, and missing required
configuration drops datagrams before filter processing.
