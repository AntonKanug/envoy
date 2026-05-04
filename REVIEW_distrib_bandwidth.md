# Review: `tcp_bandwidth: distributed limits` (`bf96336bd3`)

## Summary

In-tree distributed bandwidth limit implemented as a generic `TokenBucket`
implementation backed by a Redis-resident leased counter, plumbed into both
the HTTP `bandwidth_limit` and the network `tcp_bandwidth_limit` filters.
Uses Envoy's native `Common::Redis::Client` over a regular upstream cluster
(no HTTP adapter, no sidecar, no plugin runtime).

The shape is right and the seam is cleanly chosen. There are four substantive
bugs that need fixing before merge, and a handful of design issues that are
worth resolving now while the proto is still pre-stable.

## Approve / changes-requested

**Changes requested** — primarily for items B1, B2, B3, B5 below.

---

## Bugs

### B1. Read and write buckets collide on a single Redis key

`source/extensions/filters/network/tcp_bandwidth_limit/tcp_bandwidth_limit.cc:30-65`

`makeBucket()` is called twice from `FilterConfig` (once for read, once for
write) with the same `proto.distributed()`. Both paths construct a fetcher
keyed on `dist.key()` unchanged, so when both directions are configured every
byte transferred deducts from one shared global bucket twice. Effective fleet
rate is silently halved.

**Fix:** either suffix the Redis key in `makeBucket()` with `:read`/`:write`,
or extend `DistributedBandwidthLimit` with explicit `read_key`/`write_key`
fields. The auto-suffix is less surprising; explicit fields are more flexible.
I'd default to auto-suffix and only add explicit fields if users ask.

### B2. Fail-open over-allows by N × global rate

`source/extensions/common/distributed_token_bucket/distributed_token_bucket.cc:97-105`
`DistributedTokenBucketConfig::fail_open_rate_tokens_per_sec` is wired to
`rate_bps` (the configured *global* rate) at the call site. When Redis is
unreachable, every Envoy independently allows the full global rate. A 50-Envoy
fleet at 100 Gbps global → 5 Tbps fail-open. The proto comment acknowledges
this, but it makes "fail open" much more dangerous than the name suggests.

**Fix:** at minimum, add a dedicated `fail_open_kbps` proto field (interpreted
as per-instance) so operators can size the fallback independently. A
`fleet_size_hint` would also work but is less honest about the operational
contract. Consider defaulting `fail_open` to **false** — the safer default for
distributed semantics.

### B3. Fail-open recovery wastes a round trip and stalls one consume

`source/extensions/common/distributed_token_bucket/distributed_token_bucket.cc:39-44`

In the fail-open branch, the retry path sets `kick_off_fetch=true` but never
sets `requested_lease`, so the retry asks Redis for **0** tokens. The Lua
script computes `granted = min(0, tokens) = 0`, the bucket exits fail-open and
drops `fail_open_bucket_`, sets `local_tokens_=0`, and the next consume()
returns 0 to the filter (a brief stall) before kicking off a real refill on
the *next* consume.

**Fix:** in the fail-open retry path, also set
`requested_lease = config_.lease_size`. Same code as the normal path; just
move the assignment.

### B5. Lua script trusts caller-supplied clock

`source/extensions/common/distributed_token_bucket/redis_lease_fetcher.cc:21-32`

`ARGV[3]=now_ms` is the **Envoy's** wall clock. Refill math is
`tokens + (now - ts) * rate / 1000`. With clock skew across Envoys, a slow
clock following a fast one yields a negative delta — capped to `rate` by the
`math.min`, which silently *resets* the bucket to capacity. The opposite
direction allows a small over-refill window every time clocks cross.

**Fix:** drop `ARGV[3]` and use `redis.call('TIME')` server-side. One
authoritative clock; no skew to reason about. Document the dependency on
Redis time being monotonic across replicas (it is for a single primary; if
you ever shard or replicate, revisit).

---

## Design issues

### D1. Lua `HMSET` is deprecated

Redis 4.0+ prefers `HSET key f v f v`. `HMSET` still works but is
deprecation-flagged. One-line change.

### D2. One Redis client per fetcher per filter

`redis_lease_fetcher.cc:131-141`

Each `RedisLeaseFetcher` calls `client_factory_.create(...)` and gets its own
TCP connection. With multiple bandwidth filters per Envoy (a stated case),
this is N × M connections to Redis where N = filters, M = Envoys. Should pool
one client per `(cluster, dispatcher)` tuple in a process-global registry,
keyed off `cluster_name` plus auth. Refcount on fetcher destruction.

This is the change with the highest leverage if "many filters per Envoy" is
the real workload.

### D3. All Redis I/O serialized on `mainThreadDispatcher()`

`source/extensions/filters/network/tcp_bandwidth_limit/config.cc:24`

`server_ctx.mainThreadDispatcher()` is the xDS path. Routing Redis traffic
through it means a slow Redis can delay control-plane updates. Use a
per-worker dispatcher (each filter instance can capture its connection's
worker dispatcher in `onNewConnection`), or a dedicated lease dispatcher
created in the cluster-manager bootstrap. The latter pairs naturally with the
client pooling in D2.

### D4. Single-slot global factory

`source/extensions/common/distributed_token_bucket/lease_fetcher_factory.cc:14-26`

`LeaseFetcherFactoryRegistry` holds **one** function. Tests racing
`registerFactory` against `registerAsDefaultFactory` is the obvious break (the
"register before construct" comment is fragile). It also forecloses ever
supporting multiple backends without ripping the registry out.

**Fix:** name-keyed map (`absl::flat_hash_map<std::string, FactoryFn>`), with
the proto naming the backend (`backend: "redis"`). Default to `"redis"` when
the field is unset to preserve current behavior.

### D5. Default `lease_kb=64` is far too small

`source/extensions/filters/network/tcp_bandwidth_limit/tcp_bandwidth_limit.cc:23`

At 1 Gbps that's ~0.5 ms of capacity; `low_watermark = lease/4 = 16 KiB`
triggers a refill roughly every 0.4 ms. You will DOS your own Redis at modest
rates.

**Fix:** auto-derive when `lease_kb` is unset:
`max(64, rate_kbps * fill_interval_ms / 1024)`, clamped to some sane upper
bound (say 16 MiB). Document the trade-off (lease size vs. burstiness vs.
Redis QPS) in the proto.

### D6. Coalescing-by-drop hides as failure

`redis_lease_fetcher.cc:117-120`

When a fetch is in flight, the next `fetchLease` invocation calls
`cb(0, false)` — which the caller interprets as a Redis failure. The
`DistributedTokenBucket` itself gates with `refill_in_flight_`, so this path
is unreachable from the bucket; it only fires for tests or other callers and
silently looks like an outage. Either drop the branch (and rely on the
caller's gating contract), or queue the callback. I'd drop it and document
the contract; queuing has subtle ownership issues with the dispatcher post.

### D7. No metrics

The HTTP filter's `stat_prefix` is plumbed in but the distributed bucket
emits nothing. Operators can't tell:

- whether a slowdown is local throttling or fail-open
- whether Redis is healthy
- how many tokens a key is consuming centrally

**Add (under `<prefix>.distributed_bandwidth_limit`):**

| stat | type | meaning |
|---|---|---|
| `lease_requests_total` | counter | one per `fetchLease` |
| `lease_failures_total` | counter | `success=false` callbacks |
| `lease_granted_bytes_total` | counter | sum of `granted` |
| `lease_rtt_ms` | histogram | request → callback |
| `fail_open_active` | gauge | 1 when `fail_open_active_` is set |
| `cluster_unavailable_total` | counter | `tlc == nullptr` |

### D8. `limit_kbps` semantics are overloaded

The proto comments say `limit_kbps` is *both* the "global rate" and the
"per-instance fail-open fallback". Two meanings, one field. After B2 splits
out `fail_open_kbps`, rename the existing field's documentation: in
distributed mode, `read_limit_kbps` is the fleet-wide rate, full stop.

### D9. `maybeReset` is a downward clamp, not a reset

`distributed_token_bucket.cc:142-150`

Comment hand-waves it as best-effort, but the `TokenBucket` interface
contract is to *set* the count. Implement properly (push the new value to
Redis as well, or at minimum `local_tokens_ = num_tokens`), or
`ENVOY_BUG_RETURN` if called. Silent semantic drift on an interface method is
worse than rejecting the call.

### D10. xDS-removable cluster

`redis_lease_fetcher.cc:53-55` checks `cluster_manager.clusters().hasCluster`
at create time, but the cluster can disappear via xDS later. Runtime
`getThreadLocalCluster` already handles this (returns nullptr → fail-open) —
fine, but emit `cluster_unavailable_total` (D7) so operators see it.

---

## Test coverage

Three test files were added (`distributed_token_bucket_test.cc`,
`lease_fetcher_factory_test.cc`, `redis_lease_fetcher_test.cc`) — I haven't
read them line-by-line in this pass. Specific cases I'd want covered:

- Read + write both configured, both share a key (B1) — fails today, should
  be a regression test after the fix.
- Fail-open recovery: kill the fetcher, verify fail-open engages, restore,
  verify a single consume sequence walks back to normal mode without losing
  traffic (B3).
- Multiple filter instances sharing a `(cluster, dispatcher)` produce one
  client (D2) — needs an integration-style test with a real Redis or a
  high-fidelity mock.
- Clock skew: drive two `fetchLease` calls with deliberately stale `now_ms`
  and assert behavior matches a `TIME`-driven reference (B5).
- xDS removes the cluster mid-flight; bucket enters fail-open; xDS adds it
  back; recovery works.

## What's good

- The `TokenBucket` seam is exactly the right place to plug this in;
  `SharedTokenBucketImpl` continues to work for non-distributed users with
  zero changes.
- `alive_` shared atomic on both `DistributedTokenBucket` and
  `RedisLeaseFetcher` makes late callbacks safely no-op. Good lifecycle
  hygiene.
- Lua script does the bucket math atomically — correct call vs. RMW from
  clients (modulo B5).
- Native Envoy Redis client over a regular cluster gives you connection
  pooling, health checks, TLS via SDS, outlier detection, circuit breakers
  for free, and matches operator muscle memory. Significantly better
  ergonomics than the HTTP-adapter path I'd previously sketched.
- Code is well-commented at the right granularity (the bucket's class doc,
  the Lua block, the alive-pointer mechanism). Comments justify *why*, not
  *what*.

## Suggested commit sequence for follow-ups

1. **B5 + D1**: switch Lua to `redis.call('TIME')` and `HSET`. Pure code
   change, no proto.
2. **B3**: one-line fix to the fail-open retry path. Add a regression test.
3. **B1**: auto-suffix read/write keys. Migration-safe (existing keys keep
   working only because no users have shipped this yet).
4. **B2 + D8**: add `fail_open_kbps`, deprecate the overload in proto
   comments. Default `fail_open` to `false`.
5. **D7**: metrics.
6. **D2 + D3**: client pooling + dispatcher choice. Largest change; do last,
   ideally on its own commit so it's easy to revert.
7. **D4**: name-keyed factory registry. Deferrable.
8. **D5, D9, D10**: cleanups, deferrable.
