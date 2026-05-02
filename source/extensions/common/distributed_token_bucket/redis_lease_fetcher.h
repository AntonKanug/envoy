#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "envoy/event/dispatcher.h"
#include "envoy/network/connection.h"
#include "envoy/upstream/cluster_manager.h"

#include "source/common/common/logger.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher_factory.h"
#include "source/extensions/filters/network/common/redis/client.h"
#include "source/extensions/filters/network/common/redis/codec.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

// LeaseFetcher implementation backed by an Envoy upstream cluster of type
// `redis`. Issues a Lua-scripted EVAL command that atomically advances a
// per-key token bucket inside Redis and returns the granted token count.
//
// Threading: bound to a single dispatcher (typically the main thread) at
// construction. ``fetchLease`` may be called from any worker thread; it posts
// a task to the bound dispatcher and the Redis client is only ever touched
// from there. Responses fire on the same dispatcher and invoke the caller's
// callback synchronously from there.
//
// Concurrency: only one in-flight Redis request per fetcher. If a second
// ``fetchLease`` arrives while one is in flight the new caller is failed
// immediately with `granted=0, success=false`. Callers (i.e.
// ``DistributedTokenBucket``) are expected to coalesce.
class RedisLeaseFetcher
    : public LeaseFetcher,
      public Envoy::Extensions::NetworkFilters::Common::Redis::Client::ClientCallbacks,
      public Network::ConnectionCallbacks,
      public Logger::Loggable<Logger::Id::filter> {
public:
  // Build a fetcher. Returns InvalidArgument if cluster/key are empty, or
  // FailedPrecondition if the cluster is not configured.
  static absl::StatusOr<LeaseFetcherPtr> create(const LeaseFetcherFactoryContext& ctx,
                                                const std::string& cluster_name,
                                                const std::string& key);

  // Test-only constructor allowing the underlying Redis client factory to be
  // overridden (e.g. with a MockClientFactory). Skips the cluster-presence
  // check so unit tests can run without a live ClusterManager configuration.
  static absl::StatusOr<LeaseFetcherPtr>
  createForTest(const LeaseFetcherFactoryContext& ctx, const std::string& cluster_name,
                const std::string& key,
                Envoy::Extensions::NetworkFilters::Common::Redis::Client::ClientFactory& factory);

  // Install RedisLeaseFetcher::create as the default factory in
  // LeaseFetcherFactoryRegistry. Idempotent: safe to call from each filter
  // factory entry point. Existing factories (e.g. registered by tests) are
  // not overwritten.
  static void registerAsDefaultFactory();

  ~RedisLeaseFetcher() override;

  // LeaseFetcher
  void fetchLease(uint64_t requested, Callback cb) override;

  // ClientCallbacks
  void onResponse(Envoy::Extensions::NetworkFilters::Common::Redis::RespValuePtr&& value) override;
  void onFailure() override;
  void onRedirection(Envoy::Extensions::NetworkFilters::Common::Redis::RespValuePtr&&,
                     const std::string&, bool) override {}

  // Network::ConnectionCallbacks
  void onEvent(Network::ConnectionEvent event) override;
  void onAboveWriteBufferHighWatermark() override {}
  void onBelowWriteBufferLowWatermark() override {}

private:
  RedisLeaseFetcher(LeaseFetcherFactoryContext ctx, std::string cluster_name, std::string key,
                    Envoy::Extensions::NetworkFilters::Common::Redis::Client::ClientFactory& factory);

  // Always invoked on dispatcher_'s thread.
  void doFetch(uint64_t requested, Callback cb);
  void completeAndReset(uint64_t granted, bool success);
  void closeClient();
  bool ensureClient();

  // Build the EVAL request. KEYS[1]=key, ARGV={rate, requested, now_ms}.
  Envoy::Extensions::NetworkFilters::Common::Redis::RespValue
  buildEvalRequest(uint64_t requested, uint64_t now_ms) const;

  // Minimal RedisConfig impl for the underlying ClientFactory.
  class RedisConfig
      : public Envoy::Extensions::NetworkFilters::Common::Redis::Client::Config {
  public:
    explicit RedisConfig(std::chrono::milliseconds op_timeout) : op_timeout_(op_timeout) {}
    std::chrono::milliseconds opTimeout() const override { return op_timeout_; }
    bool disableOutlierEvents() const override { return false; }
    bool enableHashtagging() const override { return false; }
    bool enableRedirection() const override { return true; }
    uint32_t maxBufferSizeBeforeFlush() const override { return 0; }
    std::chrono::milliseconds bufferFlushTimeoutInMs() const override {
      return std::chrono::milliseconds(0);
    }
    uint32_t maxUpstreamUnknownConnections() const override { return 0; }
    bool enableCommandStats() const override { return false; }
    Envoy::Extensions::NetworkFilters::Common::Redis::Client::ReadPolicy readPolicy() const override {
      return Envoy::Extensions::NetworkFilters::Common::Redis::Client::ReadPolicy::Primary;
    }
    bool connectionRateLimitEnabled() const override { return false; }
    uint32_t connectionRateLimitPerSec() const override { return 0; }

  private:
    const std::chrono::milliseconds op_timeout_;
  };

  const std::string cluster_name_;
  const std::string key_;
  const uint64_t rate_tokens_per_sec_;
  Upstream::ClusterManager& cluster_manager_;
  Event::Dispatcher& dispatcher_;
  Stats::Scope& scope_;
  TimeSource& time_source_;
  Envoy::Extensions::NetworkFilters::Common::Redis::Client::ConfigSharedPtr config_;
  Envoy::Extensions::NetworkFilters::Common::Redis::Client::ClientFactory& client_factory_;
  Envoy::Extensions::NetworkFilters::Common::Redis::RedisCommandStatsSharedPtr command_stats_;

  // All state below is only accessed from `dispatcher_`'s thread.
  Envoy::Extensions::NetworkFilters::Common::Redis::Client::ClientPtr client_;
  Envoy::Extensions::NetworkFilters::Common::Redis::Client::PoolRequest* current_request_{nullptr};
  Callback current_callback_;

  // Marked false in dtor; lambdas posted to dispatcher_ check this before
  // touching `this` so that destruction races (lifetime of fetcher vs. enqueued
  // dispatcher tasks) become safe no-ops.
  const std::shared_ptr<std::atomic<bool>> alive_{std::make_shared<std::atomic<bool>>(true)};
};

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
