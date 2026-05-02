#pragma once

#include <functional>
#include <string>

#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"
#include "envoy/stats/scope.h"
#include "envoy/upstream/cluster_manager.h"

#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"

#include "absl/status/statusor.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

// Server-side context handed to the factory at fetcher-creation time. References
// must outlive the produced LeaseFetcher.
struct LeaseFetcherFactoryContext {
  Upstream::ClusterManager& cluster_manager;
  Event::Dispatcher& dispatcher; // Dispatcher that will own the Redis client.
  Stats::Scope& scope;
  TimeSource& time_source;
  // Per-bucket parameters.
  uint64_t rate_tokens_per_sec{};
  std::chrono::milliseconds op_timeout{std::chrono::seconds(1)};
};

// Factory function that builds a LeaseFetcher targeting a particular remote
// cluster + bucket key. Implementations are expected to validate that
// `cluster` resolves to a usable upstream and return an error otherwise.
using LeaseFetcherFactoryFn = std::function<absl::StatusOr<LeaseFetcherPtr>(
    const LeaseFetcherFactoryContext& ctx, const std::string& cluster, const std::string& key)>;

// Global hook used by the bandwidth filters to construct a remote-backed
// bucket. Production deployments register a real (e.g. Redis-backed) factory
// here at server init; tests register a fake. If unset, callers receive a
// FailedPrecondition error and should fall back to local-only mode.
class LeaseFetcherFactoryRegistry {
public:
  // Install the global factory. Pass an empty function to clear it.
  static void registerFactory(LeaseFetcherFactoryFn factory);

  // Build a fetcher. Returns FailedPrecondition if no factory is registered.
  static absl::StatusOr<LeaseFetcherPtr> create(const LeaseFetcherFactoryContext& ctx,
                                                const std::string& cluster,
                                                const std::string& key);
};

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
