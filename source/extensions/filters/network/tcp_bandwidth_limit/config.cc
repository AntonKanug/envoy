#include "source/extensions/filters/network/tcp_bandwidth_limit/config.h"

#include "envoy/extensions/filters/network/tcp_bandwidth_limit/v3/tcp_bandwidth_limit.pb.h"
#include "envoy/extensions/filters/network/tcp_bandwidth_limit/v3/tcp_bandwidth_limit.pb.validate.h"

#include "source/extensions/common/distributed_token_bucket/redis_lease_fetcher.h"
#include "source/extensions/filters/network/tcp_bandwidth_limit/tcp_bandwidth_limit.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace TcpBandwidthLimit {

Network::FilterFactoryCb TcpBandwidthLimitConfigFactory::createFilterFactoryFromProtoTyped(
    const envoy::extensions::filters::network::tcp_bandwidth_limit::v3::TcpBandwidthLimit&
        proto_config,
    Server::Configuration::FactoryContext& context) {

  // Install the Redis-backed LeaseFetcher factory exactly once (idempotent);
  // tests that need a different factory call registerFactory() before this and
  // their override is preserved.
  ::Envoy::Extensions::Common::DistributedTokenBucket::RedisLeaseFetcher::registerAsDefaultFactory();

  auto& server_ctx = context.serverFactoryContext();
  auto config = std::make_shared<FilterConfig>(
      proto_config, context.scope(), server_ctx.runtime(), server_ctx.timeSource(),
      server_ctx.clusterManager(), server_ctx.mainThreadDispatcher());

  return [config](Network::FilterManager& filter_manager) -> void {
    filter_manager.addFilter(std::make_shared<TcpBandwidthLimitFilter>(config));
  };
}

REGISTER_FACTORY(TcpBandwidthLimitConfigFactory,
                 Server::Configuration::NamedNetworkFilterConfigFactory);

} // namespace TcpBandwidthLimit
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
