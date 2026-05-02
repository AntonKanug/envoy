#include "source/extensions/common/distributed_token_bucket/lease_fetcher_factory.h"

#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

namespace {

absl::Mutex& factoryMutex() {
  static absl::Mutex* mutex = new absl::Mutex();
  return *mutex;
}

LeaseFetcherFactoryFn& factoryRef() ABSL_EXCLUSIVE_LOCKS_REQUIRED(factoryMutex()) {
  static auto* fn = new LeaseFetcherFactoryFn();
  return *fn;
}

} // namespace

void LeaseFetcherFactoryRegistry::registerFactory(LeaseFetcherFactoryFn factory) {
  absl::MutexLock lock(&factoryMutex());
  factoryRef() = std::move(factory);
}

absl::StatusOr<LeaseFetcherPtr>
LeaseFetcherFactoryRegistry::create(const LeaseFetcherFactoryContext& ctx,
                                    const std::string& cluster, const std::string& key) {
  LeaseFetcherFactoryFn factory_copy;
  {
    absl::MutexLock lock(&factoryMutex());
    factory_copy = factoryRef();
  }
  if (!factory_copy) {
    return absl::FailedPreconditionError(
        "no LeaseFetcher factory registered; distributed bandwidth limit cannot be used until a "
        "factory is installed via LeaseFetcherFactoryRegistry::registerFactory");
  }
  return factory_copy(ctx, cluster, key);
}

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
