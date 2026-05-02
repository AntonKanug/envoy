#include <utility>

#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher_factory.h"

#include "test/mocks/event/mocks.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/upstream/cluster_manager.h"

#include "gtest/gtest.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {
namespace {

class StubFetcher : public LeaseFetcher {
public:
  void fetchLease(uint64_t, Callback cb) override { cb(0, false); }
};

class LeaseFetcherFactoryRegistryTest : public ::testing::Test {
protected:
  void TearDown() override {
    // Always leave the registry empty so other tests in the suite are not
    // affected by leftover state.
    LeaseFetcherFactoryRegistry::registerFactory({});
  }

  LeaseFetcherFactoryContext makeContext() {
    return LeaseFetcherFactoryContext{cluster_manager_, dispatcher_, *scope_.rootScope(),
                                      time_system_,     /*rate_tokens_per_sec=*/1024 * 1024,
                                      std::chrono::seconds(1)};
  }

  Upstream::MockClusterManager cluster_manager_;
  Event::MockDispatcher dispatcher_;
  Stats::MockIsolatedStatsStore scope_;
  Event::SimulatedTimeSystem time_system_;
};

TEST_F(LeaseFetcherFactoryRegistryTest, ReturnsErrorWhenNoFactoryRegistered) {
  auto ctx = makeContext();
  auto result = LeaseFetcherFactoryRegistry::create(ctx, "redis-cluster", "tenant-acme");
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(absl::StatusCode::kFailedPrecondition, result.status().code());
}

TEST_F(LeaseFetcherFactoryRegistryTest, RegisteredFactoryIsInvoked) {
  std::string seen_cluster;
  std::string seen_key;
  LeaseFetcherFactoryRegistry::registerFactory(
      [&](const LeaseFetcherFactoryContext&, const std::string& cluster,
          const std::string& key) -> absl::StatusOr<LeaseFetcherPtr> {
        seen_cluster = cluster;
        seen_key = key;
        return std::make_unique<StubFetcher>();
      });

  auto ctx = makeContext();
  auto result = LeaseFetcherFactoryRegistry::create(ctx, "redis-cluster", "tenant-acme");
  ASSERT_TRUE(result.ok());
  EXPECT_NE(nullptr, result->get());
  EXPECT_EQ("redis-cluster", seen_cluster);
  EXPECT_EQ("tenant-acme", seen_key);
}

TEST_F(LeaseFetcherFactoryRegistryTest, RegisteredFactoryCanReturnError) {
  LeaseFetcherFactoryRegistry::registerFactory(
      [](const LeaseFetcherFactoryContext&, const std::string& cluster,
         const std::string&) -> absl::StatusOr<LeaseFetcherPtr> {
        if (cluster.empty()) {
          return absl::InvalidArgumentError("cluster name required");
        }
        return std::make_unique<StubFetcher>();
      });

  auto ctx = makeContext();
  auto bad = LeaseFetcherFactoryRegistry::create(ctx, "", "key");
  EXPECT_FALSE(bad.ok());
  EXPECT_EQ(absl::StatusCode::kInvalidArgument, bad.status().code());

  auto good = LeaseFetcherFactoryRegistry::create(ctx, "cluster", "key");
  EXPECT_TRUE(good.ok());
}

TEST_F(LeaseFetcherFactoryRegistryTest, ClearingFactoryRestoresFailedPrecondition) {
  LeaseFetcherFactoryRegistry::registerFactory(
      [](const LeaseFetcherFactoryContext&, const std::string&,
         const std::string&) -> absl::StatusOr<LeaseFetcherPtr> {
        return std::make_unique<StubFetcher>();
      });
  auto ctx = makeContext();
  ASSERT_TRUE(LeaseFetcherFactoryRegistry::create(ctx, "c", "k").ok());

  LeaseFetcherFactoryRegistry::registerFactory({});
  auto result = LeaseFetcherFactoryRegistry::create(ctx, "c", "k");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(absl::StatusCode::kFailedPrecondition, result.status().code());
}

} // namespace
} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
