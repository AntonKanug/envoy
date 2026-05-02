#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher_factory.h"
#include "source/extensions/common/distributed_token_bucket/redis_lease_fetcher.h"
#include "source/extensions/filters/network/common/redis/client.h"

#include "test/extensions/filters/network/common/redis/mocks.h"
#include "test/mocks/event/mocks.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/upstream/cluster_manager.h"
#include "test/mocks/upstream/load_balancer.h"
#include "test/mocks/upstream/thread_local_cluster.h"
#include "test/test_common/simulated_time_system.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {
namespace {

namespace RedisClient = Envoy::Extensions::NetworkFilters::Common::Redis::Client;
namespace RedisCommon = Envoy::Extensions::NetworkFilters::Common::Redis;

// Minimal ClientFactory that hands back a caller-supplied MockClient. The
// MockClient owned by the test fixture is returned exactly once; subsequent
// calls return nullptr.
class FakeClientFactory : public RedisClient::ClientFactory {
public:
  RedisClient::ClientPtr create(Upstream::HostConstSharedPtr, Event::Dispatcher&,
                                const RedisClient::ConfigSharedPtr&,
                                const RedisCommon::RedisCommandStatsSharedPtr&, Stats::Scope&,
                                const std::string&, const std::string&, bool,
                                absl::optional<envoy::extensions::filters::network::redis_proxy::
                                                   v3::AwsIam>,
                                absl::optional<RedisCommon::AwsIamAuthenticator::
                                                   AwsIamAuthenticatorSharedPtr>) override {
    if (!next_client_) {
      return nullptr;
    }
    create_count_++;
    return std::move(next_client_);
  }

  // Hand a client to be returned on the next `create()` call.
  void setNext(RedisClient::ClientPtr client) { next_client_ = std::move(client); }
  int createCount() const { return create_count_; }

private:
  RedisClient::ClientPtr next_client_;
  int create_count_{0};
};

class RedisLeaseFetcherTest : public ::testing::Test {
protected:
  RedisLeaseFetcherTest() {
    // Make MockDispatcher.post() execute the lambda inline so tests are
    // synchronous. We reset this expectation per test as needed.
    ON_CALL(dispatcher_, post(_)).WillByDefault(Invoke([](Event::PostCb cb) { cb(); }));
    ON_CALL(dispatcher_, isThreadSafe()).WillByDefault(Return(true));
    // By default route any cluster lookup back to the fixture's TLC mock.
    ON_CALL(cluster_manager_, getThreadLocalCluster(_))
        .WillByDefault(Return(&cluster_manager_.thread_local_cluster_));
    ON_CALL(cluster_manager_.thread_local_cluster_, loadBalancer())
        .WillByDefault(::testing::ReturnRef(load_balancer_));
  }

  LeaseFetcherFactoryContext makeContext() {
    return LeaseFetcherFactoryContext{cluster_manager_, dispatcher_, *scope_.rootScope(),
                                      time_system_,     /*rate_tokens_per_sec=*/1024 * 1024,
                                      std::chrono::seconds(1)};
  }

  // Build a fresh MockClient whose makeRequest() returns the given PoolRequest
  // handle and records the ClientCallbacks for later response injection.
  std::unique_ptr<RedisCommon::Client::MockClient>
  buildMockClient(RedisClient::PoolRequest* request) {
    auto client = std::make_unique<::testing::NiceMock<RedisCommon::Client::MockClient>>();
    EXPECT_CALL(*client, makeRequest_(_, _)).WillRepeatedly(Return(request));
    EXPECT_CALL(*client, addConnectionCallbacks(_));
    EXPECT_CALL(*client, active()).WillRepeatedly(Return(true));
    EXPECT_CALL(*client, close()).Times(::testing::AnyNumber());
    return client;
  }

  // Inject a successful Integer reply into the most recently registered client
  // callbacks.
  void respondInt(RedisCommon::Client::MockClient& client, int64_t value) {
    auto reply = std::make_unique<RedisCommon::RespValue>();
    reply->type(RedisCommon::RespType::Integer);
    reply->asInteger() = value;
    ASSERT_FALSE(client.client_callbacks_.empty());
    client.client_callbacks_.front()->onResponse(std::move(reply));
  }

  void respondError(RedisCommon::Client::MockClient& client, const std::string& msg) {
    auto reply = std::make_unique<RedisCommon::RespValue>();
    reply->type(RedisCommon::RespType::Error);
    reply->asString() = msg;
    ASSERT_FALSE(client.client_callbacks_.empty());
    client.client_callbacks_.front()->onResponse(std::move(reply));
  }

  void respondNetworkFailure(RedisCommon::Client::MockClient& client) {
    ASSERT_FALSE(client.client_callbacks_.empty());
    client.client_callbacks_.front()->onFailure();
  }

  ::testing::NiceMock<Upstream::MockClusterManager> cluster_manager_;
  ::testing::NiceMock<Event::MockDispatcher> dispatcher_;
  ::testing::NiceMock<Stats::MockIsolatedStatsStore> scope_;
  Event::SimulatedTimeSystem time_system_;
  ::testing::NiceMock<Upstream::MockLoadBalancer> load_balancer_;
  FakeClientFactory factory_;
};

TEST_F(RedisLeaseFetcherTest, CreateRejectsEmptyClusterName) {
  auto ctx = makeContext();
  auto result = RedisLeaseFetcher::createForTest(ctx, "", "key", factory_);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(absl::StatusCode::kInvalidArgument, result.status().code());
}

TEST_F(RedisLeaseFetcherTest, CreateRejectsEmptyKey) {
  auto ctx = makeContext();
  auto result = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "", factory_);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(absl::StatusCode::kInvalidArgument, result.status().code());
}

TEST_F(RedisLeaseFetcherTest, FetchSuccessReturnsGrantedTokens) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);

  ::testing::NiceMock<RedisCommon::Client::MockPoolRequest> pool_request;
  auto mock_client = buildMockClient(&pool_request);
  RedisCommon::Client::MockClient& client_ref = *mock_client;
  factory_.setNext(std::move(mock_client));

  uint64_t granted = 0;
  bool ok = false;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });

  ASSERT_EQ(1, factory_.createCount());
  respondInt(client_ref, 4096);

  EXPECT_TRUE(ok);
  EXPECT_EQ(4096, granted);
}

TEST_F(RedisLeaseFetcherTest, RedisErrorReplyIsFailure) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);

  ::testing::NiceMock<RedisCommon::Client::MockPoolRequest> pool_request;
  auto mock_client = buildMockClient(&pool_request);
  RedisCommon::Client::MockClient& client_ref = *mock_client;
  factory_.setNext(std::move(mock_client));

  uint64_t granted = 99;
  bool ok = true;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });

  respondError(client_ref, "NOSCRIPT No matching script");
  EXPECT_FALSE(ok);
  EXPECT_EQ(0, granted);
}

TEST_F(RedisLeaseFetcherTest, NetworkFailureIsReportedAndClientReset) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);

  ::testing::NiceMock<RedisCommon::Client::MockPoolRequest> pool_request;
  EXPECT_CALL(pool_request, cancel()).Times(::testing::AnyNumber());
  auto mock_client = buildMockClient(&pool_request);
  RedisCommon::Client::MockClient& client_ref = *mock_client;
  factory_.setNext(std::move(mock_client));

  uint64_t granted = 99;
  bool ok = true;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });
  respondNetworkFailure(client_ref);

  EXPECT_FALSE(ok);
  EXPECT_EQ(0, granted);

  // The next fetchLease should ask the factory for a fresh client because the
  // previous one was dropped on failure.
  ::testing::NiceMock<RedisCommon::Client::MockPoolRequest> pool_request2;
  EXPECT_CALL(pool_request2, cancel()).Times(::testing::AnyNumber());
  auto mock_client2 = buildMockClient(&pool_request2);
  RedisCommon::Client::MockClient& client_ref2 = *mock_client2;
  factory_.setNext(std::move(mock_client2));

  uint64_t granted2 = 0;
  bool ok2 = false;
  fetcher->fetchLease(4096, [&](uint64_t g, bool s) {
    granted2 = g;
    ok2 = s;
  });
  EXPECT_EQ(2, factory_.createCount());

  // Drain the second request so the fetcher dtor has nothing to cancel.
  respondInt(client_ref2, 4096);
  EXPECT_TRUE(ok2);
  EXPECT_EQ(4096, granted2);
}

TEST_F(RedisLeaseFetcherTest, ConcurrentInFlightSecondCallFailsImmediately) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);

  ::testing::NiceMock<RedisCommon::Client::MockPoolRequest> pool_request;
  EXPECT_CALL(pool_request, cancel()).Times(::testing::AnyNumber());
  auto mock_client = buildMockClient(&pool_request);
  RedisCommon::Client::MockClient& client_ref = *mock_client;
  factory_.setNext(std::move(mock_client));

  bool first_done = false;
  fetcher->fetchLease(8192, [&](uint64_t, bool) { first_done = true; });

  bool second_ok = true;
  uint64_t second_granted = 99;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    second_ok = s;
    second_granted = g;
  });
  EXPECT_FALSE(second_ok);
  EXPECT_EQ(0, second_granted);
  EXPECT_FALSE(first_done); // first is still pending

  // Drain the first request so the fetcher dtor has nothing to cancel.
  respondInt(client_ref, 0);
  EXPECT_TRUE(first_done);
}

TEST_F(RedisLeaseFetcherTest, NoHostAvailableIsFailure) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);
  // No client set on factory; it returns nullptr.

  bool ok = true;
  uint64_t granted = 99;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });
  EXPECT_FALSE(ok);
  EXPECT_EQ(0, granted);
}

TEST_F(RedisLeaseFetcherTest, DestructionCancelsInFlightRequest) {
  auto ctx = makeContext();
  auto fetcher_or = RedisLeaseFetcher::createForTest(ctx, "redis_cluster", "tenant.acme", factory_);
  ASSERT_TRUE(fetcher_or.ok());
  auto fetcher = std::move(*fetcher_or);

  RedisCommon::Client::MockPoolRequest pool_request;
  EXPECT_CALL(pool_request, cancel());
  auto mock_client = buildMockClient(&pool_request);
  factory_.setNext(std::move(mock_client));

  fetcher->fetchLease(8192, [](uint64_t, bool) {});

  // Destroying the fetcher should cancel the pending pool request.
  fetcher.reset();
}

} // namespace
} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
