#include <chrono>
#include <memory>
#include <utility>

#include "envoy/extensions/distributed_token_bucket/v3/quota.pb.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/grpc/common.h"
#include "source/extensions/common/distributed_token_bucket/grpc_lease_fetcher.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"

#include "test/mocks/event/mocks.h"
#include "test/mocks/grpc/mocks.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {
namespace {

namespace QuotaProto = envoy::extensions::distributed_token_bucket::v3;

class GrpcLeaseFetcherTest : public ::testing::Test {
protected:
  GrpcLeaseFetcherTest() {
    // Run dispatcher.post() callbacks inline.
    ON_CALL(dispatcher_, post(_)).WillByDefault(Invoke([](Event::PostCb cb) { cb(); }));
    ON_CALL(dispatcher_, isThreadSafe()).WillByDefault(Return(true));
  }

  std::unique_ptr<GrpcLeaseFetcher> makeFetcher() {
    auto raw_client = std::make_shared<NiceMock<Grpc::MockAsyncClient>>();
    raw_client_ = raw_client.get();
    return std::make_unique<GrpcLeaseFetcher>(std::move(raw_client),
                                              std::chrono::milliseconds(500), "tenant.acme:read",
                                              /*rate=*/1024 * 1024, dispatcher_);
  }

  // Build a buffer that, when parsed by the gRPC infrastructure, yields the
  // given AcquireLeaseResponse.
  Buffer::InstancePtr makeResponseBuffer(uint64_t granted_tokens, uint64_t retry_after_ms = 0) {
    QuotaProto::AcquireLeaseResponse resp;
    resp.set_granted_tokens(granted_tokens);
    resp.set_retry_after_ms(retry_after_ms);
    return Grpc::Common::serializeMessage(resp);
  }

  // The typed AsyncClient wrapper inserts itself between sendRaw and the
  // user's onSuccess/onFailure. Capture the wrapper that gets handed to
  // sendRaw so tests can drive it directly.
  Grpc::RawAsyncRequestCallbacks* captureCallbacks(NiceMock<Grpc::MockAsyncRequest>& req) {
    Grpc::RawAsyncRequestCallbacks* captured = nullptr;
    EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _))
        .WillOnce(Invoke(
            [&captured, &req](absl::string_view, absl::string_view, Buffer::InstancePtr&&,
                              Grpc::RawAsyncRequestCallbacks& callbacks, Tracing::Span&,
                              const Http::AsyncClient::RequestOptions&) -> Grpc::AsyncRequest* {
              captured = &callbacks;
              return &req;
            }));
    return captured;
  }

  NiceMock<Event::MockDispatcher> dispatcher_;
  Grpc::MockAsyncClient* raw_client_{nullptr};
};

TEST_F(GrpcLeaseFetcherTest, FetchSuccessReturnsGrantedTokens) {
  auto fetcher = makeFetcher();

  NiceMock<Grpc::MockAsyncRequest> request;
  Grpc::RawAsyncRequestCallbacks* captured = nullptr;
  EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _))
      .WillOnce(Invoke(
          [&captured, &request](absl::string_view, absl::string_view, Buffer::InstancePtr&&,
                                Grpc::RawAsyncRequestCallbacks& callbacks, Tracing::Span&,
                                const Http::AsyncClient::RequestOptions&) -> Grpc::AsyncRequest* {
            captured = &callbacks;
            return &request;
          }));

  uint64_t granted = 0;
  bool ok = false;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });

  ASSERT_NE(captured, nullptr);
  // Hand the typed wrapper a serialized success response.
  Tracing::NullSpan& span = Tracing::NullSpan::instance();
  captured->onSuccessRaw(makeResponseBuffer(4096), span);

  EXPECT_TRUE(ok);
  EXPECT_EQ(4096, granted);
}

TEST_F(GrpcLeaseFetcherTest, FetchFailureIsReported) {
  auto fetcher = makeFetcher();

  NiceMock<Grpc::MockAsyncRequest> request;
  Grpc::RawAsyncRequestCallbacks* captured = nullptr;
  EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _))
      .WillOnce(Invoke(
          [&captured, &request](absl::string_view, absl::string_view, Buffer::InstancePtr&&,
                                Grpc::RawAsyncRequestCallbacks& callbacks, Tracing::Span&,
                                const Http::AsyncClient::RequestOptions&) -> Grpc::AsyncRequest* {
            captured = &callbacks;
            return &request;
          }));

  uint64_t granted = 99;
  bool ok = true;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });

  ASSERT_NE(captured, nullptr);
  Tracing::NullSpan& span = Tracing::NullSpan::instance();
  captured->onFailure(Grpc::Status::WellKnownGrpcStatus::Unavailable, "service down", span);

  EXPECT_FALSE(ok);
  EXPECT_EQ(0, granted);
}

TEST_F(GrpcLeaseFetcherTest, ConcurrentFetchSecondRejectedImmediately) {
  // Declare the mock request *before* the fetcher so it outlives the fetcher
  // — the fetcher's destructor cancels in-flight requests, and we don't want
  // to call cancel() on a destroyed mock.
  NiceMock<Grpc::MockAsyncRequest> request;
  EXPECT_CALL(request, cancel()).Times(::testing::AnyNumber());

  auto fetcher = makeFetcher();
  EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _)).WillOnce(Return(&request));

  bool first_done = false;
  fetcher->fetchLease(8192, [&](uint64_t, bool) { first_done = true; });

  // Second call while first is in flight is rejected without issuing a new
  // gRPC request.
  bool second_ok = true;
  uint64_t second_granted = 99;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    second_ok = s;
    second_granted = g;
  });
  EXPECT_FALSE(second_ok);
  EXPECT_EQ(0, second_granted);
  EXPECT_FALSE(first_done);
}

TEST_F(GrpcLeaseFetcherTest, SendReturningNullIsTreatedAsFailure) {
  auto fetcher = makeFetcher();

  EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _)).WillOnce(Return(nullptr));

  uint64_t granted = 99;
  bool ok = true;
  fetcher->fetchLease(8192, [&](uint64_t g, bool s) {
    granted = g;
    ok = s;
  });

  EXPECT_FALSE(ok);
  EXPECT_EQ(0, granted);
}

TEST_F(GrpcLeaseFetcherTest, DestructionCancelsInFlightRequest) {
  auto fetcher = makeFetcher();

  NiceMock<Grpc::MockAsyncRequest> request;
  EXPECT_CALL(request, cancel());
  EXPECT_CALL(*raw_client_, sendRaw(_, _, _, _, _, _)).WillOnce(Return(&request));

  fetcher->fetchLease(8192, [](uint64_t, bool) {});
  fetcher.reset();
}

} // namespace
} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
