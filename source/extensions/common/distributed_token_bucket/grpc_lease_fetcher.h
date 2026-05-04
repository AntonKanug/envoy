#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "envoy/event/dispatcher.h"
#include "envoy/extensions/distributed_token_bucket/v3/quota.pb.h"
#include "envoy/grpc/async_client.h"
#include "envoy/tracing/tracer.h"

#include "source/common/common/logger.h"
#include "source/common/grpc/typed_async_client.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

// LeaseFetcher backed by a unary gRPC call to bandwidth.v1.BandwidthQuotaService
// (the standalone bandwidth-quota Go service). Uses Envoy's standard gRPC
// async-client infrastructure, so cluster pooling, mTLS, retries and outlier
// detection come for free from the cluster the GrpcService config points at.
//
// Threading: the underlying RawAsyncClient is bound to a single dispatcher.
// `fetchLease` may be called from any worker; it posts onto the bound
// dispatcher and the gRPC call (and its callback) execute there. The bucket's
// callback is invoked from the dispatcher thread; the bucket itself takes its
// own mutex on the consume path so re-entry is safe.
//
// Concurrency: only one in-flight gRPC call per fetcher. If a second
// fetchLease arrives before the first completes, the second is rejected
// immediately (granted=0, success=false). The caller (DistributedTokenBucket)
// already coalesces via its `refill_in_flight_` flag.
class GrpcLeaseFetcher
    : public LeaseFetcher,
      public Grpc::AsyncRequestCallbacks<
          envoy::extensions::distributed_token_bucket::v3::AcquireLeaseResponse>,
      public Logger::Loggable<Logger::Id::filter> {
public:
  GrpcLeaseFetcher(Grpc::RawAsyncClientSharedPtr grpc_client,
                   std::chrono::milliseconds timeout, std::string key,
                   uint64_t rate_tokens_per_sec, Event::Dispatcher& dispatcher);
  ~GrpcLeaseFetcher() override;

  // LeaseFetcher
  void fetchLease(uint64_t requested, Callback cb) override;

  // Grpc::AsyncRequestCallbacks
  void onCreateInitialMetadata(Http::RequestHeaderMap&) override {}
  void onSuccess(
      std::unique_ptr<envoy::extensions::distributed_token_bucket::v3::AcquireLeaseResponse>&&
          response,
      Tracing::Span&) override;
  void onFailure(Grpc::Status::GrpcStatus status, const std::string& message,
                 Tracing::Span&) override;

private:
  // Dispatcher-thread-only.
  void doFetch(uint64_t requested, Callback cb);
  void completeAndReset(uint64_t granted, bool success);

  Grpc::AsyncClient<envoy::extensions::distributed_token_bucket::v3::AcquireLeaseRequest,
                    envoy::extensions::distributed_token_bucket::v3::AcquireLeaseResponse>
      async_client_;
  const Protobuf::MethodDescriptor& service_method_;
  const std::chrono::milliseconds timeout_;
  const std::string key_;
  const uint64_t rate_tokens_per_sec_;
  Event::Dispatcher& dispatcher_;

  // Dispatcher-thread state.
  Grpc::AsyncRequest* request_{nullptr};
  Callback current_callback_;

  // Lifetime guard: lambdas posted to the dispatcher capture this and bail
  // out if the fetcher has been destroyed before they run.
  const std::shared_ptr<std::atomic<bool>> alive_{std::make_shared<std::atomic<bool>>(true)};
};

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
