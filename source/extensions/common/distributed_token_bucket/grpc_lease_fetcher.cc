#include "source/extensions/common/distributed_token_bucket/grpc_lease_fetcher.h"

#include <utility>

#include "source/common/common/assert.h"
#include "source/common/tracing/null_span_impl.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

namespace {
const char* kAcquireLeaseMethod =
    "envoy.extensions.distributed_token_bucket.v3.BandwidthQuotaService.AcquireLease";
}

GrpcLeaseFetcher::GrpcLeaseFetcher(Grpc::RawAsyncClientSharedPtr grpc_client,
                                   std::chrono::milliseconds timeout, std::string key,
                                   uint64_t rate_tokens_per_sec, Event::Dispatcher& dispatcher)
    : async_client_(std::move(grpc_client)),
      service_method_(*Protobuf::DescriptorPool::generated_pool()->FindMethodByName(
          kAcquireLeaseMethod)),
      timeout_(timeout), key_(std::move(key)), rate_tokens_per_sec_(rate_tokens_per_sec),
      dispatcher_(dispatcher) {}

GrpcLeaseFetcher::~GrpcLeaseFetcher() {
  alive_->store(false);
  // The underlying AsyncClient is dispatcher-thread-only; this destructor
  // must run on the dispatcher thread or the fetcher's caller must arrange
  // for cancellation prior to teardown. Cancel any in-flight request so its
  // callback doesn't fire after we're gone.
  if (request_ != nullptr) {
    request_->cancel();
    request_ = nullptr;
  }
}

void GrpcLeaseFetcher::fetchLease(uint64_t requested, Callback cb) {
  // Marshal onto the bound dispatcher. The gRPC client is not thread-safe.
  auto alive = alive_;
  dispatcher_.post([this, alive, requested, cb = std::move(cb)]() mutable {
    if (!alive->load(std::memory_order_acquire)) {
      cb(0, false);
      return;
    }
    doFetch(requested, std::move(cb));
  });
}

void GrpcLeaseFetcher::doFetch(uint64_t requested, Callback cb) {
  ASSERT(dispatcher_.isThreadSafe());

  if (current_callback_) {
    // Caller didn't coalesce; reject the duplicate immediately so we don't
    // grow an unbounded queue.
    cb(0, false);
    return;
  }

  envoy::extensions::distributed_token_bucket::v3::AcquireLeaseRequest request;
  request.set_key(key_);
  request.set_requested_tokens(requested);
  request.set_rate_tokens_per_sec(rate_tokens_per_sec_);

  current_callback_ = std::move(cb);
  Http::AsyncClient::RequestOptions options;
  options.setTimeout(timeout_);

  request_ = async_client_->send(service_method_, request, *this,
                                 Tracing::NullSpan::instance(), options);
  if (request_ == nullptr) {
    // send() may have invoked onFailure inline (in which case current_callback_
    // is already cleared) or failed silently (in which case we clear it here).
    if (current_callback_) {
      ENVOY_LOG(debug, "grpc_lease_fetcher: send() returned null without invoking onFailure");
      completeAndReset(0, false);
    }
  }
}

void GrpcLeaseFetcher::onSuccess(
    std::unique_ptr<envoy::extensions::distributed_token_bucket::v3::AcquireLeaseResponse>&&
        response,
    Tracing::Span&) {
  request_ = nullptr;
  const uint64_t granted = response ? response->granted_tokens() : 0;
  completeAndReset(granted, true);
}

void GrpcLeaseFetcher::onFailure(Grpc::Status::GrpcStatus status, const std::string& message,
                                 Tracing::Span&) {
  request_ = nullptr;
  ENVOY_LOG(debug, "grpc_lease_fetcher: AcquireLease failed status={} message={}",
            static_cast<int>(status), message);
  completeAndReset(0, false);
}

void GrpcLeaseFetcher::completeAndReset(uint64_t granted, bool success) {
  Callback cb = std::move(current_callback_);
  current_callback_ = nullptr;
  if (cb) {
    cb(granted, success);
  }
}

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
