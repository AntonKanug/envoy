#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>

#include "envoy/common/time.h"
#include "envoy/common/token_bucket.h"

#include "source/common/common/logger.h"
#include "source/common/common/token_bucket_impl.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"

#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

struct DistributedTokenBucketConfig {
  // Target lease size (bucket capacity in tokens). Each successful fetch tops
  // the local lease back up toward this value.
  uint64_t lease_size{};

  // Refetch when the local lease drops below this threshold. Should be < lease_size.
  uint64_t low_watermark{};

  // Configured per-instance fill rate in tokens per second. Used as the
  // fallback rate when `fail_open` is true and the remote bucket is unreachable.
  uint64_t fail_open_rate_tokens_per_sec{};

  // If true, fall back to a local TokenBucket at `fail_open_rate_tokens_per_sec`
  // when the remote source is unavailable. If false, return 0 from consume()
  // until the remote recovers (callers will buffer / backpressure).
  bool fail_open{true};

  // After a fetch failure, retry on the next consume() at most this often.
  std::chrono::milliseconds retry_interval{std::chrono::seconds(1)};
};

// A TokenBucket implementation backed by a remote (typically Redis) global
// counter, accessed via `LeaseFetcher`.
//
// Design: a small local lease holds tokens drawn from the global pool. consume()
// is synchronous against the local lease. When the local lease drops below
// `low_watermark` and no fetch is in flight, fetchLease() is dispatched
// asynchronously; the granted tokens top up the local lease when the callback
// fires.
//
// On fetch failure: if `fail_open`, switch to a local-only TokenBucket at the
// configured fail-open rate. Periodically retry the remote on subsequent
// consume() calls; the next successful fetch exits fail-open mode.
//
// Thread safety: all state is guarded by an internal mutex. consume() may be
// called from any thread; the LeaseFetcher callback may fire on any thread.
class DistributedTokenBucket : public TokenBucket, Logger::Loggable<Logger::Id::filter> {
public:
  DistributedTokenBucket(DistributedTokenBucketConfig config, LeaseFetcherPtr fetcher,
                         TimeSource& time_source);
  ~DistributedTokenBucket() override;

  // TokenBucket
  uint64_t consume(uint64_t tokens, bool allow_partial) override;
  uint64_t consume(uint64_t tokens, bool allow_partial,
                   std::chrono::milliseconds& time_to_next_token) override;
  std::chrono::milliseconds nextTokenAvailable() override;
  void maybeReset(uint64_t num_tokens) override;

  // Test / observability hooks.
  bool refillInFlightForTest() const;
  bool failOpenActiveForTest() const;
  uint64_t localLeaseForTest() const;

private:
  // Returns true and decrements `local_tokens_` if a refill should be requested
  // now. Caller must invoke fetcher_->fetchLease() outside the lock.
  bool shouldRequestRefillLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void onLeaseGranted(uint64_t granted, bool success);

  const DistributedTokenBucketConfig config_;
  const LeaseFetcherPtr fetcher_;
  TimeSource& time_source_;
  // Set to false in the destructor; lease-callback lambdas check this before
  // touching `this` so a late callback after destruction is a no-op.
  const std::shared_ptr<std::atomic<bool>> alive_{std::make_shared<std::atomic<bool>>(true)};

  mutable absl::Mutex mutex_;
  uint64_t local_tokens_ ABSL_GUARDED_BY(mutex_){0};
  bool refill_in_flight_ ABSL_GUARDED_BY(mutex_){false};
  bool fail_open_active_ ABSL_GUARDED_BY(mutex_){false};
  MonotonicTime last_failure_time_ ABSL_GUARDED_BY(mutex_){};
  // Allocated lazily the first time we enter fail-open mode.
  std::unique_ptr<TokenBucketImpl> fail_open_bucket_ ABSL_GUARDED_BY(mutex_);
};

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
