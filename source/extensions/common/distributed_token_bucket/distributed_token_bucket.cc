#include "source/extensions/common/distributed_token_bucket/distributed_token_bucket.h"

#include <algorithm>

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

DistributedBucketStats generateDistributedBucketStats(const std::string& prefix,
                                                     Stats::Scope& scope) {
  const std::string final_prefix = prefix + ".distributed_bandwidth_limit";
  return {ALL_DISTRIBUTED_BUCKET_STATS(POOL_COUNTER_PREFIX(scope, final_prefix),
                                       POOL_GAUGE_PREFIX(scope, final_prefix))};
}

DistributedTokenBucket::DistributedTokenBucket(DistributedTokenBucketConfig config,
                                               LeaseFetcherPtr fetcher, TimeSource& time_source)
    : config_(config), fetcher_(std::move(fetcher)), time_source_(time_source) {}

DistributedTokenBucket::~DistributedTokenBucket() {
  // Tear the fetcher down first so any in-flight remote requests are cancelled
  // by its destructor before we mark ourselves dead.
  alive_->store(false);
}

uint64_t DistributedTokenBucket::consume(uint64_t tokens, bool allow_partial) {
  std::chrono::milliseconds unused;
  return consume(tokens, allow_partial, unused);
}

uint64_t DistributedTokenBucket::consume(uint64_t tokens, bool allow_partial,
                                         std::chrono::milliseconds& time_to_next_token) {
  bool kick_off_fetch = false;
  uint64_t requested_lease = 0;

  uint64_t consumed = 0;
  {
    absl::MutexLock lock(&mutex_);

    if (fail_open_active_) {
      // Operating from the fallback bucket. consume() draws from it directly.
      ASSERT(fail_open_bucket_ != nullptr);
      consumed = fail_open_bucket_->consume(tokens, allow_partial, time_to_next_token);

      // Periodically retry the remote source. Without setting requested_lease
      // here the retry would ask for 0 tokens — which the remote happily
      // grants — and the bucket would exit fail-open with an empty local
      // lease, stalling the next consume.
      const auto now = time_source_.monotonicTime();
      if (!refill_in_flight_ && now - last_failure_time_ >= config_.retry_interval) {
        refill_in_flight_ = true;
        kick_off_fetch = true;
        requested_lease = config_.lease_size;
      }
    } else {
      // Normal mode: serve from the local lease.
      if (local_tokens_ >= tokens) {
        local_tokens_ -= tokens;
        consumed = tokens;
        time_to_next_token =
            local_tokens_ > 0 ? std::chrono::milliseconds(0) : config_.retry_interval;
      } else if (allow_partial) {
        consumed = local_tokens_;
        local_tokens_ = 0;
        // Without a fresh lease the next token will arrive when the fetch
        // completes. We don't know precisely when; report the retry interval
        // as a coarse upper bound.
        time_to_next_token = config_.retry_interval;
      } else {
        consumed = 0;
        time_to_next_token = config_.retry_interval;
      }

      if (shouldRequestRefillLocked()) {
        refill_in_flight_ = true;
        kick_off_fetch = true;
        requested_lease = (config_.lease_size > local_tokens_)
                              ? config_.lease_size - local_tokens_
                              : config_.lease_size;
      }
    }
  }

  if (kick_off_fetch) {
    if (config_.stats != nullptr) {
      config_.stats->lease_requests_total_.inc();
    }
    auto alive = alive_;
    fetcher_->fetchLease(requested_lease, [this, alive](uint64_t granted, bool success) {
      if (!alive->load(std::memory_order_acquire)) {
        return;
      }
      onLeaseGranted(granted, success);
    });
  }

  return consumed;
}

bool DistributedTokenBucket::shouldRequestRefillLocked() {
  if (refill_in_flight_) {
    return false;
  }
  return local_tokens_ < config_.low_watermark;
}

void DistributedTokenBucket::onLeaseGranted(uint64_t granted, bool success) {
  absl::MutexLock lock(&mutex_);
  refill_in_flight_ = false;

  if (success) {
    if (config_.stats != nullptr) {
      config_.stats->lease_granted_bytes_total_.add(granted);
    }
    // Only exit fail-open when we actually got tokens. A "successful" 0-grant
    // means the remote bucket is genuinely empty right now; if we exited
    // fail-open here we'd drop the fallback bucket, set local_tokens_ = 0, and
    // the next consume() would stall until the next refill returned non-zero.
    // Stay in fail-open until we get real progress.
    if (fail_open_active_ && granted > 0) {
      ENVOY_LOG(info, "distributed_token_bucket: remote recovered, leaving fail-open mode");
      fail_open_active_ = false;
      if (config_.stats != nullptr) {
        config_.stats->fail_open_active_.set(0);
      }
      // Drop the fallback bucket; the lease is the source of truth again.
      fail_open_bucket_.reset();
    }
    local_tokens_ = std::min(config_.lease_size, local_tokens_ + granted);
    return;
  }

  // Fetch failed.
  last_failure_time_ = time_source_.monotonicTime();
  if (config_.stats != nullptr) {
    config_.stats->lease_failures_total_.inc();
  }

  if (!config_.fail_open) {
    ENVOY_LOG(warn, "distributed_token_bucket: remote unavailable, fail-closed");
    return;
  }

  if (!fail_open_active_) {
    ENVOY_LOG(warn, "distributed_token_bucket: remote unavailable, entering fail-open mode at "
                    "configured rate");
    fail_open_active_ = true;
    if (config_.stats != nullptr) {
      config_.stats->fail_open_active_.set(1);
    }
    if (fail_open_bucket_ == nullptr) {
      fail_open_bucket_ = std::make_unique<TokenBucketImpl>(
          config_.fail_open_rate_tokens_per_sec, time_source_,
          static_cast<double>(config_.fail_open_rate_tokens_per_sec));
    }
  }
}

std::chrono::milliseconds DistributedTokenBucket::nextTokenAvailable() {
  absl::MutexLock lock(&mutex_);
  if (fail_open_active_ && fail_open_bucket_ != nullptr) {
    return fail_open_bucket_->nextTokenAvailable();
  }
  if (local_tokens_ > 0) {
    return std::chrono::milliseconds(0);
  }
  return config_.retry_interval;
}

void DistributedTokenBucket::maybeReset(uint64_t num_tokens) {
  // The TokenBucket interface contract is to *set* the local count to
  // `num_tokens`. The remote bucket's state is unchanged; the next refill will
  // reconcile with whatever the remote side believes. Pre-fill semantics
  // (callers that maybeReset to seed an initial burst before traffic flows)
  // now work correctly.
  absl::MutexLock lock(&mutex_);
  local_tokens_ = num_tokens;
}

bool DistributedTokenBucket::refillInFlightForTest() const {
  absl::MutexLock lock(&mutex_);
  return refill_in_flight_;
}

bool DistributedTokenBucket::failOpenActiveForTest() const {
  absl::MutexLock lock(&mutex_);
  return fail_open_active_;
}

uint64_t DistributedTokenBucket::localLeaseForTest() const {
  absl::MutexLock lock(&mutex_);
  return local_tokens_;
}

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
