#include "source/extensions/common/distributed_token_bucket/distributed_token_bucket.h"

#include <algorithm>

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

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

      // Periodically retry the remote source.
      const auto now = time_source_.monotonicTime();
      if (!refill_in_flight_ && now - last_failure_time_ >= config_.retry_interval) {
        refill_in_flight_ = true;
        kick_off_fetch = true;
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
    if (fail_open_active_) {
      ENVOY_LOG(info, "distributed_token_bucket: remote recovered, leaving fail-open mode");
      fail_open_active_ = false;
      // Drop the fallback bucket; the lease is the source of truth again.
      fail_open_bucket_.reset();
    }
    local_tokens_ = std::min(config_.lease_size, local_tokens_ + granted);
    return;
  }

  // Fetch failed.
  last_failure_time_ = time_source_.monotonicTime();

  if (!config_.fail_open) {
    ENVOY_LOG(warn, "distributed_token_bucket: remote unavailable, fail-closed");
    return;
  }

  if (!fail_open_active_) {
    ENVOY_LOG(warn, "distributed_token_bucket: remote unavailable, entering fail-open mode at "
                    "configured rate");
    fail_open_active_ = true;
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
  // The remote source is the source of truth. Local-only reset is best-effort:
  // cap the local lease at `num_tokens` so callers depending on `maybeReset` for
  // pre-fill semantics don't see stale state.
  absl::MutexLock lock(&mutex_);
  local_tokens_ = std::min(local_tokens_, num_tokens);
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
