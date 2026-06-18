#include <chrono>
#include <utility>
#include <vector>

#include "source/common/stats/isolated_store_impl.h"
#include "source/extensions/common/distributed_token_bucket/distributed_token_bucket.h"
#include "source/extensions/common/distributed_token_bucket/lease_fetcher.h"

#include "test/test_common/simulated_time_system.h"

#include "gtest/gtest.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {
namespace {

// A controllable LeaseFetcher: captures each call so the test can trigger
// success/failure responses deterministically.
class FakeLeaseFetcher : public LeaseFetcher {
public:
  struct PendingFetch {
    uint64_t requested;
    Callback cb;
  };

  void fetchLease(uint64_t requested, Callback cb) override {
    pending_.push_back({requested, std::move(cb)});
  }

  void respondSuccess(uint64_t granted) {
    ASSERT_FALSE(pending_.empty());
    auto fetch = std::move(pending_.front());
    pending_.erase(pending_.begin());
    fetch.cb(granted, true);
  }

  void respondFailure() {
    ASSERT_FALSE(pending_.empty());
    auto fetch = std::move(pending_.front());
    pending_.erase(pending_.begin());
    fetch.cb(0, false);
  }

  size_t pendingCount() const { return pending_.size(); }
  uint64_t lastRequested() const {
    EXPECT_FALSE(pending_.empty());
    return pending_.back().requested;
  }

private:
  std::vector<PendingFetch> pending_;
};

class DistributedTokenBucketTest : public ::testing::Test {
protected:
  DistributedTokenBucketConfig defaultConfig() {
    return {
        /*lease_size=*/1000,
        /*low_watermark=*/200,
        /*fail_open_rate_tokens_per_sec=*/100,
        /*fail_open=*/true,
        /*retry_interval=*/std::chrono::milliseconds(100),
        /*stats=*/nullptr,
    };
  }

  std::unique_ptr<DistributedTokenBucket> makeBucket(DistributedTokenBucketConfig cfg) {
    auto fetcher = std::make_unique<FakeLeaseFetcher>();
    fetcher_ = fetcher.get();
    return std::make_unique<DistributedTokenBucket>(cfg, std::move(fetcher), time_system_);
  }

  Event::SimulatedTimeSystem time_system_;
  FakeLeaseFetcher* fetcher_{nullptr};
};

TEST_F(DistributedTokenBucketTest, EmptyLeaseTriggersFetchAndConsumesAfterGrant) {
  auto bucket = makeBucket(defaultConfig());

  // Local lease starts empty, so the first consume() with allow_partial returns 0
  // and kicks off a fetch.
  EXPECT_EQ(0, bucket->consume(500, true));
  EXPECT_EQ(1, fetcher_->pendingCount());
  EXPECT_TRUE(bucket->refillInFlightForTest());
  EXPECT_EQ(1000, fetcher_->lastRequested());

  fetcher_->respondSuccess(800);
  EXPECT_FALSE(bucket->refillInFlightForTest());
  EXPECT_EQ(800, bucket->localLeaseForTest());

  EXPECT_EQ(500, bucket->consume(500, true));
  EXPECT_EQ(300, bucket->localLeaseForTest());
}

TEST_F(DistributedTokenBucketTest, RefillFiresWhenBelowWatermark) {
  auto bucket = makeBucket(defaultConfig());

  // Prime the local lease above the watermark.
  EXPECT_EQ(0, bucket->consume(0, true));
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondSuccess(1000);
  ASSERT_EQ(1000, bucket->localLeaseForTest());

  // Drain to just above the low_watermark (200). No refill expected.
  EXPECT_EQ(700, bucket->consume(700, true));
  EXPECT_EQ(0, fetcher_->pendingCount());

  // Cross the watermark. Now a refill kicks off.
  EXPECT_EQ(150, bucket->consume(150, true));
  EXPECT_EQ(150, bucket->localLeaseForTest());
  EXPECT_EQ(1, fetcher_->pendingCount());
  // We requested headroom: lease_size - local_tokens_ = 1000 - 150 = 850.
  EXPECT_EQ(850, fetcher_->lastRequested());
}

TEST_F(DistributedTokenBucketTest, AllowPartialReturnsPartial) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondSuccess(50);
  ASSERT_EQ(50, bucket->localLeaseForTest());

  // Asking for 200 with only 50 available, allow_partial=true, returns 50.
  EXPECT_EQ(50, bucket->consume(200, true));
  EXPECT_EQ(0, bucket->localLeaseForTest());
}

TEST_F(DistributedTokenBucketTest, NoPartialReturnsZeroWhenInsufficient) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondSuccess(50);
  ASSERT_EQ(50, bucket->localLeaseForTest());

  // Asking for 200 with only 50 available, allow_partial=false, returns 0
  // without consuming.
  EXPECT_EQ(0, bucket->consume(200, false));
  EXPECT_EQ(50, bucket->localLeaseForTest());
}

TEST_F(DistributedTokenBucketTest, FailOpenEntersFallbackOnFailure) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondFailure();

  EXPECT_TRUE(bucket->failOpenActiveForTest());

  // Fallback bucket fills at 100 tokens/sec. Advancing 1s gives 100 tokens.
  time_system_.advanceTimeWait(std::chrono::seconds(1));
  EXPECT_EQ(100, bucket->consume(500, true));
}

TEST_F(DistributedTokenBucketTest, FailOpenRetriesAfterIntervalAndExitsOnSuccess) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondFailure();
  ASSERT_TRUE(bucket->failOpenActiveForTest());

  // Immediately calling consume again should NOT trigger a retry (within retry_interval).
  bucket->consume(0, true);
  EXPECT_EQ(0, fetcher_->pendingCount());

  // Advance past the retry interval. Next consume kicks off a retry.
  time_system_.advanceTimeWait(std::chrono::milliseconds(150));
  bucket->consume(0, true);
  EXPECT_EQ(1, fetcher_->pendingCount());

  // B3 regression: the retry must request a real lease (lease_size), not 0.
  // If it asked for 0, the next consume would stall with an empty local lease.
  EXPECT_EQ(1000, fetcher_->lastRequested());

  // Successful retry exits fail-open mode and tops up the lease.
  fetcher_->respondSuccess(1000);
  EXPECT_FALSE(bucket->failOpenActiveForTest());
  EXPECT_EQ(1000, bucket->localLeaseForTest());
}

TEST_F(DistributedTokenBucketTest, FailClosedReturnsZeroOnFailure) {
  auto cfg = defaultConfig();
  cfg.fail_open = false;
  auto bucket = makeBucket(cfg);

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondFailure();

  EXPECT_FALSE(bucket->failOpenActiveForTest());
  // No tokens available at all; consume returns 0.
  EXPECT_EQ(0, bucket->consume(100, true));
  EXPECT_EQ(0, bucket->consume(100, false));
}

TEST_F(DistributedTokenBucketTest, ConcurrentRequestsCoalesceIntoOneFetch) {
  auto bucket = makeBucket(defaultConfig());

  // First call kicks off a fetch.
  bucket->consume(0, true);
  EXPECT_EQ(1, fetcher_->pendingCount());

  // Subsequent calls while the fetch is in flight do NOT issue more fetches.
  bucket->consume(0, true);
  bucket->consume(0, true);
  EXPECT_EQ(1, fetcher_->pendingCount());

  // After the response, a future drain below watermark issues a new fetch.
  fetcher_->respondSuccess(1000);
  EXPECT_EQ(900, bucket->consume(900, true));
  EXPECT_EQ(1, fetcher_->pendingCount());
}

TEST_F(DistributedTokenBucketTest, GrantedTokensCappedAtLeaseSize) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  // Server "over-grants" beyond the lease capacity; bucket caps it at lease_size.
  fetcher_->respondSuccess(5000);
  EXPECT_EQ(1000, bucket->localLeaseForTest());
}

TEST_F(DistributedTokenBucketTest, ConsumeZeroAboveWatermarkDoesNotRefill) {
  auto bucket = makeBucket(defaultConfig());

  // Get the lease above the watermark.
  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondSuccess(1000);
  ASSERT_EQ(1000, bucket->localLeaseForTest());

  // consume(0) above the watermark consumes nothing and triggers no fetch.
  EXPECT_EQ(0, bucket->consume(0, true));
  EXPECT_EQ(0, fetcher_->pendingCount());
}

TEST_F(DistributedTokenBucketTest, NextTokenAvailableReportsZeroWhenTokensPresent) {
  auto bucket = makeBucket(defaultConfig());
  bucket->consume(0, true);
  fetcher_->respondSuccess(500);
  EXPECT_EQ(std::chrono::milliseconds(0), bucket->nextTokenAvailable());
}

TEST_F(DistributedTokenBucketTest, NextTokenAvailableReportsRetryIntervalWhenEmpty) {
  auto cfg = defaultConfig();
  cfg.retry_interval = std::chrono::milliseconds(250);
  auto bucket = makeBucket(cfg);
  // No tokens, no successful fetch yet.
  EXPECT_EQ(std::chrono::milliseconds(250), bucket->nextTokenAvailable());
}

// D9 regression: maybeReset must SET the local count, not clamp downward.
TEST_F(DistributedTokenBucketTest, MaybeResetSetsLocalCount) {
  auto bucket = makeBucket(defaultConfig());

  bucket->consume(0, true);
  fetcher_->respondSuccess(500);
  ASSERT_EQ(500, bucket->localLeaseForTest());

  // The old implementation clamped via std::min, so reset(900) would have left
  // the count at 500. The contract is to *set* the count to 900.
  bucket->maybeReset(900);
  EXPECT_EQ(900, bucket->localLeaseForTest());

  // And reset can also lower the count, as before.
  bucket->maybeReset(100);
  EXPECT_EQ(100, bucket->localLeaseForTest());
}

// Regression: a "successful" lease that grants 0 tokens (remote bucket empty)
// must NOT exit fail-open mode. Otherwise the bucket would drop its fallback,
// reset local_tokens_ to 0, and stall the next consume.
TEST_F(DistributedTokenBucketTest, FailOpenStaysActiveOnZeroGrant) {
  auto bucket = makeBucket(defaultConfig());

  // Enter fail-open by failing a fetch.
  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondFailure();
  ASSERT_TRUE(bucket->failOpenActiveForTest());

  // Cross retry interval and trigger a retry.
  time_system_.advanceTimeWait(std::chrono::milliseconds(150));
  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());

  // Remote responds OK with 0 tokens (bucket is genuinely drained globally).
  fetcher_->respondSuccess(0);

  // Bucket should still be in fail-open mode; otherwise we'd be stuck with an
  // empty local lease and no fallback.
  EXPECT_TRUE(bucket->failOpenActiveForTest());
  EXPECT_EQ(0, bucket->localLeaseForTest());

  // The fallback bucket must still be operational. Advance time by 1s; at
  // fail_open_rate_tokens_per_sec=100 the fallback should accumulate ~100
  // tokens. A consume against the fallback should return them. If the
  // fallback had been silently dropped during the zero-grant path, this
  // would return 0 — which is the regression we're guarding against.
  time_system_.advanceTimeWait(std::chrono::seconds(1));
  EXPECT_EQ(50, bucket->consume(50, true));

  // A subsequent retry that returns >0 grants should finally exit fail-open.
  time_system_.advanceTimeWait(std::chrono::milliseconds(150));
  bucket->consume(0, true);
  ASSERT_EQ(1, fetcher_->pendingCount());
  fetcher_->respondSuccess(500);
  EXPECT_FALSE(bucket->failOpenActiveForTest());
  EXPECT_EQ(500, bucket->localLeaseForTest());
}

// D7: emit stats while the bucket is in use; verify counters move.
TEST_F(DistributedTokenBucketTest, StatsTrackLeaseActivity) {
  Stats::IsolatedStoreImpl store;
  auto stats = generateDistributedBucketStats("bw", *store.rootScope());

  auto cfg = defaultConfig();
  cfg.stats = &stats;
  auto bucket = makeBucket(cfg);

  // First consume kicks a fetch.
  bucket->consume(100, true);
  EXPECT_EQ(1, stats.lease_requests_total_.value());
  EXPECT_EQ(0, stats.lease_granted_bytes_total_.value());

  // Successful fetch increments granted bytes.
  fetcher_->respondSuccess(800);
  EXPECT_EQ(800, stats.lease_granted_bytes_total_.value());
  EXPECT_EQ(0, stats.lease_failures_total_.value());
  EXPECT_EQ(0, stats.fail_open_active_.value());

  // Drain below watermark to trigger another fetch, then fail it.
  bucket->consume(700, true);  // 800 - 700 = 100, below low_watermark=200
  EXPECT_EQ(2, stats.lease_requests_total_.value());
  fetcher_->respondFailure();
  EXPECT_EQ(1, stats.lease_failures_total_.value());
  EXPECT_EQ(1, stats.fail_open_active_.value());

  // Recovery clears the gauge.
  time_system_.advanceTimeWait(std::chrono::milliseconds(150));
  bucket->consume(0, true);
  fetcher_->respondSuccess(1000);
  EXPECT_EQ(0, stats.fail_open_active_.value());
}

} // namespace
} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
