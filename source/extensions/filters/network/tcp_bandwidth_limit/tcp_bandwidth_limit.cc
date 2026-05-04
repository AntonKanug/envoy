#include "source/extensions/filters/network/tcp_bandwidth_limit/tcp_bandwidth_limit.h"

#include "envoy/buffer/buffer.h"
#include "envoy/common/exception.h"
#include "envoy/event/dispatcher.h"
#include "envoy/network/connection.h"

#include "envoy/grpc/async_client_manager.h"

#include "source/common/common/assert.h"
#include "source/common/common/fmt.h"
#include "source/common/common/shared_token_bucket_impl.h"
#include "source/common/grpc/typed_async_client.h"
#include "source/extensions/common/distributed_token_bucket/distributed_token_bucket.h"
#include "source/extensions/common/distributed_token_bucket/grpc_lease_fetcher.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace TcpBandwidthLimit {
namespace {

constexpr uint64_t kiloBytesToBytes(uint64_t val) { return val * 1024; }
constexpr int64_t RateUpdateIntervalMs = 1000;
constexpr uint64_t MillisecondsPerSecond = 1000;
constexpr uint64_t MinLeaseKb = 64;
constexpr uint64_t MaxLeaseKb = 16 * 1024;       // 16 MiB
constexpr std::chrono::milliseconds QuotaRpcTimeout{500};

// Auto-derive a sensible lease size when the operator hasn't picked one. Want
// roughly one fill_interval's worth of bandwidth, clamped between MinLeaseKb
// and MaxLeaseKb so we don't DoS the quota service at small rates and don't
// over-allow at huge ones.
uint64_t deriveLeaseKb(uint64_t rate_kbps, uint64_t fill_interval_ms) {
  const uint64_t target_kb = rate_kbps * fill_interval_ms / 1000;
  return std::min(MaxLeaseKb, std::max(MinLeaseKb, target_kb));
}

std::shared_ptr<TokenBucket> makeBucket(
    bool has_limit, uint64_t limit_kbps, uint64_t fill_interval_ms, absl::string_view direction,
    const envoy::extensions::filters::network::tcp_bandwidth_limit::v3::TcpBandwidthLimit& proto,
    Server::Configuration::ServerFactoryContext& server_ctx, Stats::Scope& scope,
    Common::DistributedTokenBucket::DistributedBucketStats* stats) {
  if (!has_limit) {
    return nullptr;
  }
  const uint64_t rate_bps = kiloBytesToBytes(limit_kbps);

  if (!proto.has_distributed()) {
    return std::make_shared<SharedTokenBucketImpl>(rate_bps, server_ctx.timeSource(),
                                                   static_cast<double>(rate_bps));
  }

  const auto& dist = proto.distributed();
  const uint64_t lease_kb =
      dist.has_lease_kb() ? dist.lease_kb().value() : deriveLeaseKb(limit_kbps, fill_interval_ms);
  const uint64_t lease_size = kiloBytesToBytes(lease_kb);
  // B2: per-instance fail-open rate. If the operator did not pick a value,
  // default to the configured global rate divided by 1 — i.e. *no* implicit
  // de-rating, matching the operationally-safe-but-conservative choice that
  // ``fail_open`` itself defaults to false.
  const uint64_t fail_open_rate_kbps =
      dist.has_fail_open_kbps() ? dist.fail_open_kbps().value() : limit_kbps;

  Common::DistributedTokenBucket::DistributedTokenBucketConfig dt_config{
      /*lease_size=*/lease_size,
      /*low_watermark=*/lease_size / 4,
      /*fail_open_rate_tokens_per_sec=*/kiloBytesToBytes(fail_open_rate_kbps),
      /*fail_open=*/dist.fail_open(),
      /*retry_interval=*/std::chrono::milliseconds(1000),
      /*stats=*/stats,
  };

  // Build a typed gRPC client for the configured quota service.
  Grpc::GrpcServiceConfigWithHashKey config_with_hash_key(dist.quota_service());
  auto client_or = server_ctx.clusterManager().grpcAsyncClientManager()
                       .getOrCreateRawAsyncClientWithHashKey(config_with_hash_key, scope, true);
  if (!client_or.ok()) {
    throw EnvoyException(fmt::format(
        "tcp_bandwidth_limit: distributed mode quota_service is misconfigured: {}",
        client_or.status().message()));
  }

  // B1: separate read and write keys to prevent the two directions sharing a
  // single global pool by accident.
  std::string suffixed_key = absl::StrCat(dist.key(), ":", direction);

  auto fetcher = std::make_unique<Common::DistributedTokenBucket::GrpcLeaseFetcher>(
      client_or.value(), QuotaRpcTimeout, std::move(suffixed_key), rate_bps,
      server_ctx.mainThreadDispatcher());

  return std::make_shared<Common::DistributedTokenBucket::DistributedTokenBucket>(
      dt_config, std::move(fetcher), server_ctx.timeSource());
}

} // namespace

FilterConfig::FilterConfig(
    const envoy::extensions::filters::network::tcp_bandwidth_limit::v3::TcpBandwidthLimit& config,
    Stats::Scope& scope, Server::Configuration::ServerFactoryContext& server_ctx)
    : runtime_(server_ctx.runtime()), time_source_(server_ctx.timeSource()),
      read_limit_kbps_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, read_limit_kbps, 0)),
      write_limit_kbps_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, write_limit_kbps, 0)),
      fill_interval_(std::chrono::milliseconds(
          PROTOBUF_GET_MS_OR_DEFAULT(config, fill_interval, 50))), // Default 50ms
      enabled_(config.runtime_enabled(), server_ctx.runtime()),
      stats_(generateStats(config.stat_prefix(), scope)),
      distributed_stats_(
          config.has_distributed()
              ? std::make_unique<
                    Extensions::Common::DistributedTokenBucket::DistributedBucketStats>(
                    Extensions::Common::DistributedTokenBucket::generateDistributedBucketStats(
                        config.stat_prefix(), scope))
              : nullptr),
      // The token bucket is configured with a max token count of the number of
      // bytes per second, and refills at the same rate, so that we have a per
      // second limit which refills gradually over the fill interval. When the
      // proto sets ``distributed``, the bucket is replaced with a remote-leased
      // implementation sharing state across processes.
      read_token_bucket_(makeBucket(config.has_read_limit_kbps(), read_limit_kbps_,
                                    fill_interval_.count(), "read", config, server_ctx, scope,
                                    distributed_stats_.get())),
      write_token_bucket_(makeBucket(config.has_write_limit_kbps(), write_limit_kbps_,
                                     fill_interval_.count(), "write", config, server_ctx, scope,
                                     distributed_stats_.get())) {}

TcpBandwidthLimitStats FilterConfig::generateStats(const std::string& prefix, Stats::Scope& scope) {
  const std::string final_prefix = prefix + ".tcp_bandwidth_limit";
  return {ALL_TCP_BANDWIDTH_LIMIT_STATS(POOL_COUNTER_PREFIX(scope, final_prefix),
                                        POOL_GAUGE_PREFIX(scope, final_prefix))};
}

TcpBandwidthLimitFilter::TcpBandwidthLimitFilter(FilterConfigSharedPtr config)
    : config_(config), read_buffer_([this]() { onReadBufferLowWatermark(); },
                                    [this]() { onReadBufferHighWatermark(); }, []() -> void {}),
      write_buffer_([this]() { onWriteBufferLowWatermark(); },
                    [this]() { onWriteBufferHighWatermark(); }, []() -> void {}),
      last_read_rate_update_(config->timeSource().monotonicTime()),
      last_write_rate_update_(config->timeSource().monotonicTime()) {}

TcpBandwidthLimitFilter::~TcpBandwidthLimitFilter() {
  if (read_timer_) {
    read_timer_->disableTimer();
    read_timer_.reset();
  }
  if (write_timer_) {
    write_timer_->disableTimer();
    write_timer_.reset();
  }
}

Network::FilterStatus TcpBandwidthLimitFilter::onData(Buffer::Instance& data, bool end_stream) {
  if (!config_->enabled() || !config_->hasReadLimit()) {
    return Network::FilterStatus::Continue;
  }

  config_->stats().read_enabled_.inc();

  // If there's already buffered data, we must buffer new data too to preserve byte ordering.
  if (read_buffer_.length() > 0) {
    config_->stats().read_throttled_.inc();
    read_end_stream_ = end_stream;
    read_buffer_.move(data);
    config_->stats().read_bytes_buffered_.set(read_buffer_.length());
    return Network::FilterStatus::StopIteration;
  }

  uint64_t data_size = data.length();
  uint64_t consumed = config_->readTokenBucket()->consume(data_size, true);

  if (consumed < data_size) {
    config_->stats().read_throttled_.inc();

    if (consumed > 0) {
      Buffer::OwnedImpl passthrough;
      passthrough.move(data, consumed);
      read_callbacks_->injectReadDataToFilterChain(passthrough, false);
      updateReadRate(consumed);
    }

    read_end_stream_ = end_stream;
    read_buffer_.move(data);
    config_->stats().read_bytes_buffered_.set(read_buffer_.length());

    if (!read_timer_) {
      read_timer_ =
          read_callbacks_->connection().dispatcher().createTimer([this]() { onReadTokenTimer(); });
      read_timer_->enableTimer(config_->fillInterval());
    }

    return Network::FilterStatus::StopIteration;
  }

  updateReadRate(data_size);
  return Network::FilterStatus::Continue;
}

Network::FilterStatus TcpBandwidthLimitFilter::onWrite(Buffer::Instance& data, bool end_stream) {
  if (!config_->enabled() || !config_->hasWriteLimit()) {
    return Network::FilterStatus::Continue;
  }

  config_->stats().write_enabled_.inc();

  // If there's already buffered data, we must buffer new data too to preserve byte ordering.
  if (write_buffer_.length() > 0) {
    config_->stats().write_throttled_.inc();
    write_end_stream_ = end_stream;
    write_buffer_.move(data);
    config_->stats().write_bytes_buffered_.set(write_buffer_.length());
    return Network::FilterStatus::StopIteration;
  }

  uint64_t data_size = data.length();
  uint64_t consumed = config_->writeTokenBucket()->consume(data_size, true);

  if (consumed < data_size) {
    config_->stats().write_throttled_.inc();

    if (consumed > 0) {
      Buffer::OwnedImpl to_send;
      to_send.move(data, consumed);
      write_callbacks_->injectWriteDataToFilterChain(to_send, false);
      updateWriteRate(consumed);
    }

    write_end_stream_ = end_stream;
    write_buffer_.move(data);
    config_->stats().write_bytes_buffered_.set(write_buffer_.length());

    if (!write_timer_) {
      write_timer_ =
          read_callbacks_->connection().dispatcher().createTimer([this]() { onWriteTokenTimer(); });
      write_timer_->enableTimer(config_->fillInterval());
    }

    return Network::FilterStatus::StopIteration;
  }

  updateWriteRate(data_size);
  return Network::FilterStatus::Continue;
}

void TcpBandwidthLimitFilter::onReadTokenTimer() {
  processBufferedReadData();

  if (read_buffer_.length() > 0) {
    read_timer_->enableTimer(config_->fillInterval());
  } else {
    read_timer_.reset();
  }
}

void TcpBandwidthLimitFilter::onWriteTokenTimer() {
  processBufferedWriteData();

  if (write_buffer_.length() > 0) {
    write_timer_->enableTimer(config_->fillInterval());
  } else {
    write_timer_.reset();
  }
}

void TcpBandwidthLimitFilter::onReadBufferHighWatermark() {
  read_callbacks_->connection().readDisable(true);
}

void TcpBandwidthLimitFilter::onReadBufferLowWatermark() {
  read_callbacks_->connection().readDisable(false);
}

void TcpBandwidthLimitFilter::onWriteBufferHighWatermark() {
  write_callbacks_->onAboveWriteBufferHighWatermark();
}

void TcpBandwidthLimitFilter::onWriteBufferLowWatermark() {
  write_callbacks_->onBelowWriteBufferLowWatermark();
}

void TcpBandwidthLimitFilter::processBufferedReadData() {
  if (read_buffer_.length() == 0 || !config_->readTokenBucket()) {
    return;
  }

  uint64_t buffer_size = read_buffer_.length();
  uint64_t consumed = config_->readTokenBucket()->consume(buffer_size, true);

  if (consumed > 0) {
    Buffer::OwnedImpl data_to_send;
    data_to_send.move(read_buffer_, consumed);
    const bool end_stream = read_end_stream_ && read_buffer_.length() == 0;
    read_callbacks_->injectReadDataToFilterChain(data_to_send, end_stream);
    updateReadRate(consumed);
    config_->stats().read_bytes_buffered_.set(read_buffer_.length());
  }
}

void TcpBandwidthLimitFilter::processBufferedWriteData() {
  if (write_buffer_.length() == 0 || !config_->writeTokenBucket()) {
    return;
  }

  uint64_t buffer_size = write_buffer_.length();
  uint64_t consumed = config_->writeTokenBucket()->consume(buffer_size, true);

  if (consumed > 0) {
    Buffer::OwnedImpl data_to_send;
    data_to_send.move(write_buffer_, consumed);
    const bool end_stream = write_end_stream_ && write_buffer_.length() == 0;
    write_callbacks_->injectWriteDataToFilterChain(data_to_send, end_stream);
    updateWriteRate(consumed);
    config_->stats().write_bytes_buffered_.set(write_buffer_.length());
  }
}

void TcpBandwidthLimitFilter::updateReadRate(uint64_t bytes) {
  config_->stats().read_total_bytes_.add(bytes);
  read_bytes_since_last_rate_ += bytes;
  const auto now = config_->timeSource().monotonicTime();
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - last_read_rate_update_);
  if (elapsed.count() >= RateUpdateIntervalMs) {
    const uint64_t rate = (read_bytes_since_last_rate_ * MillisecondsPerSecond) / elapsed.count();
    config_->stats().read_rate_bps_.set(rate);
    read_bytes_since_last_rate_ = 0;
    last_read_rate_update_ = now;
  }
}

void TcpBandwidthLimitFilter::updateWriteRate(uint64_t bytes) {
  config_->stats().write_total_bytes_.add(bytes);
  write_bytes_since_last_rate_ += bytes;
  const auto now = config_->timeSource().monotonicTime();
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - last_write_rate_update_);
  if (elapsed.count() >= RateUpdateIntervalMs) {
    const uint64_t rate = (write_bytes_since_last_rate_ * MillisecondsPerSecond) / elapsed.count();
    config_->stats().write_rate_bps_.set(rate);
    write_bytes_since_last_rate_ = 0;
    last_write_rate_update_ = now;
  }
}

} // namespace TcpBandwidthLimit
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
