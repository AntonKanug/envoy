#include "source/extensions/common/distributed_token_bucket/redis_lease_fetcher.h"

#include <chrono>
#include <utility>

#include "envoy/upstream/load_balancer.h"
#include "envoy/upstream/thread_local_cluster.h"

#include "source/common/common/fmt.h"
#include "source/extensions/filters/network/common/redis/client_impl.h"
#include "source/extensions/filters/network/common/redis/codec_impl.h"
#include "source/extensions/filters/network/common/redis/redis_command_stats.h"

#include "absl/base/call_once.h"
#include "absl/status/status.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

namespace RedisClient = Envoy::Extensions::NetworkFilters::Common::Redis::Client;
namespace RedisCommon = Envoy::Extensions::NetworkFilters::Common::Redis;

namespace {

// KEYS[1] = bucket key
// ARGV[1] = rate (tokens/sec, also bucket capacity)
// ARGV[2] = requested tokens
// ARGV[3] = now in ms (caller-provided clock)
// Returns: granted (integer)
constexpr absl::string_view kEvalScript = R"LUA(
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local req  = tonumber(ARGV[2])
local now  = tonumber(ARGV[3])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1]) or rate
local ts     = tonumber(data[2]) or now
tokens = math.min(rate, tokens + (now - ts) * rate / 1000)
local granted = math.min(req, tokens)
tokens = tokens - granted
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, 60000)
return math.floor(granted)
)LUA";

constexpr int kEvalNumKeys = 1;

RedisCommon::RespValue makeBulkString(absl::string_view s) {
  RedisCommon::RespValue v;
  v.type(RedisCommon::RespType::BulkString);
  v.asString() = std::string(s);
  return v;
}

} // namespace

absl::StatusOr<LeaseFetcherPtr>
RedisLeaseFetcher::create(const LeaseFetcherFactoryContext& ctx, const std::string& cluster_name,
                          const std::string& key) {
  if (cluster_name.empty()) {
    return absl::InvalidArgumentError("RedisLeaseFetcher: cluster name is required");
  }
  if (key.empty()) {
    return absl::InvalidArgumentError("RedisLeaseFetcher: key is required");
  }
  // Validate that the cluster exists. We can't check the type without holding
  // a reference, so just confirm the cluster manager knows about it. Picking
  // hosts at fetch time will catch type mismatches via failed connections.
  if (!ctx.cluster_manager.clusters().hasCluster(cluster_name)) {
    return absl::FailedPreconditionError(
        fmt::format("RedisLeaseFetcher: cluster '{}' is not configured", cluster_name));
  }
  return std::unique_ptr<LeaseFetcher>(
      new RedisLeaseFetcher(ctx, cluster_name, key, RedisClient::ClientFactoryImpl::instance_));
}

absl::StatusOr<LeaseFetcherPtr>
RedisLeaseFetcher::createForTest(const LeaseFetcherFactoryContext& ctx,
                                 const std::string& cluster_name, const std::string& key,
                                 RedisClient::ClientFactory& factory) {
  if (cluster_name.empty()) {
    return absl::InvalidArgumentError("RedisLeaseFetcher: cluster name is required");
  }
  if (key.empty()) {
    return absl::InvalidArgumentError("RedisLeaseFetcher: key is required");
  }
  return std::unique_ptr<LeaseFetcher>(new RedisLeaseFetcher(ctx, cluster_name, key, factory));
}

RedisLeaseFetcher::RedisLeaseFetcher(LeaseFetcherFactoryContext ctx, std::string cluster_name,
                                     std::string key, RedisClient::ClientFactory& factory)
    : cluster_name_(std::move(cluster_name)), key_(std::move(key)),
      rate_tokens_per_sec_(ctx.rate_tokens_per_sec), cluster_manager_(ctx.cluster_manager),
      dispatcher_(ctx.dispatcher), scope_(ctx.scope), time_source_(ctx.time_source),
      config_(std::make_shared<RedisConfig>(ctx.op_timeout)), client_factory_(factory),
      command_stats_(RedisCommon::RedisCommandStats::createRedisCommandStats(
          scope_.symbolTable())) {}

RedisLeaseFetcher::~RedisLeaseFetcher() {
  alive_->store(false);
  // The fetcher must outlive any in-flight Redis request; cancel and tear down
  // synchronously here. dispatcher_ is the only thread that touches client_.
  if (current_request_ != nullptr) {
    current_request_->cancel();
    current_request_ = nullptr;
  }
  if (client_) {
    client_->close();
    client_.reset();
  }
}

void RedisLeaseFetcher::fetchLease(uint64_t requested, Callback cb) {
  // The caller may be on any worker; marshal onto our dispatcher.
  auto alive = alive_;
  dispatcher_.post([this, alive, requested, cb = std::move(cb)]() mutable {
    if (!alive->load(std::memory_order_acquire)) {
      // Fetcher was destroyed before this post executed. Tell the caller the
      // request "failed" so they don't think a refill is in flight forever.
      cb(0, false);
      return;
    }
    doFetch(requested, std::move(cb));
  });
}

void RedisLeaseFetcher::doFetch(uint64_t requested, Callback cb) {
  ASSERT(dispatcher_.isThreadSafe());

  if (current_callback_) {
    // Another fetch is already in flight. Drop this one rather than queue it.
    cb(0, false);
    return;
  }

  if (!ensureClient()) {
    cb(0, false);
    return;
  }

  current_callback_ = std::move(cb);
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          time_source_.systemTime().time_since_epoch())
          .count();
  RedisCommon::RespValue request = buildEvalRequest(requested, static_cast<uint64_t>(now_ms));
  current_request_ = client_->makeRequest(request, *this);
  if (current_request_ == nullptr) {
    ENVOY_LOG(debug, "redis_lease_fetcher: makeRequest returned null; treating as failure");
    completeAndReset(0, false);
  }
}

bool RedisLeaseFetcher::ensureClient() {
  if (client_ && client_->active()) {
    return true;
  }

  // Fresh start: drop any old client.
  closeClient();

  auto* tlc = cluster_manager_.getThreadLocalCluster(cluster_name_);
  if (tlc == nullptr) {
    ENVOY_LOG(debug, "redis_lease_fetcher: cluster '{}' has no thread-local entry", cluster_name_);
    return false;
  }
  auto host_or = tlc->loadBalancer().chooseHost(nullptr);
  if (!host_or.host) {
    ENVOY_LOG(debug, "redis_lease_fetcher: no healthy host in cluster '{}'", cluster_name_);
    return false;
  }
  client_ = client_factory_.create(host_or.host, dispatcher_, config_, command_stats_,
                                   tlc->info()->statsScope(), /*auth_username=*/"",
                                   /*auth_password=*/"", /*is_transaction_client=*/false,
                                   absl::nullopt, absl::nullopt);
  if (client_ == nullptr) {
    return false;
  }
  client_->addConnectionCallbacks(*this);
  return true;
}

void RedisLeaseFetcher::onResponse(RedisCommon::RespValuePtr&& value) {
  ASSERT(dispatcher_.isThreadSafe());
  current_request_ = nullptr;
  if (!current_callback_) {
    return; // late response after cancel; ignore
  }

  uint64_t granted = 0;
  bool success = false;
  if (value && value->type() == RedisCommon::RespType::Integer && value->asInteger() >= 0) {
    granted = static_cast<uint64_t>(value->asInteger());
    success = true;
  } else if (value && value->type() == RedisCommon::RespType::Error) {
    ENVOY_LOG(warn, "redis_lease_fetcher: Redis error: {}", value->asString());
  } else {
    ENVOY_LOG(warn, "redis_lease_fetcher: unexpected reply type");
  }
  completeAndReset(granted, success);
}

void RedisLeaseFetcher::onFailure() {
  ASSERT(dispatcher_.isThreadSafe());
  current_request_ = nullptr;
  ENVOY_LOG(debug, "redis_lease_fetcher: Redis request failed");
  // Drop the client so the next fetch reconnects.
  closeClient();
  completeAndReset(0, false);
}

void RedisLeaseFetcher::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    // Defer the close so the upstream callback chain can finish.
    if (client_) {
      dispatcher_.deferredDelete(std::move(client_));
    }
  }
}

void RedisLeaseFetcher::completeAndReset(uint64_t granted, bool success) {
  Callback cb = std::move(current_callback_);
  current_callback_ = nullptr;
  if (cb) {
    cb(granted, success);
  }
}

void RedisLeaseFetcher::closeClient() {
  if (current_request_ != nullptr) {
    current_request_->cancel();
    current_request_ = nullptr;
  }
  if (client_) {
    client_->close();
    client_.reset();
  }
}

void RedisLeaseFetcher::registerAsDefaultFactory() {
  static absl::once_flag flag;
  absl::call_once(flag, []() {
    LeaseFetcherFactoryRegistry::registerFactory(
        [](const LeaseFetcherFactoryContext& ctx, const std::string& cluster,
           const std::string& key) -> absl::StatusOr<LeaseFetcherPtr> {
          return RedisLeaseFetcher::create(ctx, cluster, key);
        });
  });
}

RedisCommon::RespValue RedisLeaseFetcher::buildEvalRequest(uint64_t requested,
                                                           uint64_t now_ms) const {
  RedisCommon::RespValue eval;
  eval.type(RedisCommon::RespType::Array);
  std::vector<RedisCommon::RespValue> args;
  args.reserve(6);
  args.push_back(makeBulkString("EVAL"));
  args.push_back(makeBulkString(kEvalScript));
  args.push_back(makeBulkString(std::to_string(kEvalNumKeys)));
  args.push_back(makeBulkString(key_));
  args.push_back(makeBulkString(std::to_string(rate_tokens_per_sec_)));
  args.push_back(makeBulkString(std::to_string(requested)));
  args.push_back(makeBulkString(std::to_string(now_ms)));
  eval.asArray() = std::move(args);
  return eval;
}

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
