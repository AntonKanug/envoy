#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "envoy/common/pure.h"

namespace Envoy {
namespace Extensions {
namespace Common {
namespace DistributedTokenBucket {

class LeaseFetcher {
public:
  virtual ~LeaseFetcher() = default;

  // Called once per fetch with the number of granted tokens. `success` is true
  // if the remote source responded; false if the call failed (timeout, network
  // error, missing cluster, etc.). On failure `granted` is 0.
  using Callback = std::function<void(uint64_t granted, bool success)>;

  // Asynchronously fetch up to `requested` tokens from the remote bucket.
  // The implementation must invoke `cb` exactly once. Implementations are
  // expected to be safe to call from any worker thread; the callback may
  // fire on a different thread than the caller.
  virtual void fetchLease(uint64_t requested, Callback cb) PURE;
};

using LeaseFetcherPtr = std::unique_ptr<LeaseFetcher>;

} // namespace DistributedTokenBucket
} // namespace Common
} // namespace Extensions
} // namespace Envoy
