#include "source/common/filter/udp_listener_filter.h"

#include "envoy/common/exception.h"

namespace Envoy {
namespace Filter {

UdpListenerFilter::UdpListenerFilter(
    Config::ExtensionConfigProvider<Network::UdpListenerFilterFactoryCb>& provider,
    Network::UdpReadFilterCallbacks& callbacks, Stats::Counter& update_failures)
    : UdpListenerReadFilter(callbacks), provider_(provider), update_failures_(update_failures) {
  update();
  update_handle_ = provider_.addUpdateCallback([this]() { update(); });
}

void UdpListenerFilter::update() {
  // Updates and datagram dispatch run on the same worker, so an old filter cannot be on the stack
  // here. Keep it alive until replacement construction finishes, then destroy it on this worker.
  auto previous = std::move(filters_);
  filters_.clear();
  auto factory = provider_.config();
  if (!factory.has_value()) {
    return;
  }

  std::string error;
  bool failed = false;
  TRY_NEEDS_AUDIT {
    factory.value()(static_cast<Network::UdpListenerFilterManager&>(*this), *read_callbacks_);
    if (!isConfigReady()) {
      error = "factory produced an empty or unready filter chain";
      failed = true;
    }
  }
  END_TRY
  CATCH(const EnvoyException& e, {
    error = e.what();
    failed = true;
  });
  if (failed) {
    // Worker construction happens after ECDS acceptance and cannot NACK the response. Drop new
    // datagrams until a subsequent configuration constructs successfully; never keep stale policy
    // or pass through a partially built chain.
    filters_.clear();
    update_failures_.inc();
    ENVOY_LOG(error, "UDP listener filter {} worker update failed: {}", provider_.name(), error);
  }
}

void UdpListenerFilter::addReadFilter(Network::UdpListenerReadFilterPtr&& filter) {
  filters_.push_back(std::move(filter));
}

bool UdpListenerFilter::isConfigReady() const {
  if (filters_.empty()) {
    return false;
  }
  for (const auto& filter : filters_) {
    if (filter == nullptr || !filter->isConfigReady()) {
      return false;
    }
  }
  return true;
}

Network::FilterStatus UdpListenerFilter::onData(Network::UdpRecvData& data) {
  if (!isConfigReady()) {
    return Network::FilterStatus::StopIteration;
  }
  for (const auto& filter : filters_) {
    if (filter->onData(data) == Network::FilterStatus::StopIteration) {
      return Network::FilterStatus::StopIteration;
    }
  }
  return Network::FilterStatus::Continue;
}

Network::FilterStatus UdpListenerFilter::onReceiveError(Api::IoError::IoErrorCode error_code) {
  if (!isConfigReady()) {
    return Network::FilterStatus::StopIteration;
  }
  for (const auto& filter : filters_) {
    if (filter->onReceiveError(error_code) == Network::FilterStatus::StopIteration) {
      return Network::FilterStatus::StopIteration;
    }
  }
  return Network::FilterStatus::Continue;
}

} // namespace Filter
} // namespace Envoy
