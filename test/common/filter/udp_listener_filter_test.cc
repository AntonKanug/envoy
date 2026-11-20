#include "envoy/common/exception.h"

#include "source/common/common/callback_impl.h"
#include "source/common/filter/udp_listener_filter.h"
#include "source/common/stats/isolated_store_impl.h"

#include "test/mocks/network/mocks.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Filter {
namespace {

using testing::ElementsAre;
using testing::ReturnRef;
using Factory = Network::UdpListenerFilterFactoryCb;

class TestProvider : public Config::ExtensionConfigProvider<Factory> {
public:
  const std::string& name() override { return name_; }
  OptRef<Factory> config() override {
    return factory_.has_value() ? OptRef<Factory>(*factory_) : std::nullopt;
  }
  Common::CallbackHandlePtr addUpdateCallback(std::function<void()> callback) override {
    return updates_.add(std::move(callback));
  }
  void update(std::optional<Factory> factory) {
    factory_ = std::move(factory);
    updates_.runCallbacks();
  }

  const std::string name_{"test"};
  std::optional<Factory> factory_;
  Common::CallbackManager<void> updates_;
};

struct FilterState {
  bool ready_{true};
  Network::FilterStatus status_{Network::FilterStatus::Continue};
  uint64_t data_calls_{0};
  uint64_t error_calls_{0};
  uint64_t destroyed_{0};
  std::function<void()> on_destroy_;
};

class RecordingFilter : public Network::UdpListenerReadFilter {
public:
  RecordingFilter(Network::UdpReadFilterCallbacks& callbacks, FilterState& state, uint64_t id,
                  std::vector<uint64_t>& calls)
      : UdpListenerReadFilter(callbacks), state_(state), id_(id), calls_(calls) {}
  ~RecordingFilter() override {
    ++state_.destroyed_;
    if (state_.on_destroy_) {
      state_.on_destroy_();
    }
  }
  bool isConfigReady() const override { return state_.ready_; }
  Network::FilterStatus onData(Network::UdpRecvData&) override {
    ++state_.data_calls_;
    calls_.push_back(id_);
    return state_.status_;
  }
  Network::FilterStatus onReceiveError(Api::IoError::IoErrorCode) override {
    ++state_.error_calls_;
    calls_.push_back(id_);
    return state_.status_;
  }

private:
  FilterState& state_;
  const uint64_t id_;
  std::vector<uint64_t>& calls_;
};

class UdpListenerFilterTest : public testing::Test {
public:
  Factory factory(FilterState& state, uint64_t id = 1) {
    return [this, &state, id](Network::UdpListenerFilterManager& manager,
                              Network::UdpReadFilterCallbacks& callbacks) {
      manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, state, id, calls_));
    };
  }

  TestProvider provider_;
  Stats::IsolatedStoreImpl stats_;
  Stats::Counter& update_failures_{stats_.rootScope()->counterFromString("update_failures")};
  testing::NiceMock<Network::MockUdpReadFilterCallbacks> callbacks_;
  std::vector<uint64_t> calls_;
  Network::UdpRecvData data_;
};

TEST_F(UdpListenerFilterTest, MissingAndEmptyConfigurationFailClosedThenRecover) {
  FilterState state;
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_EQ(Network::FilterStatus::StopIteration,
            filter.onReceiveError(Api::IoError::IoErrorCode::UnknownError));
  EXPECT_EQ(0, update_failures_.value());

  provider_.update([](Network::UdpListenerFilterManager&, Network::UdpReadFilterCallbacks&) {});
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_EQ(1, update_failures_.value());

  provider_.update(factory(state));
  EXPECT_TRUE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::Continue, filter.onData(data_));
  EXPECT_EQ(1, state.data_calls_);

  provider_.update(std::nullopt);
  EXPECT_EQ(1, state.destroyed_);
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_EQ(1, state.data_calls_);

  provider_.update(factory(state));
  EXPECT_TRUE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::Continue,
            filter.onReceiveError(Api::IoError::IoErrorCode::UnknownError));
  EXPECT_EQ(1, state.error_calls_);
  EXPECT_EQ(1, update_failures_.value());
}

TEST_F(UdpListenerFilterTest, FailedWorkerConstructionClearsPartialAndPreviousFilters) {
  FilterState old_state;
  FilterState partial_state;
  FilterState recovered_state;
  provider_.factory_ = factory(old_state);
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  provider_.update([&](Network::UdpListenerFilterManager& manager,
                       Network::UdpReadFilterCallbacks& callbacks) {
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, partial_state, 2, calls_));
    // Even an exception without a message must not publish the partial chain.
    throw EnvoyException("");
  });
  EXPECT_EQ(1, old_state.destroyed_);
  EXPECT_EQ(1, partial_state.destroyed_);
  EXPECT_EQ(1, update_failures_.value());
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_TRUE(calls_.empty());

  provider_.update(factory(recovered_state, 3));
  EXPECT_TRUE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::Continue, filter.onData(data_));
  EXPECT_THAT(calls_, ElementsAre(3));
  EXPECT_EQ(1, update_failures_.value());
}

TEST_F(UdpListenerFilterTest, InitialWorkerConstructionFailureIsClosedAndCanRecover) {
  FilterState state;
  provider_.factory_ = [](Network::UdpListenerFilterManager&, Network::UdpReadFilterCallbacks&) {
    throw EnvoyException("worker construction failed");
  };
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(1, update_failures_.value());
  provider_.update(factory(state));
  EXPECT_TRUE(filter.isConfigReady());
  EXPECT_EQ(1, update_failures_.value());
}

TEST_F(UdpListenerFilterTest, NullWorkerFilterFailsClosed) {
  provider_.factory_ = [](Network::UdpListenerFilterManager& manager,
                          Network::UdpReadFilterCallbacks&) { manager.addReadFilter(nullptr); };
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_EQ(1, update_failures_.value());
}

TEST_F(UdpListenerFilterTest, ReplacementKeepsOldInstanceUntilNewConstructionCompletes) {
  FilterState old_state;
  FilterState new_state;
  FilterState static_state;
  RecordingFilter static_filter(callbacks_, static_state, 0, calls_);
  provider_.factory_ = factory(old_state);
  {
    UdpListenerFilter filter(provider_, callbacks_, update_failures_);
    provider_.update([&](Network::UdpListenerFilterManager& manager,
                         Network::UdpReadFilterCallbacks& callbacks) {
      EXPECT_EQ(0, old_state.destroyed_);
      EXPECT_EQ(0, static_state.destroyed_);
      manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, new_state, 2, calls_));
    });
    EXPECT_EQ(1, old_state.destroyed_);
    EXPECT_EQ(0, new_state.destroyed_);
    EXPECT_EQ(0, static_state.destroyed_);
    EXPECT_EQ(Network::FilterStatus::Continue, filter.onData(data_));
    EXPECT_THAT(calls_, ElementsAre(2));
  }
  EXPECT_EQ(1, new_state.destroyed_);
  EXPECT_EQ(0, static_state.destroyed_);
}

TEST_F(UdpListenerFilterTest, MultipleFiltersPreserveOrderAndStopIterationForDataAndErrors) {
  FilterState first;
  FilterState second;
  FilterState third;
  second.status_ = Network::FilterStatus::StopIteration;
  provider_.factory_ = [&](Network::UdpListenerFilterManager& manager,
                           Network::UdpReadFilterCallbacks& callbacks) {
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, first, 1, calls_));
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, second, 2, calls_));
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, third, 3, calls_));
  };
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_THAT(calls_, ElementsAre(1, 2));
  calls_.clear();
  EXPECT_EQ(Network::FilterStatus::StopIteration,
            filter.onReceiveError(Api::IoError::IoErrorCode::UnknownError));
  EXPECT_THAT(calls_, ElementsAre(1, 2));
  EXPECT_EQ(0, third.data_calls_);
  EXPECT_EQ(0, third.error_calls_);

  second.status_ = Network::FilterStatus::Continue;
  calls_.clear();
  EXPECT_EQ(Network::FilterStatus::Continue, filter.onData(data_));
  EXPECT_THAT(calls_, ElementsAre(1, 2, 3));
  calls_.clear();
  EXPECT_EQ(Network::FilterStatus::Continue,
            filter.onReceiveError(Api::IoError::IoErrorCode::UnknownError));
  EXPECT_THAT(calls_, ElementsAre(1, 2, 3));
}

TEST_F(UdpListenerFilterTest, UnreadyMemberBlocksEntireSubchainBeforeSideEffects) {
  FilterState first;
  FilterState second;
  second.ready_ = false;
  provider_.factory_ = [&](Network::UdpListenerFilterManager& manager,
                           Network::UdpReadFilterCallbacks& callbacks) {
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, first, 1, calls_));
    manager.addReadFilter(std::make_unique<RecordingFilter>(callbacks, second, 2, calls_));
  };
  UdpListenerFilter filter(provider_, callbacks_, update_failures_);
  EXPECT_FALSE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::StopIteration, filter.onData(data_));
  EXPECT_EQ(Network::FilterStatus::StopIteration,
            filter.onReceiveError(Api::IoError::IoErrorCode::UnknownError));
  EXPECT_TRUE(calls_.empty());
  EXPECT_EQ(1, first.destroyed_);
  EXPECT_EQ(1, second.destroyed_);
  EXPECT_EQ(1, update_failures_.value());
  second.ready_ = true;
  provider_.update(provider_.factory_);
  EXPECT_TRUE(filter.isConfigReady());
  EXPECT_EQ(Network::FilterStatus::Continue, filter.onData(data_));
  EXPECT_THAT(calls_, ElementsAre(1, 2));
}

TEST_F(UdpListenerFilterTest, UnregistersBeforeFilterDestructionWhileListenerIsAlive) {
  FilterState state;
  provider_.factory_ = factory(state);
  auto filter = std::make_unique<UdpListenerFilter>(provider_, callbacks_, update_failures_);
  EXPECT_EQ(1, provider_.updates_.size());
  state.on_destroy_ = [&]() {
    EXPECT_EQ(0, provider_.updates_.size());
    // Real UDP filters can use their listener during destruction, for example to cancel work.
    callbacks_.udpListener().activateRead();
  };
  EXPECT_CALL(callbacks_, udpListener()).WillOnce(ReturnRef(callbacks_.udp_listener_));
  EXPECT_CALL(callbacks_.udp_listener_, activateRead());
  filter.reset();
  EXPECT_EQ(1, state.destroyed_);
  EXPECT_EQ(0, provider_.updates_.size());

  bool constructed_after_destruction = false;
  provider_.update([&](Network::UdpListenerFilterManager&, Network::UdpReadFilterCallbacks&) {
    constructed_after_destruction = true;
  });
  EXPECT_FALSE(constructed_after_destruction);
}

} // namespace
} // namespace Filter
} // namespace Envoy
