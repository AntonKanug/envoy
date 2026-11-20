#include <array>
#include <atomic>
#include <map>

#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "envoy/extensions/filters/udp/dns_filter/v3/dns_filter.pb.h"
#include "envoy/extensions/network/dns_resolver/cares/v3/cares_dns_resolver.pb.h"
#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"
#include "envoy/service/discovery/v3/discovery.pb.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/socket_impl.h"
#include "source/common/protobuf/utility.h"

#include "test/extensions/filters/udp/dns_filter/dns_filter_test_utils.h"
#include "test/integration/filters/udp_listener_ecds.pb.h"
#include "test/integration/filters/udp_listener_ecds.pb.validate.h"
#include "test/integration/integration.h"
#include "test/test_common/network_utility.h"

#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace {

constexpr absl::string_view FilterType =
    "type.googleapis.com/test.integration.UdpListenerEcdsConfig";
constexpr absl::string_view ResourceType =
    "type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig";
constexpr absl::string_view ReloadCounter =
    "extension_config_discovery.udp_listener_filter.policy.config_reload";
constexpr absl::string_view FailureCounter =
    "extension_config_discovery.udp_listener_filter.policy.config_fail";
constexpr absl::string_view MissingCounter = "listener.raw_udp_ecds.udp.extension_config_missing";

std::atomic<uint64_t> next_filter_id{0};
std::atomic<uint64_t> live_filters{0};

// The dynamic instance tags a probe, and the static instance answers it. Ordinary DNS datagrams
// pass through both instances into the real dns_filter. Instance IDs detect accidental replacement
// even if an allocator reuses the same address.
class UdpListenerEcdsTestFilter : public Network::UdpListenerReadFilter {
public:
  UdpListenerEcdsTestFilter(Network::UdpReadFilterCallbacks& callbacks,
                            const test::integration::UdpListenerEcdsConfig& config)
      : UdpListenerReadFilter(callbacks), config_(config), id_(++next_filter_id) {
    ++live_filters;
  }
  ~UdpListenerEcdsTestFilter() override { --live_filters; }

  Network::FilterStatus onData(Network::UdpRecvData& data) override {
    if (!absl::StartsWith(data.buffer_->toString(), "probe|")) {
      return Network::FilterStatus::Continue;
    }
    if (!config_.respond_to_probe()) {
      data.buffer_->add(config_.generation());
      data.buffer_->add("|");
      return Network::FilterStatus::Continue;
    }
    auto& listener = read_callbacks_->udpListener();
    data.buffer_->add(
        fmt::format("{}|{}|{}", listener.dispatcher().name(), id_, fmt::ptr(&listener)));
    Network::UdpSendData response{data.addresses_.local_->ip(), *data.addresses_.peer_,
                                  *data.buffer_};
    EXPECT_TRUE(listener.send(response).ok());
    return Network::FilterStatus::StopIteration;
  }

  Network::FilterStatus onReceiveError(Api::IoError::IoErrorCode) override {
    return Network::FilterStatus::Continue;
  }

private:
  const test::integration::UdpListenerEcdsConfig config_;
  const uint64_t id_;
};

class UdpListenerEcdsTestFilterFactory
    : public Server::Configuration::NamedUdpListenerFilterConfigFactory {
public:
  Network::UdpListenerFilterFactoryCb
  createFilterFactoryFromProto(const Protobuf::Message& proto_config,
                               Server::Configuration::ListenerFactoryContext& context) override {
    const auto config =
        MessageUtil::downcastAndValidate<const test::integration::UdpListenerEcdsConfig&>(
            proto_config, context.messageValidationVisitor());
    if (config.generation() == "reject") {
      throw EnvoyException("rejected UDP ECDS test configuration");
    }
    return [config](Network::UdpListenerFilterManager& manager,
                    Network::UdpReadFilterCallbacks& callbacks) {
      manager.addReadFilter(std::make_unique<UdpListenerEcdsTestFilter>(callbacks, config));
    };
  }
  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<test::integration::UdpListenerEcdsConfig>();
  }
  std::string name() const override { return "test.udp_listener_ecds"; }
};

REGISTER_FACTORY(UdpListenerEcdsTestFilterFactory,
                 Server::Configuration::NamedUdpListenerFilterConfigFactory);

// Keep failed response assertions bounded instead of blocking the integration test on recv().
class UdpTestPeer {
public:
  UdpTestPeer(Network::Address::IpVersion version, Event::Dispatcher& dispatcher)
      : socket_(Network::Test::getCanonicalLoopbackAddress(version), nullptr, true),
        dispatcher_(dispatcher) {}

  void write(const std::string& payload, const Network::Address::Instance& peer) {
    auto result = Network::Utility::writeToSocket(socket_.ioHandle(), Buffer::OwnedImpl(payload),
                                                  nullptr, peer);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(payload.size(), result.return_value_);
  }

  bool recv(Network::UdpRecvData& datagram) {
    bool received = false;
    socket_.ioHandle().initializeFileEvent(
        dispatcher_,
        [&](uint32_t) {
          std::array<char, 4096> buffer;
          Buffer::RawSlice slice{buffer.data(), buffer.size()};
          Network::IoHandle::RecvMsgOutput output(1, nullptr);
          auto result = socket_.ioHandle().recvmsg(
              &slice, 1, socket_.connectionInfoProvider().localAddress()->ip()->port(), {}, output);
          received = result.ok() && !output.msg_[0].truncated_and_dropped_;
          if (received) {
            datagram.addresses_.local_ = socket_.connectionInfoProvider().localAddress();
            datagram.addresses_.peer_ = std::move(output.msg_[0].peer_address_);
            datagram.buffer_ =
                std::make_unique<Buffer::OwnedImpl>(buffer.data(), result.return_value_);
          }
          dispatcher_.exit();
          return absl::OkStatus();
        },
        Event::FileTriggerType::Level, Event::FileReadyType::Read);
    auto timeout = dispatcher_.createTimer([&]() { dispatcher_.exit(); });
    timeout->enableTimer(TestUtility::DefaultTimeout);
    dispatcher_.run(Event::Dispatcher::RunType::Block);
    socket_.ioHandle().resetFileEvents();
    return received;
  }

private:
  Network::UdpListenSocket socket_;
  Event::Dispatcher& dispatcher_;
};

class UdpListenerExtensionDiscoveryIntegrationTest
    : public testing::TestWithParam<Network::Address::IpVersion>,
      public BaseIntegrationTest {
public:
  UdpListenerExtensionDiscoveryIntegrationTest()
      : BaseIntegrationTest(GetParam(), ConfigHelper::baseUdpListenerConfig()),
        dns_counters_(query_buffer_underflow_, record_name_overflow_, query_parsing_failure_,
                      queries_with_additional_rrs_, queries_with_answers_) {
    concurrency_ = 2;
    skip_tag_extraction_rule_check_ = true;
    defer_listener_finalization_ = true;
  }

  void TearDown() override {
    if (ecds_connection_) {
      ASSERT_TRUE(ecds_connection_->close());
      ASSERT_TRUE(ecds_connection_->waitForDisconnect());
      ecds_connection_.reset();
    }
    test_server_.reset();
    EXPECT_EQ(0, live_filters.load());
  }

  void createUpstreams() override {
    BaseIntegrationTest::createUpstreams();
    addFakeUpstream(Http::CodecType::HTTP2);
  }

  void initializeEcds(bool default_config = false, bool short_initial_timeout = false) {
    ASSERT_EQ(0, live_filters.load());
    setUdpFakeUpstream(FakeUpstreamConfig::UdpConfig{});
    config_helper_.addConfigModifier([this, default_config, short_initial_timeout](
                                         envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      auto* ecds_cluster = bootstrap.mutable_static_resources()->add_clusters();
      ecds_cluster->MergeFrom(bootstrap.static_resources().clusters(0));
      ecds_cluster->set_name("ecds_cluster");
      ConfigHelper::setHttp2(*ecds_cluster);

      auto* listener = bootstrap.mutable_static_resources()->mutable_listeners(0);
      listener->set_stat_prefix("raw_udp_ecds");
      listener->mutable_enable_reuse_port()->set_value(true);
      auto* dynamic_filter = listener->add_listener_filters();
      dynamic_filter->set_name("policy");
      auto* discovery = dynamic_filter->mutable_config_discovery();
      discovery->add_type_urls(std::string(FilterType));
      if (default_config) {
        test::integration::UdpListenerEcdsConfig config;
        config.set_generation("default");
        ASSERT_TRUE(discovery->mutable_default_config()->PackFrom(config));
        discovery->set_apply_default_config_without_warming(true);
      }
      auto* source = discovery->mutable_config_source();
      source->set_resource_api_version(envoy::config::core::v3::V3);
      if (short_initial_timeout) {
        source->mutable_initial_fetch_timeout()->set_nanos(100000000);
      }
      auto* api = source->mutable_api_config_source();
      api->set_api_type(envoy::config::core::v3::ApiConfigSource::DELTA_GRPC);
      api->set_transport_api_version(envoy::config::core::v3::V3);
      api->add_grpc_services()->mutable_envoy_grpc()->set_cluster_name("ecds_cluster");

      auto* static_filter = listener->add_listener_filters();
      static_filter->set_name("test.udp_listener_ecds");
      test::integration::UdpListenerEcdsConfig static_config;
      static_config.set_generation("static");
      static_config.set_respond_to_probe(true);
      ASSERT_TRUE(static_filter->mutable_typed_config()->PackFrom(static_config));

      auto* fallback = listener->add_listener_filters();
      fallback->set_name("envoy.filters.udp.dns_filter");
      envoy::extensions::filters::udp::dns_filter::v3::DnsFilterConfig dns;
      dns.set_stat_prefix("fallback");
      auto* table = dns.mutable_server_config()->mutable_inline_dns_table();
      table->set_external_retry_count(0);
      auto* domain = table->add_virtual_domains();
      domain->set_name("local.example");
      domain->mutable_endpoint()->mutable_address_list()->add_address("192.0.2.10");
      auto* client = dns.mutable_client_config();
      client->mutable_resolver_timeout()->set_seconds(30);
      client->set_max_pending_lookups(256);
      auto* resolver = client->mutable_typed_dns_resolver_config();
      resolver->set_name("envoy.network.dns_resolver.cares");
      envoy::extensions::network::dns_resolver::cares::v3::CaresDnsResolverConfig cares;
      Network::Utility::addressToProtobufAddress(*fake_upstreams_[0]->localAddress(),
                                                 *cares.add_resolvers());
      cares.mutable_query_timeout_seconds()->set_value(30);
      cares.mutable_query_tries()->set_value(1);
      ASSERT_TRUE(resolver->mutable_typed_config()->PackFrom(cares));
      ASSERT_TRUE(fallback->mutable_typed_config()->PackFrom(dns));
    });
    on_server_init_function_ = [this]() {
      ASSERT_TRUE(fake_upstreams_[1]->waitForHttpConnection(*dispatcher_, ecds_connection_));
      ASSERT_TRUE(ecds_connection_->waitForNewStream(*dispatcher_, ecds_stream_));
      ecds_stream_->startGrpcStream();
      envoy::service::discovery::v3::DeltaDiscoveryRequest request;
      ASSERT_TRUE(ecds_stream_->waitForGrpcMessage(*dispatcher_, request));
      EXPECT_EQ(ResourceType, request.type_url());
      EXPECT_THAT(request.resource_names_subscribe(), testing::ElementsAre("policy"));
    };
    BaseIntegrationTest::initialize();
    registerTestServerPorts({"listener_0"});
    listener_address_ =
        Network::Utility::resolveUrl(
            fmt::format("tcp://{}:{}", Network::Test::getLoopbackAddressUrlString(GetParam()),
                        lookupPort("listener_0")))
            .value();
  }

  void waitReady() {
    test_server_->waitUntilListenersReady();
    test_server_->waitForCounter("listener_manager.listener_create_success", testing::Eq(2));
    EXPECT_EQ(Init::Manager::State::Initialized, test_server_->server().initManager().state());
  }

  void sendConfig(const std::string& version, const std::string& generation) {
    test::integration::UdpListenerEcdsConfig config;
    config.set_generation(generation);
    sendTypedConfig(version, config);
  }

  void sendTypedConfig(const std::string& version, const Protobuf::Message& config) {
    envoy::service::discovery::v3::DeltaDiscoveryResponse response;
    response.set_type_url(std::string(ResourceType));
    response.set_nonce(version);
    auto* resource = response.add_resources();
    resource->set_name("policy");
    resource->set_version(version);
    envoy::config::core::v3::TypedExtensionConfig extension;
    extension.set_name("policy");
    ASSERT_TRUE(extension.mutable_typed_config()->PackFrom(config));
    ASSERT_TRUE(resource->mutable_resource()->PackFrom(extension));
    ecds_stream_->sendGrpcMessage(response);
  }

  void removeConfig(const std::string& version) {
    envoy::service::discovery::v3::DeltaDiscoveryResponse response;
    response.set_type_url(std::string(ResourceType));
    response.set_nonce(version);
    response.add_removed_resources("policy");
    ecds_stream_->sendGrpcMessage(response);
  }

  void expectAck(const std::string& version, bool accepted = true) {
    envoy::service::discovery::v3::DeltaDiscoveryRequest request;
    ASSERT_TRUE(ecds_stream_->waitForGrpcMessage(*dispatcher_, request));
    EXPECT_EQ(version, request.response_nonce());
    EXPECT_EQ(accepted, request.error_detail().code() == 0) << request.DebugString();
  }

  void waitApplied(uint64_t reloads) {
    // An xDS ACK acknowledges acceptance/queueing. config_reload is the worker-application barrier.
    test_server_->waitForCounter(std::string(ReloadCounter), testing::Eq(reloads));
  }

  std::vector<std::string> probe(UdpTestPeer& client) {
    client.write("probe|", *listener_address_);
    Network::UdpRecvData response;
    if (!client.recv(response)) {
      ADD_FAILURE() << "Timed out waiting for UDP probe response";
      return {};
    }
    return absl::StrSplit(response.buffer_->toString(), '|');
  }

  void findWorkers(const std::string& generation) {
    for (size_t attempt = 0; workers_.size() < concurrency_ && attempt < 64; ++attempt) {
      auto client = std::make_unique<UdpTestPeer>(GetParam(), *dispatcher_);
      const auto fields = probe(*client);
      ASSERT_EQ(5, fields.size());
      EXPECT_EQ("probe", fields[0]);
      EXPECT_EQ(generation, fields[1]);
      if (workers_.find(fields[2]) == workers_.end()) {
        identities_[fields[2]] = fields[3] + "|" + fields[4];
        workers_[fields[2]] = std::move(client);
      }
    }
    ASSERT_EQ(concurrency_, workers_.size()) << "UDP probes did not exercise every worker";
  }

  void expectWorkers(const std::string& generation) {
    ASSERT_EQ(concurrency_, workers_.size());
    for (auto& [worker, client] : workers_) {
      const auto fields = probe(*client);
      ASSERT_EQ(5, fields.size());
      EXPECT_EQ(generation, fields[1]);
      EXPECT_EQ(worker, fields[2]);
      EXPECT_EQ(identities_[worker], fields[3] + "|" + fields[4]);
    }
    EXPECT_EQ(2 * concurrency_, live_filters.load());
    EXPECT_EQ(2, test_server_->counter("listener_manager.listener_create_success")->value());
    registerTestServerPorts({"listener_0"});
    EXPECT_EQ(lookupPort("listener_0"), listener_address_->ip()->port());
  }

  void sendDns(UdpTestPeer& client, const std::string& domain) {
    namespace Dns = Extensions::UdpFilters::DnsFilter;
    client.write(Dns::Utils::buildQueryForDomain(domain, Dns::DNS_RECORD_TYPE_A,
                                                 Dns::DNS_RECORD_CLASS_IN, 123),
                 *listener_address_);
  }

  void expectDns(UdpTestPeer& client, const std::string& address) {
    namespace Dns = Extensions::UdpFilters::DnsFilter;
    Network::UdpRecvData response;
    ASSERT_TRUE(client.recv(response));
    auto parsed = Dns::Utils::DnsResponseValidator::createResponseContext(response, dns_counters_);
    ASSERT_TRUE(parsed->parse_status_);
    ASSERT_EQ(1, parsed->answers_.size());
    EXPECT_EQ(Dns::DNS_RESPONSE_CODE_NO_ERROR, parsed->getQueryResponseCode());
    Dns::Utils::verifyAddress({address}, parsed->answers_.begin()->second);
  }

  void answerExternalDns(Network::UdpRecvData& query) {
    // Preserve c-ares' transaction ID and question, omit its optional EDNS record, and answer A.
    std::string response = query.buffer_->toString();
    ASSERT_GE(response.size(), 12);
    size_t end = 12;
    while (end < response.size() && response[end] != '\0') {
      end += static_cast<unsigned char>(response[end]) + 1;
    }
    end += 5; // Root label and query type/class.
    ASSERT_LE(end, response.size());
    response.resize(end);
    response[2] = '\x81';
    response[3] = '\x80';
    response[6] = '\0';
    response[7] = '\1';
    response[8] = response[9] = response[10] = response[11] = '\0';
    constexpr char answer[] = "\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c\x00\x04\xc0\x00\x02\x7b";
    response.append(answer, sizeof(answer) - 1);
    fake_upstreams_[0]->sendUdpDatagram(response, query.addresses_.peer_);
  }

  void expectMissing(uint64_t drops) {
    UdpTestPeer client(GetParam(), *dispatcher_);
    sendDns(client, "missing.example");
    test_server_->waitForCounter(std::string(MissingCounter), testing::Eq(drops));
    Network::UdpRecvData query;
    EXPECT_FALSE(fake_upstreams_[0]->waitForUdpDatagram(query, std::chrono::milliseconds(50)));
  }

  FakeHttpConnectionPtr ecds_connection_;
  FakeStreamPtr ecds_stream_;
  Network::Address::InstanceConstSharedPtr listener_address_;
  std::map<std::string, std::unique_ptr<UdpTestPeer>> workers_;
  std::map<std::string, std::string> identities_;
  testing::NiceMock<Stats::MockCounter> query_buffer_underflow_;
  testing::NiceMock<Stats::MockCounter> record_name_overflow_;
  testing::NiceMock<Stats::MockCounter> query_parsing_failure_;
  testing::NiceMock<Stats::MockCounter> queries_with_additional_rrs_;
  testing::NiceMock<Stats::MockCounter> queries_with_answers_;
  Extensions::UdpFilters::DnsFilter::DnsParserCounters dns_counters_;
};

INSTANTIATE_TEST_SUITE_P(IpVersions, UdpListenerExtensionDiscoveryIntegrationTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()),
                         TestUtility::ipTestParamsToString);

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, UpdatesPreserveSocketWorkersAndPendingDns) {
  initializeEcds();
  EXPECT_EQ(Init::Manager::State::Initializing, test_server_->server().initManager().state());
  sendConfig("1", "first");
  expectAck("1");
  waitApplied(1);
  waitReady();
  findWorkers("first");

  for (auto& [worker, client] : workers_) {
    sendDns(*client, "local.example");
    expectDns(*client, "192.0.2.10");
  }

  // Leave an external resolution pending on each worker while replacing the dynamic filter.
  std::vector<Network::UdpRecvData> pending(workers_.size());
  size_t index = 0;
  for (auto& [worker, client] : workers_) {
    sendDns(*client, worker + ".external.example");
    ASSERT_TRUE(fake_upstreams_[0]->waitForUdpDatagram(pending[index++]));
  }
  sendConfig("2", "second");
  expectAck("2");
  waitApplied(2);
  expectWorkers("second");
  for (auto& query : pending) {
    answerExternalDns(query);
  }
  for (auto& [worker, client] : workers_) {
    expectDns(*client, "192.0.2.123");
    sendDns(*client, "local.example");
    expectDns(*client, "192.0.2.10");
  }

  // Repeated factory failures must NACK without making the rejected config the retained version.
  sendConfig("bad1", "reject");
  expectAck("bad1", false);
  sendConfig("bad2", "reject");
  expectAck("bad2", false);
  Protobuf::StringValue unexpected_type;
  unexpected_type.set_value("not a UDP filter configuration");
  sendTypedConfig("bad-type", unexpected_type);
  expectAck("bad-type", false);
  test_server_->waitForCounter(std::string(FailureCounter), testing::Eq(3));
  expectWorkers("second");
  sendConfig("3", "third");
  expectAck("3");
  waitApplied(3);
  expectWorkers("third");
}

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, RemovalFailsClosedAndRecovers) {
  initializeEcds();
  sendConfig("1", "first");
  expectAck("1");
  waitApplied(1);
  waitReady();
  findWorkers("first");
  removeConfig("removed");
  expectAck("removed");
  waitApplied(2);
  uint64_t drops = 0;
  for (auto& [worker, client] : workers_) {
    sendDns(*client, "removed.example");
    test_server_->waitForCounter(std::string(MissingCounter), testing::Eq(++drops));
  }
  Network::UdpRecvData unexpected_query;
  EXPECT_FALSE(
      fake_upstreams_[0]->waitForUdpDatagram(unexpected_query, std::chrono::milliseconds(50)));
  sendConfig("2", "recovered");
  expectAck("2");
  waitApplied(3);
  expectWorkers("recovered");
}

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, InitialFailureFailsClosedAndRecovers) {
  initializeEcds();
  sendConfig("bad", "reject");
  expectAck("bad", false);
  waitReady();
  expectMissing(1);
  sendConfig("1", "recovered");
  expectAck("1");
  waitApplied(1);
  findWorkers("recovered");
}

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, InitialFetchTimeoutFailsClosed) {
  initializeEcds(false, true);
  waitReady();
  expectMissing(1);
  sendConfig("1", "recovered");
  expectAck("1");
  waitApplied(1);
  findWorkers("recovered");
}

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, DefaultBeforeDiscoveryAndAfterRemoval) {
  initializeEcds(true);
  waitReady();
  findWorkers("default");
  sendConfig("1", "discovered");
  expectAck("1");
  waitApplied(1);
  expectWorkers("discovered");
  removeConfig("removed");
  expectAck("removed");
  waitApplied(2);
  expectWorkers("default");
}

TEST_P(UdpListenerExtensionDiscoveryIntegrationTest, ShutdownWithPendingDnsAfterAcceptedUpdate) {
  initializeEcds();
  sendConfig("1", "first");
  expectAck("1");
  waitApplied(1);
  waitReady();
  UdpTestPeer client(GetParam(), *dispatcher_);
  sendDns(client, "pending.external.example");
  Network::UdpRecvData pending;
  ASSERT_TRUE(fake_upstreams_[0]->waitForUdpDatagram(pending));
  sendConfig("2", "second");
  // Shut down after acceptance without waiting for worker application. Provider tests cover
  // queued callback teardown deterministically.
  expectAck("2");
  test_server_.reset();
  EXPECT_EQ(0, live_filters.load());
  ASSERT_TRUE(ecds_connection_->waitForDisconnect());
  ecds_connection_.reset();
}

} // namespace
} // namespace Envoy
