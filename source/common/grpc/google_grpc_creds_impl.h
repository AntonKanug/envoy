#pragma once

#include "envoy/api/api.h"
#include "envoy/common/platform.h"
#include "envoy/config/core/v3/grpc_service.pb.h"
#include "envoy/registry/registry.h"

#include "grpcpp/grpcpp.h"

namespace Envoy {
namespace Grpc {

grpc::SslCredentialsOptions buildSslOptionsFromConfig(
    const envoy::config::core::v3::GrpcService::GoogleGrpc::SslCredentials& ssl_config);

std::shared_ptr<grpc::ChannelCredentials>
getGoogleGrpcChannelCredentials(const envoy::config::core::v3::GrpcService& grpc_service,
                                Api::Api& api);

class CredsUtility {
public:
  /**
   * Translation from envoy::config::core::v3::GrpcService::GoogleGrpc to grpc::ChannelCredentials
   * for channel credentials.
   * @param google_grpc Google gRPC config.
   * @param api reference to the Api object
   * @return std::shared_ptr<grpc::ChannelCredentials> channel credentials. A nullptr
   *         will be returned in the absence of any configured credentials.
   */
  static std::shared_ptr<grpc::ChannelCredentials>
  getChannelCredentials(const envoy::config::core::v3::GrpcService::GoogleGrpc& google_grpc,
                        Api::Api& api);

  /**
   * Static translation from envoy::config::core::v3::GrpcService::GoogleGrpc to a vector of
   * grpc::CallCredentials. Any plugin based call credentials will be elided.
   * @param grpc_service Google gRPC config.
   * @return std::vector<std::shared_ptr<grpc::CallCredentials>> call credentials.
   */
  static std::vector<std::shared_ptr<grpc::CallCredentials>>
  callCredentials(const envoy::config::core::v3::GrpcService::GoogleGrpc& google_grpc);

  /**
   * Default translation from envoy::config::core::v3::GrpcService::GoogleGrpc to
   * grpc::ChannelCredentials for SSL channel credentials.
   * @param grpc_service_config gRPC service config.
   * @param api reference to the Api object
   * @return std::shared_ptr<grpc::ChannelCredentials> SSL channel credentials. Empty SSL
   *         credentials will be set in the absence of any configured SSL in grpc_service_config,
   *         forcing the channel to SSL.
   */
  static std::shared_ptr<grpc::ChannelCredentials>
  defaultSslChannelCredentials(const envoy::config::core::v3::GrpcService& grpc_service_config,
                               Api::Api& api);

  /**
   * Default static translation from envoy::config::core::v3::GrpcService::GoogleGrpc to
   * grpc::ChannelCredentials for all non-plugin based channel and call credentials.
   * @param grpc_service_config gRPC service config.
   * @param api reference to the Api object
   * @return std::shared_ptr<grpc::ChannelCredentials> composite channel and call credentials.
   *         will be set in the absence of any configured SSL in grpc_service_config, forcing the
   *         channel to SSL.
   */
  static std::shared_ptr<grpc::ChannelCredentials>
  defaultChannelCredentials(const envoy::config::core::v3::GrpcService& grpc_service_config,
                            Api::Api& api);

  /**
   * Removes every CA in `pem_bundle` whose ``notAfter`` is in the past, returning the
   * remaining certificates as a PEM string. Certificates that fail to parse are also
   * skipped. Used by `getChannelCredentials` to drop expired duplicates of renewed
   * roots from a trust bundle before handing it to gRPC, so that chain building does
   * not pick the dead anchor by accident.
   * @param pem_bundle PEM-encoded concatenation of zero or more X.509 certificates.
   * @return PEM-encoded subset containing only certs whose ``notAfter`` is in the future.
   */
  static std::string filterExpiredRoots(const std::string& pem_bundle);
};
DECLARE_FACTORY(DefaultGoogleGrpcCredentialsFactory);

} // namespace Grpc
} // namespace Envoy
