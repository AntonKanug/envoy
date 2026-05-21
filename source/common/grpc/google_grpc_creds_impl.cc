#include "source/common/grpc/google_grpc_creds_impl.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "envoy/config/core/v3/grpc_service.pb.h"
#include "envoy/grpc/google_grpc_creds.h"

#include "source/common/config/datasource.h"
#include "source/common/runtime/runtime_features.h"

#include "grpcpp/security/tls_certificate_provider.h"
#include "grpcpp/security/tls_certificate_verifier.h"
#include "openssl/bio.h"
#include "openssl/pem.h"
#include "openssl/x509.h"

namespace Envoy {
namespace Grpc {
namespace {

// Verifier that runs BoringSSL chain validation with X509_V_FLAG_NO_CHECK_TIME
// set, so a chain that would otherwise be rejected only because of an expired
// notBefore/notAfter is accepted. Mirrors the semantics of envoy_grpc's
// allow_expired_certificate option (default_validator.cc:120-122).
class AllowExpiredCertVerifier : public grpc::experimental::ExternalCertificateVerifier {
public:
  explicit AllowExpiredCertVerifier(std::string root_certs)
      : root_certs_(std::move(root_certs)) {}

  bool Verify(grpc::experimental::TlsCustomVerificationCheckRequest* request,
              std::function<void(grpc::Status)> /*callback*/,
              grpc::Status* sync_status) override {
    bssl::UniquePtr<X509_STORE> store(X509_STORE_new());
    if (store == nullptr) {
      *sync_status = grpc::Status(grpc::StatusCode::INTERNAL, "X509_STORE_new failed");
      return true;
    }
    bssl::UniquePtr<BIO> root_bio(
        BIO_new_mem_buf(root_certs_.data(), static_cast<int>(root_certs_.size())));
    while (true) {
      bssl::UniquePtr<X509> ca(PEM_read_bio_X509(root_bio.get(), nullptr, nullptr, nullptr));
      if (ca == nullptr) {
        break;
      }
      X509_STORE_add_cert(store.get(), ca.get());
    }
    X509_STORE_set_flags(store.get(), X509_V_FLAG_NO_CHECK_TIME);

    const grpc::string_ref chain = request->peer_cert_full_chain();
    bssl::UniquePtr<BIO> chain_bio(
        BIO_new_mem_buf(chain.data(), static_cast<int>(chain.size())));
    bssl::UniquePtr<X509> leaf;
    bssl::UniquePtr<STACK_OF(X509)> untrusted(sk_X509_new_null());
    while (true) {
      X509* x = PEM_read_bio_X509(chain_bio.get(), nullptr, nullptr, nullptr);
      if (x == nullptr) {
        break;
      }
      if (leaf == nullptr) {
        leaf.reset(x);
      } else {
        sk_X509_push(untrusted.get(), x);
      }
    }
    if (leaf == nullptr) {
      *sync_status = grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "no peer certificate");
      return true;
    }

    bssl::UniquePtr<X509_STORE_CTX> ctx(X509_STORE_CTX_new());
    if (!X509_STORE_CTX_init(ctx.get(), store.get(), leaf.get(), untrusted.get())) {
      *sync_status = grpc::Status(grpc::StatusCode::INTERNAL, "X509_STORE_CTX_init failed");
      return true;
    }
    if (X509_verify_cert(ctx.get()) != 1) {
      const int err = X509_STORE_CTX_get_error(ctx.get());
      *sync_status =
          grpc::Status(grpc::StatusCode::UNAUTHENTICATED, X509_verify_cert_error_string(err));
    } else {
      *sync_status = grpc::Status::OK;
    }
    return true;
  }

  void Cancel(grpc::experimental::TlsCustomVerificationCheckRequest* /*request*/) override {}

private:
  const std::string root_certs_;
};

} // namespace

std::shared_ptr<grpc::ChannelCredentials> CredsUtility::getChannelCredentials(
    const envoy::config::core::v3::GrpcService::GoogleGrpc& google_grpc, Api::Api& api) {
  if (google_grpc.has_channel_credentials()) {
    switch (google_grpc.channel_credentials().credential_specifier_case()) {
    case envoy::config::core::v3::GrpcService::GoogleGrpc::ChannelCredentials::
        CredentialSpecifierCase::kSslCredentials: {
      const auto& ssl_credentials = google_grpc.channel_credentials().ssl_credentials();
      const auto root_certs = THROW_OR_RETURN_VALUE(
          Config::DataSource::read(ssl_credentials.root_certs(), true, api), std::string);
      const auto private_key = THROW_OR_RETURN_VALUE(
          Config::DataSource::read(ssl_credentials.private_key(), true, api), std::string);
      const auto cert_chain = THROW_OR_RETURN_VALUE(
          Config::DataSource::read(ssl_credentials.cert_chain(), true, api), std::string);
      grpc::experimental::TlsChannelCredentialsOptions options;
      if (!private_key.empty() || !cert_chain.empty()) {
        options.set_certificate_provider(
            std::make_shared<grpc::experimental::StaticDataCertificateProvider>(
                root_certs,
                std::vector<grpc::experimental::IdentityKeyCertPair>{{private_key, cert_chain}}));
      } else if (!root_certs.empty()) {
        options.set_certificate_provider(
            std::make_shared<grpc::experimental::StaticDataCertificateProvider>(root_certs));
      }
      if (!root_certs.empty()) {
        options.watch_root_certs();
      }
      if (!private_key.empty() || !cert_chain.empty()) {
        options.watch_identity_key_cert_pairs();
      }
      if (Runtime::runtimeFeatureEnabled("envoy.reloadable_features.google_grpc_disable_tls_13")) {
        options.set_max_tls_version(grpc_tls_version::TLS1_2);
      }
      if (ssl_credentials.allow_expired_certificate() && !root_certs.empty()) {
        // Skip gRPC core's built-in verification (which always enforces validity
        // dates) and replace it with our own verifier that runs BoringSSL chain
        // validation with X509_V_FLAG_NO_CHECK_TIME.
        options.set_verify_server_certs(false);
        options.set_certificate_verifier(
            grpc::experimental::ExternalCertificateVerifier::Create<AllowExpiredCertVerifier>(
                root_certs));
      }
      return grpc::experimental::TlsCredentials(options);
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::ChannelCredentials::
        CredentialSpecifierCase::kLocalCredentials: {
      return grpc::experimental::LocalCredentials(UDS);
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::ChannelCredentials::
        CredentialSpecifierCase::kGoogleDefault: {
      return grpc::GoogleDefaultCredentials();
    }
    default:
      return nullptr;
    }
  }
  return nullptr;
}

std::shared_ptr<grpc::ChannelCredentials> CredsUtility::defaultSslChannelCredentials(
    const envoy::config::core::v3::GrpcService& grpc_service_config, Api::Api& api) {
  auto creds = getChannelCredentials(grpc_service_config.google_grpc(), api);
  if (creds != nullptr) {
    return creds;
  }
  grpc::experimental::TlsChannelCredentialsOptions options;
  if (Runtime::runtimeFeatureEnabled("envoy.reloadable_features.google_grpc_disable_tls_13")) {
    options.set_max_tls_version(grpc_tls_version::TLS1_2);
  }
  return grpc::experimental::TlsCredentials(options);
}

std::vector<std::shared_ptr<grpc::CallCredentials>>
CredsUtility::callCredentials(const envoy::config::core::v3::GrpcService::GoogleGrpc& google_grpc) {
  std::vector<std::shared_ptr<grpc::CallCredentials>> creds;
  for (const auto& credential : google_grpc.call_credentials()) {
    std::shared_ptr<grpc::CallCredentials> new_call_creds;
    switch (credential.credential_specifier_case()) {
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kAccessToken: {
      new_call_creds = grpc::AccessTokenCredentials(credential.access_token());
      break;
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kGoogleComputeEngine: {
      new_call_creds = grpc::GoogleComputeEngineCredentials();
      break;
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kGoogleRefreshToken: {
      new_call_creds = grpc::GoogleRefreshTokenCredentials(credential.google_refresh_token());
      break;
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kServiceAccountJwtAccess: {
      new_call_creds = grpc::ServiceAccountJWTAccessCredentials(
          credential.service_account_jwt_access().json_key(),
          credential.service_account_jwt_access().token_lifetime_seconds());
      break;
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kGoogleIam: {
      new_call_creds = grpc::GoogleIAMCredentials(credential.google_iam().authorization_token(),
                                                  credential.google_iam().authority_selector());
      break;
    }
    case envoy::config::core::v3::GrpcService::GoogleGrpc::CallCredentials::
        CredentialSpecifierCase::kStsService: {
      grpc::experimental::StsCredentialsOptions options = {
          credential.sts_service().token_exchange_service_uri(),
          credential.sts_service().resource(),
          credential.sts_service().audience(),
          credential.sts_service().scope(),
          credential.sts_service().requested_token_type(),
          credential.sts_service().subject_token_path(),
          credential.sts_service().subject_token_type(),
          credential.sts_service().actor_token_path(),
          credential.sts_service().actor_token_type(),
      };
      new_call_creds = grpc::experimental::StsCredentials(options);
      break;
    }
    default:
      // We don't handle plugin credentials here, callers can do so instead if they want.
      continue;
    }
    // Any of the above creds creation can fail, if they do they return nullptr
    // and we ignore them.
    if (new_call_creds != nullptr) {
      creds.emplace_back(new_call_creds);
    }
  }
  return creds;
}

std::shared_ptr<grpc::ChannelCredentials> CredsUtility::defaultChannelCredentials(
    const envoy::config::core::v3::GrpcService& grpc_service_config, Api::Api& api) {
  std::shared_ptr<grpc::ChannelCredentials> channel_creds =
      getChannelCredentials(grpc_service_config.google_grpc(), api);
  if (channel_creds == nullptr) {
    channel_creds = grpc::InsecureChannelCredentials();
  }
  auto call_creds_vec = callCredentials(grpc_service_config.google_grpc());
  if (call_creds_vec.empty()) {
    return channel_creds;
  }
  std::shared_ptr<grpc::CallCredentials> call_creds = call_creds_vec[0];
  for (uint32_t i = 1; i < call_creds_vec.size(); ++i) {
    call_creds = grpc::CompositeCallCredentials(call_creds, call_creds_vec[i]);
  }
  return grpc::CompositeChannelCredentials(channel_creds, call_creds);
}

/**
 * Default implementation of Google Grpc Credentials Factory
 * Uses ssl creds if available, or defaults to insecure channel.
 *
 * This is not the same as google_default credentials. This is the default implementation that is
 * loaded if no other implementation is configured.
 */
class DefaultGoogleGrpcCredentialsFactory : public GoogleGrpcCredentialsFactory {

public:
  std::shared_ptr<grpc::ChannelCredentials>
  getChannelCredentials(const envoy::config::core::v3::GrpcService& grpc_service_config,
                        Server::Configuration::CommonFactoryContext& context) override {
    return CredsUtility::defaultChannelCredentials(grpc_service_config, context.api());
  }

  std::string name() const override { return "envoy.grpc_credentials.default"; }
};

/**
 * Static registration for the default Google gRPC credentials factory. @see RegisterFactory.
 */
REGISTER_FACTORY(DefaultGoogleGrpcCredentialsFactory, GoogleGrpcCredentialsFactory);

} // namespace Grpc
} // namespace Envoy
