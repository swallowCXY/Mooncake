#include "client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>

#include "config.h"
#include "transfer_engine.h"
#include "types.h"
#include "centralized_client_service.h"
#include "p2p_client_service.h"
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

// ============================================================================
// Static helpers
// ============================================================================

void ClientService::initTeEndpoint() {
    if (metadata_connstring_ == P2PHANDSHAKE) {
        te_endpoint_ = transfer_engine_->getLocalIpAndPort();
    } else {
        te_endpoint_ = local_endpoint();
    }
}

size_t ClientService::CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

size_t ClientService::CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

// ============================================================================
// Construction / Destruction
// ============================================================================

ClientService::ClientService(const std::string& local_ip, uint16_t te_port,
                             const std::string& metadata_connstring,
                             uint16_t metrics_port, bool enable_metrics_http,
                             const std::map<std::string, std::string>& labels)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      local_ip_(local_ip),
      te_port_(te_port),
      metadata_connstring_(metadata_connstring),
      enable_metrics_http_(enable_metrics_http) {
    LOG(INFO) << "client_id=" << client_id_;

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }

    metrics_port_ = StartMetricsHttpServer(enable_metrics_http, metrics_port);
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const CentralizedClientConfig& config) {
    auto client = std::make_shared<CentralizedClientService>(
        config.local_ip, config.te_port, config.metadata_connstring,
        config.metrics_port, config.enable_metrics_http, config.labels);

    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize centralized client service"
                   << ", ret = " << err;
        return std::nullopt;
    }

    return client;
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const P2PClientConfig& config) {
    auto client = std::make_shared<P2PClientService>(
        config.local_ip, config.te_port, config.metadata_connstring,
        config.metrics_port, config.enable_metrics_http, config.labels);

    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize P2P client service"
                   << ", ret = " << err;
        return std::nullopt;
    }

    return client;
}

ClientService::~ClientService() {
    Stop();
    Destroy();
}

void ClientService::Stop() {
    StopMetricsHttpServer();
    StopHeartbeat();  // virtual: dispatches to subclass keepalive stop
}

void ClientService::Destroy() {
    // Free global segment memory
    segment_ptrs_.clear();
    ascend_segment_ptrs_.clear();
}

// ============================================================================
// Master connection (non-HA direct connect; HA handled by P2P subclass)
// ============================================================================

static std::optional<bool> get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        int iv = std::stoi(ev_ad);
        if (iv == 1) {
            LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
            return true;
        } else if (iv == 0) {
            LOG(INFO) << "auto discovery not set by env MC_MS_AUTO_DISC";
            return false;
        } else {
            LOG(WARNING)
                << "invalid MC_MS_AUTO_DISC value: " << ev_ad
                << ", should be 0 or 1, using default: auto discovery not set";
        }
    }
    return std::nullopt;
}

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

static std::vector<std::string> get_auto_discover_filters() {
    std::vector<std::string> whitelst_filters;
    char* ev_ad = std::getenv("MC_MS_FILTERS");
    if (ev_ad) {
        LOG(INFO) << "whitelist filters: " << ev_ad;
        char delimiter = ',';
        char* end = ev_ad + std::strlen(ev_ad);
        char *start = ev_ad, *pos = ev_ad;
        while ((pos = std::find(start, end, delimiter)) != end) {
            std::string str(start, pos);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
            start = pos + 1;
        }
        if (start != (end + 1)) {
            std::string str(start, end);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
        }
    }
    return whitelst_filters;
}

tl::expected<void, ErrorCode> ClientService::CheckRegisterMemoryParams(
    const void* addr, size_t length) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto max_mr_size = globalConfig().max_mr_size;
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode ClientService::ConnectToMaster(
    const std::string& master_server_entry) {
    // Non-HA direct connect. HA/etcd view resolution is handled by the P2P
    // subclass (which overrides keepalive + owns master_view_helper_).
    auto err = GetMasterClient().Connect(master_server_entry);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master";
        return err;
    }
    return ErrorCode::OK;
}

ErrorCode ClientService::InitTransferEngine(
    const std::string& endpoint, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    if (!transfer_engine_) {
        LOG(ERROR) << "Transfer engine pointer is null";
        return ErrorCode::INVALID_PARAMS;
    }

    std::optional<bool> env_auto_discover = get_auto_discover();
    bool auto_discover = false;
    if (env_auto_discover.has_value()) {
        auto_discover = env_auto_discover.value();
    } else {
        if (protocol == "rdma" && !device_names.has_value()) {
            LOG(INFO) << "Set auto discovery ON by default for RDMA protocol, "
                         "since no "
                         "device names provided";
            auto_discover = true;
        }
    }
    transfer_engine_->setAutoDiscover(auto_discover);

    if (auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is enabled for protocol: "
                  << protocol;
        auto filters = get_auto_discover_filters();
        transfer_engine_->setWhitelistFilters(std::move(filters));
    } else {
        const char* env_filters = std::getenv("MC_MS_FILTERS");
        if (env_filters && *env_filters != '\0') {
            LOG(WARNING)
                << "MC_MS_FILTERS is set but auto discovery is disabled; "
                << "ignoring whitelist: " << env_filters;
        }
    }

    auto [hostname, port] = parseHostNameWithPort(endpoint);
    int rc =
        transfer_engine_->init(metadata_connstring, endpoint, hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (!auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is disabled for protocol: "
                  << protocol;

        Transport* transport = nullptr;

        if (protocol == "rdma") {
            if (!device_names.has_value() || device_names.value().empty()) {
                LOG(ERROR) << "RDMA protocol requires device names when auto "
                              "discovery is disabled";
                return ErrorCode::INVALID_PARAMS;
            }

            LOG(INFO) << "Using specified RDMA devices: "
                      << device_names.value();

            std::vector<std::string> devices =
                splitString(device_names.value(), ',', /*skip_empty=*/true);

            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }

            transport = transfer_engine_->installTransport("rdma", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install RDMA transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "tcp") {
            if (device_names.has_value()) {
                LOG(WARNING)
                    << "TCP protocol does not use device names, ignoring";
            }

            try {
                transport = transfer_engine_->installTransport("tcp", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ascend") {
            if (device_names.has_value()) {
                LOG(WARNING) << "Ascend protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport =
                    transfer_engine_->installTransport("ascend", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "ascend_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install Ascend transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

// ============================================================================
// Common query helpers (dispatch via GetMasterClient interface)
// ============================================================================

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
ClientService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().BatchQueryIp(client_ids);
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
ClientService::QueryByRegex(const std::string& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().GetReplicaListByRegex(str);
}

tl::expected<void, ErrorCode> ClientService::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> ClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// ============================================================================
// Metrics HTTP server
// ============================================================================

uint16_t ClientService::StartMetricsHttpServer(bool enable_metrics_http,
                                               uint16_t metrics_port) {
    if (!enable_metrics_http) {
        LOG(INFO) << "Client metrics HTTP server disabled";
        return 0;
    }

    if (!metrics_) {
        LOG(INFO) << "Client metrics disabled, skipping HTTP server start";
        return 0;
    }

    try {
        metrics_http_server_ =
            std::make_unique<coro_http::coro_http_server>(1, metrics_port);

        using namespace coro_http;

        metrics_http_server_->set_http_handler<GET>(
            "/metrics",
            [this](coro_http_request& req, coro_http_response& resp) {
                if (!metrics_) {
                    resp.set_status_and_content(
                        status_type::service_unavailable,
                        "Metrics not available");
                    return;
                }
                std::string metrics_str;
                metrics_->serialize(metrics_str);
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok,
                                            std::move(metrics_str));
            });

        metrics_http_server_->set_http_handler<GET>(
            "/metrics/summary",
            [this](coro_http_request& req, coro_http_response& resp) {
                if (!metrics_) {
                    resp.set_status_and_content(
                        status_type::service_unavailable,
                        "Metrics not available");
                    return;
                }
                std::string summary = metrics_->summary_metrics();
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok,
                                            std::move(summary));
            });

        metrics_http_server_->set_http_handler<GET>(
            "/health",
            [this](coro_http_request& req, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, GetHealthStatus());
            });

        metrics_http_server_->async_start();
        uint16_t actual_port = metrics_http_server_->port();
        LOG(INFO) << "Client metrics HTTP server started on port "
                  << actual_port;
        return actual_port;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start client metrics HTTP server: "
                   << e.what();
        return 0;
    }
}

void ClientService::StopMetricsHttpServer() {
    if (metrics_http_server_) {
        LOG(INFO) << "Stopping client metrics HTTP server on port "
                  << metrics_port_;
        metrics_http_server_->stop();
        metrics_http_server_.reset();
    }
}

}  // namespace mooncake
