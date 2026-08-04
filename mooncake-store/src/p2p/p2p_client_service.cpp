#include "p2p_client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <exception>
#include <future>
#include <optional>
#include <thread>

#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include "config.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

// ============================================================================
// Static helpers (absorbed from former base)
// ============================================================================

void P2PClientService::initTeEndpoint() {
    // For P2PHANDSHAKE the TE picks a random port and updates
    // local_server_name_ accordingly, so getLocalIpAndPort() is authoritative.
    if (metadata_connstring_ == P2PHANDSHAKE) {
        te_endpoint_ = transfer_engine_->getLocalIpAndPort();
    } else {
        te_endpoint_ = local_endpoint();
    }
}

size_t P2PClientService::CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

size_t P2PClientService::CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

// ============================================================================
// Construction / Destruction
// ============================================================================

P2PClientService::P2PClientService(
    const std::string& local_ip, uint16_t te_port,
    const std::string& metadata_connstring, uint16_t metrics_port,
    bool enable_metrics_http, const std::map<std::string, std::string>& labels)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      local_ip_(local_ip),
      te_port_(te_port),
      metadata_connstring_(metadata_connstring),
      enable_metrics_http_(enable_metrics_http),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr) {
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

std::optional<std::shared_ptr<P2PClientService>>
P2PClientService::Create(const P2PClientConfig& config) {
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

P2PClientService::~P2PClientService() {
    Stop();
    Destroy();
}

void P2PClientService::Stop() {
    if (!MarkShuttingDown()) {
        return;  // Already shut down.
    }

    LOG(INFO) << "P2PClientService::Stop() — begin";

    // Stop HA recovery thread first
    if (ha_manager_) {
        ha_manager_->Stop();
    }

    // Stop RPC server so no new requests arrive.
    if (client_rpc_server_) {
        client_rpc_server_->stop();
    }
    if (client_rpc_server_thread_.joinable()) {
        client_rpc_server_thread_.join();
    }

    // Stop async notifier before data_manager to drain pending ops
    if (async_route_notifier_) {
        async_route_notifier_->Stop();
    }

    // Stop tier scheduler of tierd_backend
    if (data_manager_.has_value()) {
        data_manager_->Stop();
    }

    // Stop heartbeat + metrics HTTP (former base Stop())
    StopMetricsHttpServer();
    StopHeartbeat();

    LOG(INFO) << "P2PClientService::Stop() — complete";
}

void P2PClientService::StopHeartbeat() {
    if (heartbeat_running_) {
        {
            std::lock_guard<std::mutex> lock(heartbeat_mtx_);
            heartbeat_running_ = false;
        }
        heartbeat_cv_.notify_all();
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
    }
}

void P2PClientService::Destroy() {
    LOG(INFO) << "P2PClientService::Destroy() — begin";

    {
        std::lock_guard<std::mutex> lock(peer_clients_mutex_);
        peer_clients_.clear();
    }

    client_rpc_service_.reset();
    ha_manager_.reset();
    async_route_notifier_.reset();
    if (data_manager_.has_value()) {
        data_manager_->Destroy();
    }
    data_manager_.reset();

    // Free global segment memory (former base Destroy())
    segment_ptrs_.clear();
    ascend_segment_ptrs_.clear();

    LOG(INFO) << "P2PClientService::Destroy() — complete";
}

// ============================================================================
// Master connection / TransferEngine init (former base)
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

tl::expected<void, ErrorCode> P2PClientService::CheckRegisterMemoryParams(
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

ErrorCode P2PClientService::ConnectToMaster(
    const std::string& master_server_entry) {
    if (master_server_entry.find("etcd://") == 0) {
        std::string etcd_entry = master_server_entry.substr(strlen("etcd://"));

        auto err = master_view_helper_.ConnectToEtcd(etcd_entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd";
            return err;
        }
        std::string master_address;
        ViewVersionId master_version = 0;
        err = master_view_helper_.GetMasterView(master_address, master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address";
            return err;
        }

        err = GetMasterClient().Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        return ErrorCode::OK;
    } else {
        auto err = GetMasterClient().Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        return ErrorCode::OK;
    }
}

ErrorCode P2PClientService::InitTransferEngine(
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
// Common query helpers (former base)
// ============================================================================

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
P2PClientService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().BatchQueryIp(client_ids);
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
P2PClientService::QueryByRegex(const std::string& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().GetReplicaListByRegex(str);
}

tl::expected<void, ErrorCode> P2PClientService::RegisterLocalMemory(
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

tl::expected<void, ErrorCode> P2PClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// ============================================================================
// Heartbeat (former base)
// ============================================================================

void P2PClientService::StartHeartbeat(const std::string& master_server_entry) {
    if (heartbeat_running_) {
        LOG(WARNING) << "Heartbeat thread already running, skip starting";
        return;
    }

    bool is_ha_mode = (master_server_entry.find("etcd://") == 0);
    std::string current_master_address;

    if (is_ha_mode) {
        ViewVersionId master_version = 0;
        auto err = master_view_helper_.GetMasterView(current_master_address,
                                                     master_version);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to get master address from etcd, "
                         << "starting heartbeat thread in degraded mode "
                         << "(will retry): " << err;
        }
    } else {
        current_master_address = master_server_entry;
    }

    heartbeat_running_ = true;
    heartbeat_thread_ = std::thread([this, is_ha_mode, current_master_address,
                                     master_server_entry]() mutable {
        this->HeartbeatThreadMain(is_ha_mode, std::move(current_master_address),
                                  master_server_entry);
    });
}

void P2PClientService::HeartbeatThreadMain(
    bool is_ha_mode, std::string current_master_address,
    const std::string& master_server_entry) {
    const int max_heartbeat_fail_count = 10;
    const int success_heartbeat_interval_ms = 1000;
    const int fail_heartbeat_interval_ms = 1000;
    int heartbeat_fail_count = 0;

    auto register_client = [this]() {
        LOG(INFO) << "Sending RegisterClientRequest"
                  << ", client_id=" << client_id_;
        auto res = RegisterClient();
        if (!res) {
            LOG(ERROR) << "Failed to register client"
                       << ", client_id=" << client_id_
                       << ", error=" << res.error();
        } else {
            LOG(INFO) << "Client registered successfully"
                      << ", client_id=" << client_id_
                      << ", view_version=" << res.value().view_version;
            OnHAEvent(HAEvent::MASTER_RECONNECTED);
        }
    };
    std::future<void> register_client_future;

    while (heartbeat_running_) {
        if (register_client_future.valid() &&
            register_client_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            register_client_future = std::future<void>();
        }

        HeartbeatRequest req = build_heartbeat_request();
        auto heartbeat_result = GetMasterClient().Heartbeat(req);
        if (heartbeat_result) {
            heartbeat_fail_count = 0;
            HandleHeartbeatResponse(heartbeat_result.value(),
                                    current_master_address, register_client,
                                    register_client_future);
            WaitForNextHeartbeat(success_heartbeat_interval_ms);
        } else {
            heartbeat_fail_count++;
            if (heartbeat_fail_count < max_heartbeat_fail_count) {
                LOG(ERROR) << "Failed to send heartbeat to master";
            } else {
                if (!connection_interrupted_) {
                    OnHAEvent(HAEvent::MASTER_UNREACHABLE);
                    connection_interrupted_ = true;
                }
                if (ReconnectToMaster(is_ha_mode, current_master_address)) {
                    heartbeat_fail_count = 0;
                    continue;
                }
            }
            WaitForNextHeartbeat(fail_heartbeat_interval_ms);
        }
    }

    if (register_client_future.valid()) {
        register_client_future.wait();
    }
}

void P2PClientService::WaitForNextHeartbeat(int interval_ms) {
    std::unique_lock<std::mutex> lock{heartbeat_mtx_};
    heartbeat_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                           [this] { return !heartbeat_running_; });
}

bool P2PClientService::HandleHeartbeatResponse(
    const HeartbeatResponse& response,
    const std::string& current_master_address,
    const std::function<void()>& register_client,
    std::future<void>& register_client_future) {
    if (response.view_version != view_version_.load()) {
        LOG(WARNING) << "Master view_version changed"
                     << ", client status in master: " << (int)response.status
                     << ", master address: " << current_master_address
                     << ", Master version: " << response.view_version
                     << ", Client version: " << view_version_.load();
    }
    for (auto& task_result : response.task_results) {
        HandleHeartbeatTaskResult(task_result);
    }
    if (response.status == P2PClientStatus::HEALTH) {
        if (connection_interrupted_) {
            OnHAEvent(HAEvent::MASTER_RECONNECTED);
            connection_interrupted_ = false;
        }
    } else if (response.status == P2PClientStatus::UNDEFINED &&
               !register_client_future.valid()) {
        register_client_future =
            std::async(std::launch::async, register_client);
    }
    return true;
}

void P2PClientService::HandleHeartbeatTaskResult(
    const HeartbeatTaskResult& task_result) {
    if (task_result.error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to process task"
                   << ", task_type=" << (int)task_result.type
                   << ", error=" << toString(task_result.error);
    }

    if (std::holds_alternative<SyncSegmentMetaResult>(task_result.detail)) {
        auto& sync_res = std::get<SyncSegmentMetaResult>(task_result.detail);
        for (auto& sub : sync_res.sub_results) {
            if (sub.error != ErrorCode::OK) {
                LOG(WARNING) << "Failed to sync segment usage"
                             << ", segment_id=" << sub.segment_id
                             << ", error=" << toString(sub.error);
            }
        }
    }
}

bool P2PClientService::ReconnectToMaster(bool is_ha_mode,
                                          std::string& current_master_address) {
    if (is_ha_mode) {
        LOG(ERROR) << "Heartbeat failure threshold exceeded;"
                   << " fetching latest master view and reconnecting";
        std::string master_address;
        ViewVersionId next_version = 0;
        auto err =
            master_view_helper_.GetMasterView(master_address, next_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get new master view: " << toString(err);
            return false;
        }

        err = GetMasterClient().Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master " << master_address
                       << ": " << toString(err);
            return false;
        }

        current_master_address = master_address;
        LOG(INFO) << "Reconnected to master " << master_address;
        return true;
    } else {
        LOG(ERROR) << "Heartbeat failure threshold exceeded (non-HA);"
                   << " reconnecting to " << current_master_address;
        auto err = GetMasterClient().Connect(current_master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Reconnect failed to " << current_master_address
                       << ": " << toString(err);
            return false;
        }
        LOG(INFO) << "Reconnected to master " << current_master_address;
        return true;
    }
}

// ============================================================================
// Metrics HTTP server (former base)
// ============================================================================

uint16_t P2PClientService::StartMetricsHttpServer(bool enable_metrics_http,
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

void P2PClientService::StopMetricsHttpServer() {
    if (metrics_http_server_) {
        LOG(INFO) << "Stopping client metrics HTTP server on port "
                  << metrics_port_;
        metrics_http_server_->stop();
        metrics_http_server_.reset();
    }
}

// ============================================================================
// P2P Init / Storage
// ============================================================================

ErrorCode P2PClientService::Init(const P2PClientConfig& config) {
    client_rpc_port_ = config.client_rpc_port;

    bool master_connected = false;
    ErrorCode err = ConnectToMaster(config.master_server_entry);
    if (err == ErrorCode::OK) {
        master_connected = true;
        LOG(INFO) << "Connected to master successfully";
    } else {
        LOG(WARNING)
            << "Failed to connect to master, starting in DEGRADED mode: "
            << err;
    }

    bool client_registered = false;
    if (master_connected) {
        auto reg = RegisterClient();
        if (reg) {
            client_registered = true;
            LOG(INFO) << "Registered with master successfully";
        } else {
            LOG(WARNING) << "Failed to register with master: " << reg.error()
                         << ", starting in DEGRADED mode";
        }
    }

    HAClientState initial_state =
        client_registered ? HAClientState::FULL : HAClientState::DEGRADED;
    ha_manager_ = std::make_unique<HARecoveryManager>(
        client_id_, master_client_, data_manager_, async_route_notifier_,
        view_version_, initial_state);

    StartHeartbeat(config.master_server_entry);

    if (config.transfer_engine == nullptr) {
        transfer_engine_ = std::make_shared<TransferEngine>();
        err = InitTransferEngine(local_endpoint(), metadata_connstring_,
                                 config.protocol, config.rdma_devices);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return err;
        }
    } else {
        transfer_engine_ = config.transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }
    initTeEndpoint();

    err = InitStorage(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize TieredBackend";
        return err;
    }

    if (config.async_sender_thread_count > 0) {
        SyncFailureCallback failure_cb = [this](const std::string& key,
                                                const UUID& segment_id,
                                                ErrorCode error) {
            LOG(WARNING) << "Async ADD rejected by Master, deleting local"
                         << ", key=" << key << ", error=" << error;
            if (data_manager_.has_value()) {
                auto r = data_manager_->Delete(key, segment_id);
                if (!r) {
                    LOG(ERROR) << "Failed to delete local replica"
                               << ", key=" << key << ", error=" << r.error();
                }
            }
        };
        async_route_notifier_ = std::make_unique<AsyncMetadataNotifier>(
            master_client_, client_id_, config.async_sender_thread_count,
            config.async_max_batch_size, config.async_route_queue_size,
            std::move(failure_cb));
        async_route_notifier_->Start();
        LOG(INFO) << "Async route notifier enabled, thread_count="
                  << config.async_sender_thread_count
                  << ", queue_size=" << config.async_route_queue_size;
    }

    client_rpc_service_.emplace(*data_manager_);
    client_rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
        config.rpc_thread_num, client_rpc_port_);
    RegisterClientRpcService(*client_rpc_server_, *client_rpc_service_);

    client_rpc_server_thread_ = std::thread([this]() {
        auto ec = client_rpc_server_->start();
        if (ec) {
            LOG(ERROR) << "P2P RPC server failed to start on port "
                       << client_rpc_port_ << ": " << ec.message();
        }
    });

    is_running_ = true;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    LOG(INFO) << "P2P RPC server started on port " << client_rpc_port_;

    if (!ha_manager_->IsDegraded()) {
        ha_manager_->SetSyncCompleted();
    } else {
        LOG(INFO) << "P2P client started in DEGRADED mode, heartbeat will "
                  << "establish master connection when available";
    }

    ha_manager_->SetReadyForRecovery();

    return ErrorCode::OK;
}

ErrorCode P2PClientService::InitStorage(const P2PClientConfig& config) {
    auto tiered_backend = std::make_unique<TieredBackend>();

    auto add_replica_callback = BuildAddReplicaCallback();
    auto remove_replica_callback = BuildRemoveReplicaCallback();
    auto segment_sync_callback = BuildSegmentSyncCallback();

    auto init_result = tiered_backend->Init(
        config.tiered_backend_config, transfer_engine_.get(),
        add_replica_callback, remove_replica_callback, segment_sync_callback);
    if (!init_result) {
        LOG(ERROR) << "Failed to init TieredBackend: " << init_result.error();
        return init_result.error();
    }

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = config.local_transfer_mode;
    if (config.local_transfer_mode == LocalTransferMode::TE) {
        local_transfer_config.te_endpoint = get_te_endpoint();
    } else {
        local_transfer_config.local_memcpy_async_worker_num =
            config.local_memcpy_async_worker_num;
    }

    data_manager_ = DataManager(std::move(tiered_backend), transfer_engine_,
                                config.lock_shard_count, local_transfer_config);
    data_manager_->SetRectifyCallback([this](const std::string& key,
                                             std::optional<UUID> tier_id) {
        if (async_route_notifier_) {
            if (!tier_id.has_value()) {
                auto tier_views = data_manager_->GetTierViews();
                for (const auto& tv : tier_views) {
                    auto r = async_route_notifier_->EnqueueRemove(key, tv.id);
                    if (!r) {
                        LOG(WARNING) << "Failed to enqueue rectify remove"
                                     << ", key=" << key;
                    }
                }
            } else {
                auto r = async_route_notifier_->EnqueueRemove(key, *tier_id);
                if (!r) {
                    LOG(WARNING) << "Failed to enqueue rectify remove"
                                 << ", key=" << key;
                }
            }
        } else {
            if (!tier_id.has_value()) {
                auto tier_views = data_manager_->GetTierViews();
                std::vector<UUID> segment_ids;
                segment_ids.reserve(tier_views.size());
                for (const auto& tv : tier_views) {
                    segment_ids.push_back(tv.id);
                }
                SyncBatchRemoveReplica(key, std::move(segment_ids));
            } else {
                SyncRemoveReplica(key, *tier_id);
            }
        }
    });

    if (config.route_cache_max_memory_bytes > 0 &&
        config.route_cache_ttl_ms > 0) {
        route_cache_.emplace(config.route_cache_max_memory_bytes,
                             config.route_cache_ttl_ms);
    }

    return ErrorCode::OK;
}

AddReplicaCallback P2PClientService::BuildAddReplicaCallback() {
    return [this](const std::string& key, const UUID& tier_id,
                  size_t size) -> tl::expected<void, ErrorCode> {
        if (ha_manager_ && ha_manager_->IsDegraded()) {
            return {};
        }
        if (async_route_notifier_) {
            return async_route_notifier_->EnqueueAdd(key, tier_id, size);
        }
        return SyncAddReplica(key, tier_id, size);
    };
}

RemoveReplicaCallback P2PClientService::BuildRemoveReplicaCallback() {
    return
        [this](
            const std::string& key, const UUID& tier_id,
            enum REMOVE_CALLBACK_TYPE type) -> tl::expected<void, ErrorCode> {
            if (ha_manager_ && ha_manager_->IsDegraded()) {
                return {};
            }
            if (type == REMOVE_CALLBACK_TYPE::DELETE) {
                if (async_route_notifier_) {
                    return async_route_notifier_->EnqueueRemove(key, tier_id);
                }
                return SyncRemoveReplica(key, tier_id);
            } else if (type == REMOVE_CALLBACK_TYPE::DELETE_ALL) {
                LOG(ERROR) << "DELETE_ALL callback is not supported"
                           << ", key: " << key;
                return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
            }

            LOG(ERROR) << "Unknown callback type: " << static_cast<int>(type);
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        };
}

tl::expected<void, ErrorCode> P2PClientService::SyncAddReplica(
    const std::string& key, const UUID& tier_id, size_t size) {
    AddReplicaRequest req;
    req.key = key;
    req.size = size;
    req.client_id = client_id_;
    req.segment_id = tier_id;
    auto result = master_client_.AddReplica(req);
    if (!result) {
        LOG(ERROR) << "Failed to add replica for key: " << key
                   << " error: " << result.error();
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> P2PClientService::SyncRemoveReplica(
    const std::string& key, const UUID& tier_id) {
    RemoveReplicaRequest req;
    req.key = key;
    req.client_id = client_id_;
    req.segment_id = tier_id;
    auto result = master_client_.RemoveReplica(req);
    if (!result) {
        LOG(ERROR) << "Failed to remove replica for key: " << key
                   << " error: " << result.error();
        return tl::unexpected(result.error());
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
P2PClientService::SyncBatchRemoveReplica(const std::string& key,
                                         std::vector<UUID> segment_ids) {
    BatchRemoveReplicaRequest req;
    req.key = key;
    req.client_id = client_id_;
    req.segment_ids = std::move(segment_ids);
    auto results = master_client_.BatchRemoveReplica(req);
    for (size_t i = 0; i < results.size(); i++) {
        if (!results[i]) {
            LOG(ERROR) << "Failed to remove replica for key: " << key
                       << ", segment_id: " << req.segment_ids[i]
                       << ", error: " << results[i].error();
        }
    }
    return results;
}

SegmentSyncCallback P2PClientService::BuildSegmentSyncCallback() {
    return [this](const Segment& segment,
                  bool mount) -> tl::expected<void, ErrorCode> {
        if (mount) {
            if (ha_manager_ && ha_manager_->IsDegraded()) {
                LOG(INFO) << "Skipping MountSegment in DEGRADED mode: id="
                          << segment.id << ", name=" << segment.name;
                return {};
            }
            LOG(INFO) << "Mounting segment with Master: id=" << segment.id
                      << ", name=" << segment.name << ", size=" << segment.size;
            auto result = master_client_.MountSegment(segment);
            if (!result) {
                LOG(ERROR) << "Failed to mount segment with Master: id="
                           << segment.id << ", error=" << result.error();
                return tl::unexpected(result.error());
            }
            return {};
        } else {
            LOG(INFO) << "Unmounting segment from Master: id=" << segment.id
                      << ", name=" << segment.name;
            auto result = master_client_.UnmountSegment(segment.id);
            if (!result) {
                LOG(ERROR) << "Failed to unmount segment from Master: id="
                           << segment.id << ", error=" << result.error();
                return tl::unexpected(result.error());
            }
            return {};
        }
    };
}

// ============================================================================
// Heartbeat & Registration (P2P)
// ============================================================================

HeartbeatRequest P2PClientService::build_heartbeat_request() {
    HeartbeatRequest req;
    req.client_id = client_id_;

    if (data_manager_.has_value()) {
        SyncSegmentMetaParam param;
        auto tier_views = data_manager_->GetTierViews();
        for (const auto& view : tier_views) {
            TierUsageInfo info;
            info.segment_id = view.id;
            info.usage = view.usage;
            param.tier_usages.push_back(info);
        }
        req.tasks.emplace_back(HeartbeatTaskType::SYNC_SEGMENT_META,
                               std::move(param));
    }

    return req;
}

std::vector<Segment> P2PClientService::CollectTierSegments() const {
    std::vector<Segment> segments;
    if (!data_manager_.has_value()) {
        return segments;
    }

    auto tier_views = data_manager_->GetTierViews();
    segments.reserve(tier_views.size());
    for (const auto& view : tier_views) {
        Segment seg;
        seg.id = view.id;
        seg.name = "tier_" + std::to_string(view.id.first) + "_" +
                   std::to_string(view.id.second);
        seg.size = view.capacity;
        auto& p2p_extra = seg.GetP2PExtra();
        p2p_extra.priority = view.priority;
        p2p_extra.tags = view.tags;
        p2p_extra.memory_type = view.type;
        p2p_extra.usage = view.usage;
        segments.push_back(std::move(seg));
    }
    return segments;
}

tl::expected<RegisterClientResponse, ErrorCode>
P2PClientService::RegisterClient() {
    RegisterClientRequest req;
    req.client_id = client_id_;
    req.segments = CollectTierSegments();
    req.deployment_mode = DeploymentMode::P2P;
    req.ip_address = local_ip_;
    req.rpc_port = client_rpc_port_;

    auto register_result = master_client_.RegisterClient(req);
    if (!register_result) {
        LOG(ERROR) << "Failed to register P2P client: "
                   << register_result.error() << ", client_id=" << client_id_;
    } else {
        view_version_ = register_result.value().view_version;
    }
    return register_result;
}

std::string P2PClientService::GetHealthStatus() const {
    if (ha_manager_) {
        return toString(ha_manager_->GetState());
    }
    return "OK";
}

// ============================================================================
// Put Operations
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::Put(const ObjectKey& key,
                                                    std::vector<Slice>& slices,
                                                    const WriteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Put");
    timer.LogRequest("key=", key, "slice_count=", slices.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto task_handle_ptr = CreatePutHandle(key, slices, *route_config);
    if (!task_handle_ptr) {
        LOG(ERROR) << "Failed to create put handle for key: " << key
                   << ", error: " << task_handle_ptr.error();
        timer.LogResponse("error_code=", task_handle_ptr.error());
        return tl::unexpected(task_handle_ptr.error());
    } else if (!task_handle_ptr.value()) {
        LOG(ERROR) << "put task handle is null for key: " << key;
        timer.LogResponse("error_code=", ErrorCode::INTERNAL_ERROR);
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = task_handle_ptr.value()->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to put key: " << key
                   << ", error: " << result.error();
    }
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchPut");
    timer.LogRequest("batch_size=", keys.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
    }
    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    if (keys.size() != batched_slices.size()) {
        LOG(ERROR) << "BatchPut input size mismatch";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
        handles;
    handles.reserve(keys.size());

    if (ha_manager_ && ha_manager_->IsDegraded()) {
        for (size_t i = 0; i < keys.size(); ++i) {
            handles.push_back(CreateLocalPutHandle(keys[i], batched_slices[i]));
        }
    } else {
        auto batch_route_result =
            BatchFetchWriteRoutes(keys, batched_slices, *route_config);
        if (!batch_route_result) {
            LOG(ERROR) << "BatchGetWriteRoute RPC failed: "
                       << batch_route_result.error();
            std::fill(results.begin(), results.end(),
                      tl::unexpected(batch_route_result.error()));
            timer.LogResponse("success=0 fail=", keys.size(),
                              " error_code=", batch_route_result.error());
            return results;
        }

        auto& batch_resp = batch_route_result.value();
        for (size_t i = 0; i < keys.size(); ++i) {
            if (batch_resp.error_codes[i] != ErrorCode::OK) {
                handles.push_back(tl::unexpected(batch_resp.error_codes[i]));
                continue;
            }
            handles.push_back(InnerCreatePutHandle(
                keys[i], batched_slices[i], *route_config,
                std::move(batch_resp.responses[i].candidates)));
        }
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            LOG(ERROR) << "Failed to create put handle for key: " << keys[i]
                       << ", error: " << handles[i].error();
            results[i] = tl::unexpected(handles[i].error());
        } else if (!handles[i].value()) {
            LOG(ERROR) << "put task handle is null for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } else {
            auto result = handles[i].value()->Wait();
            if (!result) {
                LOG(ERROR) << "Failed to put key: " << keys[i]
                           << ", error: " << result.error();
            }
            results[i] = result;
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

inline bool IsAlreadyExistsError(ErrorCode err) {
    return err == ErrorCode::REPLICA_NUM_EXCEEDED ||
           err == ErrorCode::REPLICA_ALREADY_EXISTS ||
           err == ErrorCode::OBJECT_ALREADY_EXISTS;
}

static async_simple::coro::Lazy<void> RunWriteRetry(
    std::vector<P2PProxyDescriptor> proxies,
    std::shared_ptr<RemoteWriteRequest> write_req,
    std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise,
    RouteCache* route_cache,
    std::function<PeerClient&(const std::string&)> get_peer, std::string key) {
    try {
        for (auto& proxy : proxies) {
            try {
                std::string endpoint =
                    proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
                auto& peer = get_peer(endpoint);
                auto result = co_await peer.AsyncWriteRemoteData(*write_req);
                if (result.has_value()) {
                    if (route_cache) {
                        P2PProxyDescriptor np = proxy;
                        np.segment_id = result.value();
                        route_cache->Upsert(key, {np});
                    }
                    promise->set_value({});
                    co_return;
                } else if (IsAlreadyExistsError(result.error())) {
                    promise->set_value({});
                    co_return;
                } else {
                    LOG(ERROR) << "Failed to write to remote, key: " << key
                               << ", error: " << result.error();
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to write to remote, key: " << key
                           << ", exception: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Failed to write to remote, key: " << key
                           << ", unknown exception";
            }
        }
        promise->set_value(tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    } catch (...) {
        promise->set_value(tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
}

tl::expected<BatchGetWriteRouteResponse, ErrorCode>
P2PClientService::BatchFetchWriteRoutes(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    const WriteRouteRequestConfig& config) {
    BatchGetWriteRouteRequest req;
    req.client_id = client_id_;
    req.config = config;
    req.keys.reserve(keys.size());
    req.sizes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        req.keys.push_back(keys[i]);
        req.sizes.push_back(CalculateSliceSize(batched_slices[i]));
    }
    auto batch_route_result = master_client_.BatchGetWriteRoute(req);
    if (!batch_route_result) {
        LOG(ERROR) << "BatchGetWriteRoute RPC failed: "
                   << batch_route_result.error();
        return batch_route_result;
    }
    return batch_route_result;
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::CreatePutHandle(const std::string& key,
                                  std::vector<Slice>& slices,
                                  const WriteRouteRequestConfig& config) {
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return CreateLocalPutHandle(key, slices);
    }

    WriteRouteRequest route_req;
    route_req.key = key;
    route_req.client_id = client_id_;
    route_req.size = CalculateSliceSize(slices);
    route_req.config = config;

    auto route_result = master_client_.GetWriteRoute(route_req);
    if (!route_result) {
        LOG(WARNING) << "Failed to get write route for key: " << key
                     << " error: " << route_result.error();
        return tl::unexpected(route_result.error());
    }

    return InnerCreatePutHandle(key, slices, config,
                                std::move(route_result.value().candidates));
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::CreateLocalPutHandle(const std::string& key,
                                       std::vector<Slice>& slices) {
    auto local_handle = data_manager_->Put(key, slices);
    if (local_handle) {
        return std::move(local_handle.value());
    }
    LOG(ERROR) << "Local write failed for key: " << key
               << ", error: " << local_handle.error();
    return tl::unexpected(local_handle.error());
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::InnerCreatePutHandle(
    const std::string& key, std::vector<Slice>& slices,
    const WriteRouteRequestConfig& config,
    std::vector<WriteCandidate> candidates) {
    if (candidates.empty()) {
        LOG(ERROR) << "No write candidates for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<P2PProxyDescriptor> remote_proxies;
    for (size_t i = 0; i < candidates.size(); ++i) {
        auto& proxy = candidates[i].replica;

        if (proxy.client_id == client_id_) {
            if (!config.allow_local) {
                LOG(WARNING) << "Master returned local candidate but "
                                "allow_local=false, skipping";
                continue;
            }
            if (data_manager_.has_value()) {
                auto local_handle = data_manager_->Put(key, slices);
                if (local_handle) {
                    return std::move(local_handle.value());
                } else if (IsAlreadyExistsError(local_handle.error())) {
                    return ImmediateHandle<void>::Create();
                } else {
                    LOG(WARNING) << "Local write failed, trying next candidate"
                                 << ", error: " << local_handle.error();
                    continue;
                }
            }
        } else {
            remote_proxies.push_back(proxy);
        }
    }

    if (remote_proxies.empty()) {
        LOG(ERROR) << "No remote candidates for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto write_req = std::make_shared<RemoteWriteRequest>();
    write_req->key = key;
    for (const auto& slice : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(slice.ptr);
        buf.size = slice.size;
        write_req->src_buffers.push_back(buf);
    }

    auto promise =
        std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->get_future();

    RunWriteRetry(
        std::move(remote_proxies), write_req, promise,
        route_cache_ ? &(*route_cache_) : nullptr,
        [this](const std::string& ep) -> PeerClient& {
            return GetOrCreatePeerClient(ep);
        },
        key)
        .start([](auto&&) {});

    return RemoteRpcHandle<void>::Create(std::move(write_req),
                                         std::move(future));
}

// ============================================================================
// Get Operations
// ============================================================================

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> P2PClientService::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Get");
    timer.LogRequest("key=", key);

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto handle = CreateGetHandle(key, allocator, config);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to create get handle for key: " << key
                       << ", error: " << handle.error();
        }
        timer.LogResponse("error_code=", handle.error());
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "get failed for key: " << key
                   << ", error: " << result.error();
        timer.LogResponse("error_code=", result.error());
        return tl::unexpected(result.error());
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return handle->read_buf;
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
P2PClientService::BatchGet(const std::vector<std::string>& keys,
                           std::shared_ptr<ClientBufferAllocator> allocator,
                           const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchGet");
    timer.LogRequest("batch_size=", keys.size());

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::OK));

    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::SHUTTING_DOWN);
        }
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return results;
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        handles.push_back(CreateGetHandle(keys[i], allocator, config));
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            if (handles[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                           << ", error: " << handles[i].error();
            }
            results[i] = tl::unexpected(handles[i].error());
            continue;
        }
        auto get_result = handles[i]->task_handle->Wait();
        if (!get_result) {
            LOG(ERROR) << "Failed to get key: " << keys[i]
                       << ", error: " << get_result.error();
            results[i] = tl::unexpected(get_result.error());
        } else {
            results[i] = handles[i]->read_buf;
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

tl::expected<int64_t, ErrorCode> P2PClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Get");
    timer.LogRequest("key=", key, "buffer_count=", buffers.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::vector<Slice> slices;
    slices.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        slices.emplace_back(Slice{buffers[i], sizes[i]});
    }

    auto handle = CreateGetHandle(key, slices, config);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to create get handle for key: " << key
                       << ", error: " << handle.error();
        }
        timer.LogResponse("error_code=", handle.error());
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to wait for get handle for key: " << key
                   << ", error: " << result.error();
        timer.LogResponse("error_code=", result.error());
        return tl::unexpected(result.error());
    }
    timer.LogResponse("error_code=", ErrorCode::OK,
                      " data_size=", handle->data_size);
    return handle->data_size;
}

std::vector<tl::expected<int64_t, ErrorCode>> P2PClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool /*aggregate_same_segment_task*/) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchGet");
    timer.LogRequest("batch_size=", keys.size());

    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::vector<Slice>> all_slices(keys.size());
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;
    handles.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        all_slices[i].reserve(all_buffers[i].size());
        for (size_t j = 0; j < all_buffers[i].size(); ++j) {
            all_slices[i].emplace_back(
                Slice{all_buffers[i][j], all_sizes[i][j]});
        }
        handles.push_back(CreateGetHandle(keys[i], all_slices[i], config));
    }

    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            if (handles[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                           << ", error: " << handles[i].error();
            }
            results.push_back(tl::unexpected(handles[i].error()));
            continue;
        }
        auto get_result = handles[i]->task_handle->Wait();
        if (!get_result) {
            LOG(ERROR) << "Failed to get key: " << keys[i]
                       << ", error: " << get_result.error();
            results.push_back(tl::unexpected(get_result.error()));
        } else {
            results.push_back(handles[i]->data_size);
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateGetHandle(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    if (data_manager_.has_value()) {
        auto local = data_manager_->Get(key, allocator);
        if (local.has_value()) {
            return std::move(local.value());
        }
        if (local.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get from local, key: " << key
                       << ", error: " << local.error();
            return tl::unexpected(local.error());
        }
    }

    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto iter_result = BuildRouteIter(key, config);
    if (!iter_result) {
        if (iter_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "fail to build route iterator"
                       << ", key=" << key << ", error=" << iter_result.error();
        }
        return tl::unexpected(iter_result.error());
    }
    auto& iter = iter_result.value();

    iter.Prime();
    if (iter.empty()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    const uint64_t object_size = iter.object_size();
    auto alloc_result = allocator->allocate(object_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto read_buf = std::make_shared<BufferHandle>(std::move(*alloc_result));
    std::vector<Slice> slices = {{read_buf->ptr(), object_size}};

    auto fetch_result = InnerGetViaRoute(key, slices, std::move(iter));
    if (!fetch_result) {
        LOG(ERROR) << "Failed to inner get via route for key: " << key
                   << ", error: " << fetch_result.error();
        return tl::unexpected(fetch_result.error());
    } else if (!fetch_result.value().task_handle) {
        LOG(ERROR) << "task handle ptr is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    fetch_result->read_buf = std::move(read_buf);
    return std::move(fetch_result.value());
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateGetHandle(
    const std::string& key, std::vector<Slice>& slices,
    const ReadRouteConfig& config) {
    if (data_manager_.has_value()) {
        auto local = data_manager_->Get(key, slices);
        if (local.has_value()) {
            return std::move(local.value());
        }
        if (local.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get from local, key: " << key
                       << ", error: " << local.error();
            return tl::unexpected(local.error());
        }
    }

    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto iter_result = BuildRouteIter(key, config);
    if (!iter_result) {
        if (iter_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to build route iterator for key: " << key
                       << ", error: " << iter_result.error();
        }
        return tl::unexpected(iter_result.error());
    }
    auto& iter = iter_result.value();

    iter.Prime();
    if (iter.empty()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto fetch_result = InnerGetViaRoute(key, slices, std::move(iter));
    if (!fetch_result) {
        LOG(ERROR) << "Failed to inner get via route for key: " << key
                   << ", error: " << fetch_result.error();
        return tl::unexpected(fetch_result.error());
    } else if (!fetch_result.value().task_handle) {
        LOG(ERROR) << "get task handle is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return std::move(fetch_result.value());
}

async_simple::coro::Lazy<void> P2PClientService::RunReadRetry(
    RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
    std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise) {
    ErrorCode final_result = ErrorCode::OBJECT_NOT_FOUND;
    try {
        while (auto route = co_await iter.AsyncNext()) {
            try {
                auto result = co_await route->peer->AsyncReadRemoteData(*req);
                if (result.has_value()) {
                    promise->set_value({});
                    co_return;
                } else if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(ERROR) << "Failed to get from remote, key: " << req->key
                               << ", error: " << result.error();
                } else {
                    final_result = result.error();
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", exception: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", unknown exception";
            }
            iter.Evict(*route);
        }
        promise->set_value(tl::unexpected(final_result));
    } catch (...) {
        promise->set_value(tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::InnerGetViaRoute(
    const std::string& key, std::vector<Slice>& slices, RouteIterator iter) {
    auto req = std::make_shared<RemoteReadRequest>();
    req->key = key;
    for (const auto& s : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(s.ptr);
        buf.size = s.size;
        req->dest_buffers.push_back(buf);
    }

    auto promise =
        std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->get_future();

    const uint64_t object_size = iter.object_size();
    RunReadRetry(std::move(iter), req, promise).start([](auto&&) {});

    ReadTaskHandle res;
    res.data_size = object_size;
    res.task_handle =
        RemoteRpcHandle<void>::Create(std::move(req), std::move(future));
    return res;
}

// ============================================================================
// RouteIterator
// ============================================================================

P2PClientService::RouteIterator::RouteIterator(
    std::string key, std::vector<ResolvedRoute> initial, uint64_t object_size,
    RouteCache* route_cache, MasterFetch master_fetch)
    : key_(std::move(key)),
      routes_(std::move(initial)),
      object_size_(object_size),
      route_cache_(route_cache),
      master_fetch_(std::move(master_fetch)) {}

void P2PClientService::RouteIterator::Prime() {
    if (!routes_.empty() || master_queried_) {
        return;
    }
    master_queried_ = true;
    auto master_routes = async_simple::coro::syncAwait(master_fetch_());
    if (master_routes.empty()) {
        return;
    }
    UpsertToCache(master_routes);
    routes_.insert(routes_.end(),
                   std::make_move_iterator(master_routes.begin()),
                   std::make_move_iterator(master_routes.end()));
    if (!routes_.empty()) {
        object_size_ = routes_.front().object_size;
    }
}

auto P2PClientService::RouteIterator::AsyncNext()
    -> async_simple::coro::Lazy<std::optional<ResolvedRoute>> {
    if (idx_ < routes_.size()) {
        co_return routes_[idx_++];
    }
    if (master_queried_) {
        co_return std::nullopt;
    }
    master_queried_ = true;
    auto master_routes = co_await master_fetch_();
    if (master_routes.empty()) {
        co_return std::nullopt;
    }
    UpsertToCache(master_routes);
    routes_.insert(routes_.end(),
                   std::make_move_iterator(master_routes.begin()),
                   std::make_move_iterator(master_routes.end()));
    if (object_size_ == 0) {
        object_size_ = routes_[idx_].object_size;
    }
    if (idx_ < routes_.size()) {
        co_return routes_[idx_++];
    }
    co_return std::nullopt;
}

void P2PClientService::RouteIterator::UpsertToCache(
    const std::vector<ResolvedRoute>& routes) {
    if (!route_cache_ || routes.empty()) {
        return;
    }
    std::vector<P2PProxyDescriptor> ps;
    ps.reserve(routes.size());
    for (const auto& r : routes) {
        ps.push_back(r.proxy);
    }
    route_cache_->Upsert(key_, ps);
}

void P2PClientService::RouteIterator::Evict(const ResolvedRoute& route) {
    if (route.is_cached && route_cache_) {
        route_cache_->RemoveReplica(key_, {route.proxy});
    }
}

auto P2PClientService::BuildRouteIter(const std::string& key,
                                      const ReadRouteConfig& config)
    -> tl::expected<RouteIterator, ErrorCode> {
    std::vector<ResolvedRoute> routes;

    if (route_cache_) {
        auto cached = route_cache_->Get(key);
        for (const auto& item : cached.items()) {
            P2PProxyDescriptor proxy;
            proxy.client_id = item.client_id;
            proxy.segment_id = item.segment_id;
            proxy.ip_address = item.ip_address;
            proxy.rpc_port = item.rpc_port;
            proxy.object_size = item.object_size;

            std::string endpoint =
                proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
            auto& peer = GetOrCreatePeerClient(endpoint);

            ResolvedRoute route;
            route.peer = &peer;
            route.object_size = proxy.object_size;
            route.is_cached = true;
            route.proxy = proxy;
            routes.push_back(std::move(route));
        }
    }

    uint64_t object_size = routes.empty() ? 0 : routes.front().object_size;
    return RouteIterator(key, std::move(routes), object_size,
                         route_cache_ ? &(*route_cache_) : nullptr,
                         [this, key, config]() {
                             return AsyncResolveRoutesFromMaster(key, config);
                         });
}

async_simple::coro::Lazy<std::vector<P2PClientService::ResolvedRoute>>
P2PClientService::AsyncResolveRoutesFromMaster(const std::string& key,
                                               const ReadRouteConfig& config) {
    std::vector<ResolvedRoute> result;
    auto replica_result =
        co_await master_client_.AsyncGetReplicaList(key, config);
    if (!replica_result) {
        if (replica_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to query replica size for key: " << key
                       << ", error: " << replica_result.error();
        }
        co_return result;
    }
    auto& replicas = replica_result.value().replicas;
    if (replicas.empty()) {
        co_return result;
    }
    uint64_t total_size = 0;
    for (auto& replica : replicas) {
        if (replica.is_p2p_proxy_replica()) {
            total_size = calculate_total_size(replica);
            break;
        }
    }
    if (total_size == 0) {
        LOG(ERROR) << "Cannot determine size for key: " << key;
        co_return result;
    }
    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) {
            continue;
        }
        auto proxy = replica.get_p2p_proxy_descriptor();
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        auto& peer = GetOrCreatePeerClient(endpoint);
        ResolvedRoute route;
        route.peer = &peer;
        route.object_size = total_size;
        route.is_cached = false;
        route.proxy = proxy;
        result.push_back(std::move(route));
    }
    co_return result;
}

// ============================================================================
// IsExist / BatchIsExist (P2P: local-first)
// ============================================================================

tl::expected<bool, ErrorCode> P2PClientService::IsExist(
    const std::string& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }

    if (data_manager_.has_value() && data_manager_->Exist(key)) {
        return true;
    }

    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return false;
    }

    return master_client_.ExistKey(key);
}

std::vector<tl::expected<bool, ErrorCode>> P2PClientService::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }

    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    std::vector<size_t> miss_indices;
    std::vector<std::string> miss_keys;

    for (size_t i = 0; i < keys.size(); ++i) {
        const bool local_hit =
            data_manager_.has_value() && data_manager_->Exist(keys[i]);
        if (local_hit) {
            results[i] = true;
        } else {
            miss_indices.push_back(i);
            miss_keys.push_back(keys[i]);
        }
    }

    if (!miss_keys.empty()) {
        auto master_results = master_client_.BatchExistKey(miss_keys);
        for (size_t j = 0; j < miss_indices.size(); ++j) {
            results[miss_indices[j]] = master_results[j];
        }
    }

    return results;
}

// ============================================================================
// Query Operations
// ============================================================================

tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode> P2PClientService::Query(
    const std::string& object_key, const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }

    if (ha_manager_ && ha_manager_->IsDegraded()) {
        LOG(WARNING) << "fail to access master"
                     << ", key=" << object_key;
        return tl::make_unexpected(ErrorCode::INACCESSIBLE_MASTER);
    }

    auto result = master_client_.GetReplicaList(object_key, config);
    if (!result) {
        LOG(WARNING) << "fail to get replica list"
                     << ", key=" << object_key << ", error=" << result.error();
        return tl::unexpected(result.error());
    }

    return std::make_unique<P2PQueryResult>(std::move(result.value().replicas));
}

std::vector<tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode>>
P2PClientService::BatchQuery(const std::vector<std::string>& object_keys,
                             const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::vector<tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.push_back(tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
        }
        return results;
    }
    auto responses = master_client_.BatchGetReplicaList(object_keys, config);
    std::vector<tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode>> results;
    results.reserve(responses.size());
    for (size_t i = 0; i < responses.size(); ++i) {
        if (responses[i]) {
            results.emplace_back(std::make_unique<P2PQueryResult>(
                std::move(responses[i].value().replicas)));
        } else {
            results.emplace_back(tl::unexpected(responses[i].error()));
        }
    }
    return results;
}

// ============================================================================
// Remove Operations (Not Supported in P2P)
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::Remove(const ObjectKey& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "Remove is not supported in P2P mode";
    return {};
}

tl::expected<long, ErrorCode> P2PClientService::RemoveByRegex(
    const ObjectKey& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "RemoveByRegex is not supported in P2P mode";
    return {};
}

tl::expected<long, ErrorCode> P2PClientService::RemoveAll() {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "RemoveAll is not supported in P2P mode";
    return {};
}

// ============================================================================
// MountSegment / UnmountSegment (Not Supported)
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::MountSegment(const void* buffer,
                                                             size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "MountSegment is not supported in P2P mode. "
                 << "Please use TieredBackend::Init config for tier setup.";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> P2PClientService::UnmountSegment(
    const void* buffer, size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "UnmountSegment is not supported in P2P mode.";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

// ============================================================================
// PeerClient management
// ============================================================================

PeerClient& P2PClientService::GetOrCreatePeerClient(
    const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(peer_clients_mutex_);
    auto it = peer_clients_.find(endpoint);
    if (it != peer_clients_.end()) {
        return *it->second;
    }

    auto client = std::make_unique<PeerClient>();
    auto connect_result = client->Connect(endpoint);
    if (!connect_result) {
        LOG(ERROR) << "Failed to connect PeerClient to " << endpoint
                   << " error: " << connect_result.error();
    }

    auto [inserted_it, _] = peer_clients_.emplace(endpoint, std::move(client));
    return *inserted_it->second;
}

}  // namespace mooncake
