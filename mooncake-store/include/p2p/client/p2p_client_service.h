#pragma once

#include <boost/functional/hash.hpp>
#include <condition_variable>
#include <coroutine>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "async_metadata_notifier.h"
#include "client_buffer.hpp"
#include "client_config_builder.h"
#include "client_metric.h"
#include "client_rpc_service.h"
#include "data_manager.h"
#include "ha_helper.h"
#include "ha_recovery_manager.h"
#include "mutex.h"
#include "p2p_master_client.h"
#include "p2p_rpc_types.h"
#include "peer_client.h"
#include "replica.h"
#include "route_cache.h"
#include "task_handle.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

using WriteConfig = std::variant<ReplicateConfig, WriteRouteRequestConfig>;

/**
 * @brief Result of a query operation containing replica information (P2P).
 * Renamed from QueryResult to avoid clash with central client_service.h's
 * QueryResult (which carries lease_timeout).
 */
class P2PQueryResult {
   public:
    const std::vector<Replica::Descriptor> replicas;

    explicit P2PQueryResult(std::vector<Replica::Descriptor>&& replicas_param)
        : replicas(std::move(replicas_param)) {}

    ~P2PQueryResult() = default;

    P2PQueryResult(const P2PQueryResult&) = delete;
    P2PQueryResult& operator=(const P2PQueryResult&) = delete;
    P2PQueryResult(P2PQueryResult&&) = default;
    P2PQueryResult& operator=(P2PQueryResult&&) = default;
};

/**
 * @brief P2P client service.
 *
 * Flattened from p2p-branch ClientService (abstract base) + P2PClientService
 * (subclass). No base class, no virtual methods — standalone ("分家"). The
 * central client (Client in client_service.h) is untouched and independent.
 */
class P2PClientService {
   public:
    P2PClientService(const std::string& local_ip, uint16_t te_port,
                     const std::string& metadata_connstring,
                     uint16_t metrics_port = 9003,
                     bool enable_metrics_http = true,
                     const std::map<std::string, std::string>& labels = {});

    ~P2PClientService();

    ErrorCode Init(const P2PClientConfig& config);

    static std::optional<std::shared_ptr<P2PClientService>> Create(
        const P2PClientConfig& config);

    /**
     * @brief stops background threads
     */
    void Stop();

    /**
     * @brief stops heartbeat thread
     */
    void StopHeartbeat();

    /**
     * @brief Release internal resources. Should be called after Stop()
     */
    void Destroy();

    DeploymentMode deployment_mode() const { return DeploymentMode::P2P; }

    // ---- Query / Get / Put (P2P) ----
    tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode> Query(
        const std::string& object_key, const ReadRouteConfig& config = {});

    std::vector<tl::expected<std::unique_ptr<P2PQueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {});

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {});

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {});

    tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes, const ReadRouteConfig& config = {});

    std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false);

    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config);

    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config);

    tl::expected<void, ErrorCode> Remove(const ObjectKey& key);
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str);
    tl::expected<long, ErrorCode> RemoveAll();

    tl::expected<void, ErrorCode> MountSegment(const void* buffer, size_t size);
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size);

    tl::expected<bool, ErrorCode> IsExist(const std::string& key);
    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys);

    // ---- Common helpers (absorbed from former base) ----
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str);

    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics_->summary_metrics();
    }

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() {
        auto guard = AcquireInflightGuard();
        if (!guard.is_valid()) {
            LOG(ERROR) << "client is shutting down";
            return tl::unexpected(ErrorCode::SHUTTING_DOWN);
        }
        return GetMasterClient().CalcCacheStats();
    }

    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics_->serialize(str);
        return str;
    }

    uint16_t GetMetricsPort() const { return metrics_port_; }
    bool IsMetricsHttpEnabled() const { return enable_metrics_http_; }

    std::string GetHealthStatus() const;

    [[nodiscard]] std::string GetTransportEndpoint() {
        return transfer_engine_->getLocalIpAndPort();
    }
    UUID GetClientID() const { return client_id_; }
    ViewVersionId GetViewVersion() const { return view_version_.load(); }

    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);

    [[nodiscard]] static size_t CalculateSliceSize(
        const std::vector<Slice>& slices);
    [[nodiscard]] static size_t CalculateSliceSize(std::span<const Slice> slices);

   protected:
    P2PMasterClient& GetMasterClient() { return master_client_; }

    ErrorCode ConnectToMaster(const std::string& master_server_entry);

    ErrorCode InitTransferEngine(
        const std::string& endpoint, const std::string& metadata_connstring,
        const std::string& protocol,
        const std::optional<std::string>& device_names);

    void StartHeartbeat(const std::string& master_server_entry);
    void HeartbeatThreadMain(bool is_ha_mode, std::string current_master_address,
                             const std::string& master_server_entry);
    bool HandleHeartbeatResponse(const HeartbeatResponse& response,
                                 const std::string& current_master_address,
                                 const std::function<void()>& register_client,
                                 std::future<void>& register_client_future);
    void HandleHeartbeatTaskResult(const HeartbeatTaskResult& task_result);
    bool ReconnectToMaster(bool is_ha_mode,
                           std::string& current_master_address);
    void WaitForNextHeartbeat(int interval_ms);
    HeartbeatRequest build_heartbeat_request();

    uint16_t StartMetricsHttpServer(bool enable_metrics_http,
                                    uint16_t metrics_port);
    void StopMetricsHttpServer();

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient();
    void OnHAEvent(HAEvent event) {
        if (ha_manager_) ha_manager_->HandleEvent(event);
    }

    void initTeEndpoint();
    std::string local_endpoint() const {
        return local_ip_ + ":" + std::to_string(te_port_);
    }
    const std::string& get_te_endpoint() const { return te_endpoint_; }

    // RAII guard for managing in-flight requests during service shutdown.
    class InflightRequestGuard {
       public:
        explicit InflightRequestGuard(P2PClientService* client)
            : client_(client),
              valid_(false),
              lock_(&client_->running_rw_mtx_, shared_lock) {
            valid_ = client_->is_running_;
        }
        ~InflightRequestGuard() = default;
        InflightRequestGuard(const InflightRequestGuard&) = delete;
        InflightRequestGuard& operator=(const InflightRequestGuard&) = delete;
        InflightRequestGuard(InflightRequestGuard&&) = delete;
        InflightRequestGuard& operator=(InflightRequestGuard&&) = delete;
        bool is_valid() const { return valid_; }

       private:
        P2PClientService* client_;
        bool valid_;
        SharedMutexLocker lock_;
    };

    InflightRequestGuard AcquireInflightGuard() {
        return InflightRequestGuard(this);
    }

    bool MarkShuttingDown() {
        SharedMutexLocker lock(&running_rw_mtx_);
        if (!is_running_) return false;
        is_running_ = false;
        return true;
    }

   private:
    // ---- P2P internal helpers ----
    ErrorCode InitStorage(const P2PClientConfig& config);
    AddReplicaCallback BuildAddReplicaCallback();
    RemoveReplicaCallback BuildRemoveReplicaCallback();
    SegmentSyncCallback BuildSegmentSyncCallback();
    tl::expected<void, ErrorCode> SyncAddReplica(const std::string& key,
                                                 const UUID& tier_id,
                                                 size_t size);
    tl::expected<void, ErrorCode> SyncRemoveReplica(const std::string& key,
                                                    const UUID& tier_id);
    std::vector<tl::expected<void, ErrorCode>> SyncBatchRemoveReplica(
        const std::string& key, std::vector<UUID> segment_ids);
    std::vector<Segment> CollectTierSegments() const;

    struct ResolvedRoute {
        PeerClient* peer = nullptr;
        uint64_t object_size = 0;
        bool is_cached = false;
        P2PProxyDescriptor proxy;
    };

    class RouteIterator {
       public:
        using MasterFetch = std::function<
            async_simple::coro::Lazy<std::vector<ResolvedRoute>>()>;

        RouteIterator(std::string key, std::vector<ResolvedRoute> initial,
                      uint64_t object_size, RouteCache* route_cache,
                      MasterFetch master_fetch);

        uint64_t object_size() const { return object_size_; }
        bool empty() const { return routes_.empty() && master_queried_; }

        void Prime();
        async_simple::coro::Lazy<std::optional<ResolvedRoute>> AsyncNext();
        void Evict(const ResolvedRoute& route);

       private:
        void UpsertToCache(const std::vector<ResolvedRoute>& routes);
        std::string key_;
        std::vector<ResolvedRoute> routes_;
        size_t idx_ = 0;
        bool master_queried_ = false;
        uint64_t object_size_ = 0;
        RouteCache* route_cache_ = nullptr;
        MasterFetch master_fetch_;
    };

    tl::expected<RouteIterator, ErrorCode> BuildRouteIter(
        const std::string& key, const ReadRouteConfig& config);
    async_simple::coro::Lazy<std::vector<ResolvedRoute>>
    AsyncResolveRoutesFromMaster(const std::string& key,
                                 const ReadRouteConfig& config);
    static async_simple::coro::Lazy<void> RunReadRetry(
        RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
        std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise);

    tl::expected<BatchGetWriteRouteResponse, ErrorCode> BatchFetchWriteRoutes(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices,
        const WriteRouteRequestConfig& config);
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> CreatePutHandle(
        const std::string& key, std::vector<Slice>& slices,
        const WriteRouteRequestConfig& config);
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
    CreateLocalPutHandle(const std::string& key, std::vector<Slice>& slices);
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
    InnerCreatePutHandle(const std::string& key, std::vector<Slice>& slices,
                         const WriteRouteRequestConfig& config,
                         std::vector<WriteCandidate> candidates);
    tl::expected<ReadTaskHandle, ErrorCode> CreateGetHandle(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config);
    tl::expected<ReadTaskHandle, ErrorCode> CreateGetHandle(
        const std::string& key, std::vector<Slice>& slices,
        const ReadRouteConfig& config);
    tl::expected<ReadTaskHandle, ErrorCode> InnerGetViaRoute(
        const std::string& key, std::vector<Slice>& slices, RouteIterator iter);
    PeerClient& GetOrCreatePeerClient(const std::string& endpoint);

   private:
    // Client identification
    const UUID client_id_;

    // Client-side metrics
    std::unique_ptr<ClientMetric> metrics_;

    // Core components
    std::shared_ptr<TransferEngine> transfer_engine_;

    struct SegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) {
                free(ptr);
            }
        }
    };
    struct AscendSegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) {
                free_memory("ascend", ptr);
            }
        }
    };
    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::vector<std::unique_ptr<void, AscendSegmentDeleter>>
        ascend_segment_ptrs_;

    // Configuration
    const std::string local_ip_;
    const uint16_t te_port_;
    std::string te_endpoint_;
    const std::string metadata_connstring_;

    // For high availability
    MasterViewHelper master_view_helper_;
    std::thread heartbeat_thread_;
    std::atomic<bool> heartbeat_running_{false};
    std::condition_variable heartbeat_cv_;
    std::mutex heartbeat_mtx_;
    std::atomic<ViewVersionId> view_version_{0};
    bool connection_interrupted_ = false;

    // Shutdown protection
    SharedMutex running_rw_mtx_;
    bool is_running_ GUARDED_BY(running_rw_mtx_) = false;

    // Metrics HTTP server
    std::unique_ptr<coro_http::coro_http_server> metrics_http_server_;
    uint16_t metrics_port_ = 0;
    bool enable_metrics_http_ = true;

    // ---- P2P members ----
    P2PMasterClient master_client_;
    uint16_t client_rpc_port_ = 12345;

    std::unique_ptr<coro_rpc::coro_rpc_server> client_rpc_server_;
    std::thread client_rpc_server_thread_;
    std::optional<DataManager> data_manager_;
    std::optional<ClientRpcService> client_rpc_service_;

    std::mutex peer_clients_mutex_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    std::optional<RouteCache> route_cache_;
    std::unique_ptr<AsyncMetadataNotifier> async_route_notifier_;
    std::unique_ptr<HARecoveryManager> ha_manager_;
};

}  // namespace mooncake
