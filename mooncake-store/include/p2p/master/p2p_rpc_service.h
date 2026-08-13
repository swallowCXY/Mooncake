#pragma once

#include <atomic>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include <boost/functional/hash.hpp>

#include "master_config.h"
#include "master_metric_manager.h"
#include "p2p_master_service.h"
#include "p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"  // GetReplicaListResponse (shared)
#include "types.h"

namespace mooncake {

static const uint64_t kP2PMetricReportIntervalSeconds = 10;

// Self-contained RPC wrapper for the P2P master. Does NOT inherit the central
// WrappedMasterService (which owns a central MasterService directly). Owns a
// P2PMasterService and exposes both common and P2P-specific RPC handlers.
class WrappedP2PMasterService {
   public:
    explicit WrappedP2PMasterService(const WrappedMasterServiceConfig& config);
    ~WrappedP2PMasterService();

    void init_http_server();

    // ---- common handlers (delegate to master_service_) ----
    tl::expected<bool, ErrorCode> ExistKey(const std::string& key);
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);
    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);
    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key,
        const GetReplicaListRequestConfig& config =
            GetReplicaListRequestConfig());
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(
        const std::vector<std::string>& keys,
        const GetReplicaListRequestConfig& config =
            GetReplicaListRequestConfig());
    tl::expected<void, ErrorCode> Remove(const std::string& key);
    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str);
    long RemoveAll();
    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);
    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);
    tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);
    tl::expected<QueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const QueryClientStatusRequest& req);
    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient(
        const RegisterClientRequest& req);
    tl::expected<std::string, ErrorCode> ServiceReady();

    // ---- P2P-specific handlers ----
    tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);
    BatchGetWriteRouteResponse BatchGetWriteRoute(
        const BatchGetWriteRouteRequest& req);
    tl::expected<void, ErrorCode> AddReplica(const AddReplicaRequest& req);
    tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);
    std::vector<tl::expected<void, ErrorCode>> BatchRemoveReplica(
        const BatchRemoveReplicaRequest& req);
    BatchSyncReplicaResponse BatchSyncReplica(
        const BatchSyncReplicaRequest& req);
    tl::expected<void, ErrorCode> SetSyncCompleted(UUID client_id);

   private:
    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
