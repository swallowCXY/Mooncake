#pragma once

#include <coroutine>
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <cstdlib>
#include <boost/functional/hash.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>

#include "client_metric.h"
#include "master_client_interface.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"  // GetReplicaListResponse (shared)
#include "types.h"

namespace mooncake {

template <auto Method>
struct RpcNameTraits;

static const std::string kP2PDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake P2P master service.
 *
 * Flattened from p2p-branch MasterClient (base) + P2PMasterClient (subclass).
 * No base class, no virtual methods. The central master client (MasterClient
 * in master_client.h) is untouched and independent. Implements
 * MasterClientInterface so the ClientService base can talk to it uniformly.
 */
class P2PMasterClient : public MasterClientInterface {
   public:
    P2PMasterClient(const UUID& client_id, MasterClientMetric* metrics = nullptr)
        : client_id_(client_id), metrics_(metrics) {
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config
            pool_conf{};
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            pool_conf.client_config.socket_config =
                coro_io::ib_socket_t::config_t{};
        }
        client_pools_ =
            std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
                pool_conf);
    }

    ~P2PMasterClient() = default;

    P2PMasterClient(const P2PMasterClient&) = delete;
    P2PMasterClient& operator=(const P2PMasterClient&) = delete;

    /**
     * @brief Connects to the master service
     */
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kP2PDefaultMasterAddress) override;

    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        const std::string& object_key);
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys);
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& key,
                   const GetReplicaListRequestConfig& config =
                       GetReplicaListRequestConfig());
    [[nodiscard]] async_simple::coro::Lazy<
        tl::expected<GetReplicaListResponse, ErrorCode>>
    AsyncGetReplicaList(const std::string& key,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig());
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig());
    [[nodiscard]] tl::expected<MasterMetricManager::CacheHitStatDict,
                               ErrorCode>
    CalcCacheStats() override;
    [[nodiscard]] tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids) override;
    [[nodiscard]] tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str) override;
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key);
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str);
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll();
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id);
    [[nodiscard]] tl::expected<QueryClientStatusResponse, ErrorCode>
    QueryClientStatus(const UUID& client_id);
    [[nodiscard]] tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment);
    [[nodiscard]] tl::expected<RegisterClientResponse, ErrorCode>
    RegisterClient(const RegisterClientRequest& req);

    // ---- P2P-specific ----
    [[nodiscard]] tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);
    [[nodiscard]] tl::expected<BatchGetWriteRouteResponse, ErrorCode>
    BatchGetWriteRoute(const BatchGetWriteRouteRequest& req);
    [[nodiscard]] tl::expected<void, ErrorCode> AddReplica(
        const AddReplicaRequest& req);
    [[nodiscard]] tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>>
    BatchRemoveReplica(const BatchRemoveReplicaRequest& req);
    [[nodiscard]] tl::expected<BatchSyncReplicaResponse, ErrorCode>
    BatchSyncReplica(const BatchSyncReplicaRequest& req);
    [[nodiscard]] tl::expected<void, ErrorCode> SetSyncCompleted(
        UUID client_id);

   protected:
    /**
     * @brief Generic RPC invocation helper for single-result operations
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>>
    invoke_rpc_async(Args&&... args) {
        auto pool = client_accessor_.GetClientPool();

        // Increment RPC counter
        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        auto ret = co_await pool->send_request(
            [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
                return client.send_request<ServiceMethod>(
                    std::forward<Args>(args)...);
            });
        if (!ret.has_value()) {
            LOG(ERROR) << "Client not available";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        auto result = co_await std::move(ret.value());
        if (!result) {
            LOG(ERROR) << "RPC call failed: " << result.error().msg;
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        if (metrics_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time);
            metrics_->rpc_latency.observe({RpcNameTraits<ServiceMethod>::value},
                                          latency.count());
        }
        co_return result->result();
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args) {
        return async_simple::coro::syncAwait(
            invoke_rpc_async<ServiceMethod, ReturnType>(
                std::forward<Args>(args)...));
    }

    /**
     * @brief Generic RPC invocation helper for batch operations
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args) {
        auto pool = client_accessor_.GetClientPool();

        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        return async_simple::coro::syncAwait(
            [&]() -> async_simple::coro::Lazy<
                      std::vector<tl::expected<ResultType, ErrorCode>>> {
                auto ret = co_await pool->send_request(
                    [&](coro_io::client_reuse_hint,
                        coro_rpc::coro_rpc_client& client) {
                        return client.send_request<ServiceMethod>(
                            std::forward<Args>(args)...);
                    });
                if (!ret.has_value()) {
                    LOG(ERROR) << "Client not available";
                    co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                        input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                auto result = co_await std::move(ret.value());
                if (!result) {
                    LOG(ERROR)
                        << "Batch RPC call failed: " << result.error().msg;
                    std::vector<tl::expected<ResultType, ErrorCode>>
                        error_results;
                    error_results.reserve(input_size);
                    for (size_t i = 0; i < input_size; ++i) {
                        error_results.emplace_back(
                            tl::make_unexpected(ErrorCode::RPC_FAIL));
                    }
                    co_return error_results;
                }
                if (metrics_) {
                    auto end_time = std::chrono::steady_clock::now();
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time);
                    metrics_->rpc_latency.observe(
                        {RpcNameTraits<ServiceMethod>::value}, latency.count());
                }
                co_return result->result();
            }());
    }

    /**
     * @brief Accessor for the coro_rpc_client pool.
     */
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

   protected:
    RpcClientAccessor client_accessor_;

    const UUID client_id_;
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    mutable Mutex connect_mutex_;
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
