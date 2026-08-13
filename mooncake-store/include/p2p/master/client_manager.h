#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "client_meta.h"
#include "heartbeat_type.h"
#include "mutex.h"
#include "p2p_rpc_types.h"
#include "types.h"

namespace mooncake {

class ClientIterator {
   public:
    virtual ~ClientIterator() = default;

    std::shared_ptr<ClientMeta> Next() {
        if (index_ < clients_.size()) {
            return clients_[index_++];
        }
        return nullptr;
    }

   protected:
    ClientIterator() = default;

    std::vector<std::shared_ptr<ClientMeta>> clients_;
    size_t index_ = 0;
};

class OrderedClientIterator : public ClientIterator {
   public:
    OrderedClientIterator(
        const std::unordered_map<UUID, std::shared_ptr<ClientMeta>,
                                 boost::hash<UUID>>& client_metas) {
        clients_.reserve(client_metas.size());
        for (const auto& [id, meta] : client_metas) {
            clients_.emplace_back(meta);
        }
    }
};

class RandomClientIterator : public ClientIterator {
   public:
    RandomClientIterator(
        const std::unordered_map<UUID, std::shared_ptr<ClientMeta>,
                                 boost::hash<UUID>>& client_metas) {
        clients_.reserve(client_metas.size());
        for (const auto& [id, meta] : client_metas) {
            clients_.emplace_back(meta);
        }
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(clients_.begin(), clients_.end(), g);
    }
};

/**
 * @brief ClientManager is a base class for managing clients' lifecycle and
 * heartbeat with a three-state state machine (HEALTH/DISCONNECTION/CRASHED).
 */
class ClientManager {
   public:
    ClientManager(const int64_t disconnect_timeout_sec,
                  const int64_t crash_timeout_sec,
                  const ViewVersionId view_version);
    virtual ~ClientManager();

    void Start();
    void Stop();

    void StartClientMonitor();
    void StopClientMonitor();

    /**
     * @brief Register a client with its segments.
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    /**
     * @brief Process a heartbeat from a client.
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    auto QuerySegment(const UUID& client_id, const UUID& segment_id)
        -> tl::expected<std::shared_ptr<Segment>, ErrorCode>;

    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) -> std::shared_ptr<ClientMeta>;
    auto GetAllClients() -> std::vector<std::shared_ptr<ClientMeta>>;

    using ClientVisitor = std::function<tl::expected<bool, ErrorCode>(
        const std::shared_ptr<ClientMeta>& client)>;
    auto ForEachClient(ObjectIterateStrategy strategy,
                       const ClientVisitor& visitor)
        -> tl::expected<void, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   protected:
    void ClientMonitorFunc();

    virtual HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                            const HeartbeatTask& task) = 0;

    virtual std::unique_ptr<ClientIterator> InnerBuildClientIterator(
        ObjectIterateStrategy strategy);

    virtual std::shared_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) = 0;

    virtual void OnClientRegistered(
        const std::shared_ptr<ClientMeta>& /*meta*/) {}

    virtual DeploymentMode GetDeploymentMode() const = 0;

   protected:
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks

    mutable SharedMutex clients_mutex_;
    // Client metadata: client_id -> metadata (including health state)
    std::unordered_map<UUID, std::shared_ptr<ClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(clients_mutex_);
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    const ViewVersionId view_version_;  // Passed from MasterService
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
