#pragma once

#include <array>
#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "master_config.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "p2p_client_manager.h"
#include "p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"  // GetReplicaListResponse (shared with central)
#include "types.h"

namespace mooncake {

// Flattened from p2p-branch MasterService (abstract base) + P2PMasterService
// (subclass). No base class, no virtual methods — the central master's
// MasterService (in master_service.h) is untouched and independent.
class P2PMasterService {
   public:
    explicit P2PMasterService(const MasterServiceConfig& config);
    ~P2PMasterService() = default;

    void InitializeClientManager();

    // ---- Common RPC methods (absorbed from the former base class) ----
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto ExistKey(const std::string& key) -> tl::expected<bool, ErrorCode>;
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto BatchQueryIp(const std::vector<UUID>& client_ids)
        -> tl::expected<
            std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
            ErrorCode>;
    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;
    auto GetReplicaList(const std::string& key,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig())
        -> tl::expected<GetReplicaListResponse, ErrorCode>;
    auto Remove(const std::string& key) -> tl::expected<void, ErrorCode>;
    auto RemoveByRegex(const std::string& str) -> tl::expected<long, ErrorCode>;
    long RemoveAll();
    size_t GetKeyCount() const;

    // ---- P2P-specific RPC methods ----
    auto GetWriteRoute(const WriteRouteRequest& req)
        -> tl::expected<WriteRouteResponse, ErrorCode>;
    auto BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
        -> BatchGetWriteRouteResponse;
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;
    auto BatchSyncReplica(const BatchSyncReplicaRequest& req)
        -> BatchSyncReplicaResponse;
    auto SetSyncCompleted(UUID client_id) -> tl::expected<void, ErrorCode>;

   private:
    struct ObjectMetadata {
        ObjectMetadata(size_t value_length, std::vector<Replica>&& reps);
        ~ObjectMetadata();
        ObjectMetadata() = delete;
        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        bool IsValid() const { return !replicas_.empty() && size_ > 0; }

        bool IsObjectAccessible() const {
            for (const auto& replica : replicas_) {
                if (IsReplicaAccessible(replica)) {
                    return true;
                }
            }
            return false;
        }

        tl::expected<void, ErrorCode> IsObjectRemovable() const { return {}; }

        bool IsReplicaAccessible(const Replica& replica) const {
            (void)replica;
            return true;
        }

        tl::expected<void, ErrorCode> IsReplicaRemovable(
            const Replica& replica) const {
            (void)replica;
            return {};
        }

        std::vector<Replica> replicas_;
        size_t size_;
    };

    struct MetadataShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>>
            metadata GUARDED_BY(mutex);

        // segment_id -> { key -> replica_reference_count }.
        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };

    // Helper class for accessing metadata with automatic locking
    class MetadataAccessor {
       public:
        MetadataAccessor(P2PMasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->GetShardIndex(key)),
              shard_(service_->GetShard(shard_idx_)),
              lock_(&shard_.mutex),
              it_(shard_.metadata.find(key)) {}

        ~MetadataAccessor() = default;

        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_.metadata.end();
        }

        MetadataShard& GetShard() NO_THREAD_SAFETY_ANALYSIS { return shard_; }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ != shard_.metadata.end()) {
                service_->RemoveReplicaFromSegmentIndex(shard_, it_->first,
                                                        it_->second->replicas_);
                shard_.metadata.erase(it_);
                it_ = shard_.metadata.end();
            }
        }

       protected:
        P2PMasterService* service_;
        std::string key_;
        size_t shard_idx_;
        MetadataShard& shard_;
        MutexLocker lock_;
        std::unordered_map<std::string,
                           std::unique_ptr<ObjectMetadata>>::iterator it_;
    };

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    MetadataShard& GetShard(size_t idx) { return metadata_shards_[idx]; }
    const MetadataShard& GetShard(size_t idx) const {
        return metadata_shards_[idx];
    }
    size_t GetShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }
    size_t GetShardCount() const { return kNumShards; }

    std::unique_ptr<MetadataAccessor> GetMetadataAccessor(
        const std::string& key) {
        return std::make_unique<MetadataAccessor>(this, key);
    }

    void AddReplicaToSegmentIndex(MetadataShard& shard, const std::string& key,
                                   const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const std::vector<Replica>& replicas) NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(MetadataShard& shard,
                                       const std::string& key,
                                       const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;

    void OnObjectRemoved(ObjectMetadata& metadata);
    void OnSegmentRemoved(const UUID& segment_id);

    // Hooks (concrete, were virtual overrides in the p2p subclass)
    void OnObjectAccessed(ObjectMetadata& metadata);
    void OnObjectHit(const ObjectMetadata& metadata);
    void OnReplicaRemoved(const Replica& replica);
    void OnReplicaAdded(const Replica& replica);

    std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata);

    ClientManager& GetClientManager() { return *client_manager_; }
    const ClientManager& GetClientManager() const { return *client_manager_; }

   private:
    tl::expected<void, ErrorCode> InnerAddReplica(
        MetadataShard& shard, const std::string& key, const UUID& client_id,
        const UUID& segment_id, size_t size,
        const std::shared_ptr<P2PClientMeta>& client) NO_THREAD_SAFETY_ANALYSIS;
    tl::expected<void, ErrorCode> InnerRemoveReplica(
        MetadataShard& shard, const std::string& key, const UUID& client_id,
        const UUID& segment_id) NO_THREAD_SAFETY_ANALYSIS;

    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<MetadataShard, kNumShards> metadata_shards_;
    // for the number of replicas of a key:
    // 1. max_replicas_per_key_ == 0 means no limitation
    // 2. max_replicas_per_key_ > 0 means the max replica number of a key
    uint64_t max_replicas_per_key_;
    const bool enable_ha_;
    ViewVersionId view_version_;

    friend class MetadataAccessor;
};

}  // namespace mooncake
