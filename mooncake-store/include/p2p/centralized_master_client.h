#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_client.h"            // main's concrete MasterClient
#include "master_client_interface.h"  // shared interface
#include "master_metric_manager.h"
#include "p2p_rpc_types.h"            // ReadRouteConfig (accepted, dropped in delegation)
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Centralized master client that wraps main's concrete MasterClient.
 *
 * Composes (not inherits) main MasterClient to avoid modifying it.
 * Adapts interface mismatches:
 *  - GetReplicaList/BatchGetReplicaList accept ReadRouteConfig but drop it
 *    (main master doesn't use route config).
 *  - No RegisterClient (CentralizedClientService::RegisterClient uses Ping).
 *  - No task methods (CopyStart/MoveStart/... — main master has no such RPC).
 *  - Exposes Ping() for the CentralizedClientService Ping thread.
 */
class CentralizedMasterClient final : public MasterClientInterface {
   public:
    CentralizedMasterClient(const UUID& client_id,
                            MasterClientMetric* metrics = nullptr)
        : master_client_(std::make_shared<mooncake::MasterClient>(client_id,
                                                                   metrics)) {}

    CentralizedMasterClient(const CentralizedMasterClient&) = delete;
    CentralizedMasterClient& operator=(const CentralizedMasterClient&) = delete;

    // ---- MasterClientInterface ----
    ErrorCode Connect(const std::string& master_addr) override {
        return master_client_->Connect(master_addr);
    }

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids) override {
        return master_client_->BatchQueryIp(client_ids);
    }

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str) override {
        return master_client_->GetReplicaListByRegex(str);
    }

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() override {
        return master_client_->CalcCacheStats();
    }

    // ---- Centralized-specific methods (delegated to main MasterClient) ----

    tl::expected<bool, ErrorCode> ExistKey(const std::string& key) {
        return master_client_->ExistKey(key);
    }

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys) {
        return master_client_->BatchExistKey(keys);
    }

    // Accept ReadRouteConfig but drop it (main master doesn't use route config)
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key, const ReadRouteConfig& /*config*/ = {}) {
        return master_client_->GetReplicaList(key);
    }

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const ReadRouteConfig& /*config*/ = {}) {
        return master_client_->BatchGetReplicaList(keys);
    }

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        const ReplicateConfig& config) {
        return master_client_->PutStart(key, slice_lengths, config);
    }

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config) {
        return master_client_->BatchPutStart(keys, slice_lengths, config);
    }

    tl::expected<void, ErrorCode> PutEnd(const std::string& key,
                                         ReplicaType replica_type) {
        return master_client_->PutEnd(key, replica_type);
    }

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys) {
        return master_client_->BatchPutEnd(keys);
    }

    tl::expected<void, ErrorCode> PutRevoke(const std::string& key,
                                            ReplicaType replica_type) {
        return master_client_->PutRevoke(key, replica_type);
    }

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys) {
        return master_client_->BatchPutRevoke(keys);
    }

    tl::expected<void, ErrorCode> Remove(const std::string& key) {
        return master_client_->Remove(key);
    }

    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str) {
        return master_client_->RemoveByRegex(str);
    }

    tl::expected<long, ErrorCode> RemoveAll() {
        return master_client_->RemoveAll();
    }

    tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment) {
        return master_client_->MountSegment(segment);
    }

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id) {
        return master_client_->UnmountSegment(segment_id);
    }

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name) {
        return master_client_->BatchReplicaClear(object_keys, client_id,
                                                  segment_name);
    }

    tl::expected<std::string, ErrorCode> GetFsdir() {
        return master_client_->GetFsdir();
    }

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig() {
        return master_client_->GetStorageConfig();
    }

    tl::expected<PingResponse, ErrorCode> Ping() {
        return master_client_->Ping();
    }

    tl::expected<void, ErrorCode> MountLocalDiskSegment(
        const UUID& client_id, bool enable_offloading) {
        return master_client_->MountLocalDiskSegment(client_id,
                                                      enable_offloading);
    }

    tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading) {
        return master_client_->OffloadObjectHeartbeat(client_id,
                                                       enable_offloading);
    }

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas) {
        return master_client_->NotifyOffloadSuccess(client_id, keys, metadatas);
    }

    UUID get_client_id() const {
        return master_client_->get_client_id();
    }

   private:
    std::shared_ptr<mooncake::MasterClient> master_client_;
};

}  // namespace mooncake
