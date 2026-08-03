#include "p2p_master_service.h"

#include <algorithm>
#include <cassert>
#include <regex>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "p2p_client_meta.h"

namespace mooncake {

// ===================== ObjectMetadata =====================

P2PMasterService::ObjectMetadata::~ObjectMetadata() {
    MasterMetricManager::instance().dec_key_count(1);
}

P2PMasterService::ObjectMetadata::ObjectMetadata(size_t value_length,
                                                 std::vector<Replica>&& reps)
    : replicas_(std::move(reps)), size_(value_length) {
    MasterMetricManager::instance().inc_key_count(1);
    MasterMetricManager::instance().observe_value_size(value_length);
}

// ===================== Construction =====================

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : max_replicas_per_key_(config.max_replicas_per_key),
      enable_ha_(config.enable_ha),
      view_version_(config.view_version) {
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_live_ttl_sec, config.client_crashed_ttl_sec,
        config.view_version);
    InitializeClientManager();
    client_manager_->Start();
}

void P2PMasterService::InitializeClientManager() {
    GetClientManager().SetSegmentRemovalCallback(
        [this](const UUID& segment_id) { this->OnSegmentRemoved(segment_id); });
}

// ===================== Segment management =====================

auto P2PMasterService::MountSegment(const Segment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = GetClientManager().GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "MountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto result = client->MountSegment(segment);
    if (!result) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment=" << segment.name
                   << ", client_id=" << client_id << ", ret=" << result.error();
        return result;
    }
    return {};
}

auto P2PMasterService::UnmountSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = GetClientManager().GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "UnmountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto result = client->UnmountSegment(segment_id);
    if (!result) {
        LOG(ERROR) << "fail to unmount segment"
                   << ", segment_id=" << segment_id
                   << ", client_id=" << client_id << ", ret=" << result.error();
        return result;
    }
    return {};
}

void P2PMasterService::OnObjectRemoved(ObjectMetadata& metadata) {
    for (auto& replica : metadata.replicas_) {
        OnReplicaRemoved(replica);
    }
}

// As a callback, called by SegmentManager when a segment is removed:
// 1. remove reverse index of segment
// 2. remove the replica in metadata about the segment
void P2PMasterService::OnSegmentRemoved(const UUID& segment_id) {
    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        auto idx_it = shard.segment_key_index.find(segment_id);
        if (idx_it == shard.segment_key_index.end()) {
            continue;
        }

        // the reverse index using string_view acquired from metadata.
        // before remove the reverse index, we should acquire the key copier.
        std::vector<std::string> affected_keys;
        affected_keys.reserve(idx_it->second.size());
        for (const auto& item : idx_it->second) {
            affected_keys.emplace_back(item.first);
        }

        // 1. Remove the segment from the reverse index
        shard.segment_key_index.erase(idx_it);

        // 2. Remove the replica in metadata about the segment
        for (const auto& key : affected_keys) {
            auto meta_it = shard.metadata.find(key);
            if (meta_it == shard.metadata.end()) {
                continue;
            }

            auto& metadata = *meta_it->second;
            auto& replicas = metadata.replicas_;

            for (int k = replicas.size() - 1; k >= 0; --k) {
                auto id = replicas[k].get_segment_id();
                if (id.has_value() && id.value() == segment_id) {
                    OnReplicaRemoved(replicas[k]);
                    replicas.erase(replicas.begin() + k);
                    break;
                }
            }

            if (replicas.empty()) {
                OnObjectRemoved(metadata);
                shard.metadata.erase(meta_it);
            }
        }
    }  // end for
}

void P2PMasterService::AddReplicaToSegmentIndex(MetadataShard& shard,
                                                const std::string& key,
                                                const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }
    auto seg_id = replica.get_segment_id();
    if (seg_id.has_value()) {
        shard.segment_key_index[seg_id.value()][std::string_view(key)]++;
    }
}

void P2PMasterService::RemoveReplicaFromSegmentIndex(
    MetadataShard& shard, const std::string& key,
    const std::vector<Replica>& replicas) {
    for (const auto& replica : replicas) {
        RemoveReplicaFromSegmentIndex(shard, key, replica);
    }
}

void P2PMasterService::RemoveReplicaFromSegmentIndex(MetadataShard& shard,
                                                     const std::string& key,
                                                     const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }

    auto seg_id = replica.get_segment_id();
    if (seg_id.has_value()) {
        auto seg_it = shard.segment_key_index.find(seg_id.value());
        if (seg_it != shard.segment_key_index.end()) {
            auto key_it = seg_it->second.find(key);
            if (key_it != seg_it->second.end()) {
                if (--key_it->second == 0) {
                    seg_it->second.erase(key_it);
                }
                if (seg_it->second.empty()) {
                    shard.segment_key_index.erase(seg_it);
                }
            } else {
                LOG(WARNING)
                    << "RemoveReplicaFromSegmentIndex: key not found"
                    << ", segment_id=" << seg_id.value() << ", key=" << key;
            }
        } else {
            LOG(WARNING) << "RemoveReplicaFromSegmentIndex: segment not found"
                         << ", segment_id=" << seg_id.value()
                         << ", key=" << key;
        }
    }
}

// ===================== Client lifecycle =====================

auto P2PMasterService::RegisterClient(const RegisterClientRequest& req)
    -> tl::expected<RegisterClientResponse, ErrorCode> {
    return GetClientManager().RegisterClient(req);
}

auto P2PMasterService::Heartbeat(const HeartbeatRequest& req)
    -> tl::expected<HeartbeatResponse, ErrorCode> {
    return GetClientManager().Heartbeat(req);
}

auto P2PMasterService::QueryClientStatus(const QueryClientStatusRequest& req)
    -> tl::expected<QueryClientStatusResponse, ErrorCode> {
    return GetClientManager().QueryClientStatus(req);
}

// ===================== Key queries =====================

auto P2PMasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    auto accessor = GetMetadataAccessor(key);
    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor->Get();
    if (metadata.IsObjectAccessible()) {
        OnObjectAccessed(metadata);
        return true;
    }

    return false;
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto P2PMasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < GetShardCount(); i++) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);
        for (const auto& item : shard.metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto P2PMasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = GetClientManager().GetAllSegments();
    if (!result.has_value()) {
        LOG(ERROR) << "fail to get all segments"
                   << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::GetClientSegments(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = GetClientManager().GetClientSegments(client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to get client segments"
                   << ", client_id=" << client_id << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    auto result = GetClientManager().QuerySegments(segment);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", segment=" << segment << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = GetClientManager().QueryIp(client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query ip"
                   << ", client_id=" << client_id << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        } else {
            LOG(WARNING) << "fail to query ip"
                         << ", client_id=" << client_id
                         << ", ret=" << ip_result.error();
        }
    }
    return results;
}

auto P2PMasterService::GetReplicaListByRegex(const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        for (auto& [key, metadata] : shard.metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                replica_list.reserve(metadata->replicas_.size());
                for (const auto& replica : metadata->replicas_) {
                    if (metadata->IsReplicaAccessible(replica)) {
                        replica_list.emplace_back(replica.get_descriptor());
                    }
                }
                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                OnObjectHit(*metadata);
                OnObjectAccessed(*metadata);
            }
        }
    }

    return results;
}

auto P2PMasterService::GetReplicaList(const std::string& key,
                                      const GetReplicaListRequestConfig& config)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    auto accessor = GetMetadataAccessor(std::string(key));

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor->Get();

    std::vector<Replica::Descriptor> replica_list =
        FilterReplicas(config, metadata);

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    OnObjectHit(metadata);
    OnObjectAccessed(metadata);

    GetReplicaListResponse resp;
    resp.replicas = std::move(replica_list);
    return resp;
}

auto P2PMasterService::Remove(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(key);
    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor->Get();

    if (auto res = metadata.IsObjectRemovable(); !res) {
        VLOG(1) << "key=" << key << ", error=" << res.error();
        return tl::make_unexpected(res.error());
    }

    // Remove object metadata
    OnObjectRemoved(metadata);
    accessor->Erase();
    return {};
}

auto P2PMasterService::RemoveByRegex(const std::string& regex_pattern)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        for (auto it = shard.metadata.begin(); it != shard.metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!it->second->IsObjectRemovable()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but object is not removable";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                OnObjectRemoved(*it->second);
                RemoveReplicaFromSegmentIndex(shard, it->first,
                                               it->second->replicas_);
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long P2PMasterService::RemoveAll() {
    long removed_count = 0;

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second->IsObjectRemovable()) {
                OnObjectRemoved(*it->second);
                RemoveReplicaFromSegmentIndex(shard, it->first,
                                               it->second->replicas_);
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count;
    return removed_count;
}

size_t P2PMasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < GetShardCount(); ++i) {
        const auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);
        total += shard.metadata.size();
    }
    return total;
}

// ===================== P2P-specific: write route =====================

std::vector<Replica::Descriptor> P2PMasterService::FilterReplicas(
    const GetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    const auto& p2p_config = config.p2p_config ? config.p2p_config.value()
                                               : P2PGetReplicaListConfigExtra();
    std::vector<std::pair<uint32_t, Replica::Descriptor>> candidates;
    // 1. filter qualified replicas
    for (const auto& replica : metadata.replicas_) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "invalid replica type"
                       << ", replica: " << replica;
            continue;
        } else if (!replica.get_p2p_client()->is_health()) {
            // The client of the replica might be disconnected, just skip it.
            continue;
        }

        // filter with config
        // 1.1 tag filter: exclude replicas whose segment contains
        // any tag listed in tag_filters.
        bool excluded_by_tag = false;
        const auto& p2p_tags = replica.get_p2p_tags();
        for (const auto& tag : p2p_config.tag_filters) {
            if (std::find(p2p_tags.begin(), p2p_tags.end(), tag) !=
                p2p_tags.end()) {
                excluded_by_tag = true;
                break;
            }
        }
        if (excluded_by_tag) continue;

        // 1.2 priority filter
        auto priority_opt = replica.get_p2p_priority();
        if (!priority_opt) {
            LOG(ERROR) << "invalid priority"
                       << ", replica: " << replica;
            continue;
        }
        if (*priority_opt < p2p_config.priority_limit) continue;

        candidates.push_back({*priority_opt, replica.get_descriptor()});
    }  // iter replicas over

    if (config.max_candidates ==
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        // return all candidates
        std::vector<Replica::Descriptor> result;
        result.reserve(candidates.size());
        for (const auto& p : candidates) {
            result.push_back(p.second);
        }
        return result;
    }

    // 2. the number of qualified replicas is larger than limit,
    // choose the best ones.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<Replica::Descriptor> result;
    result.reserve(config.max_candidates);
    for (size_t i = 0; i < config.max_candidates; ++i) {
        result.push_back(candidates[i].second);
    }
    return result;
}

auto P2PMasterService::GetWriteRoute(const WriteRouteRequest& req)
    -> tl::expected<WriteRouteResponse, ErrorCode> {
    // pre check replica num.
    if (!req.key.empty() && max_replicas_per_key_ > 0) {
        auto accessor = GetMetadataAccessor(req.key);
        if (accessor->Exists()) {
            auto& metadata = accessor->Get();
            if (metadata.replicas_.size() >= max_replicas_per_key_) {
                LOG(WARNING)
                    << "replica num exceeded"
                    << ", key: " << req.key << ", client_id: " << req.client_id
                    << ", current replica num:" << metadata.replicas_.size()
                    << ", max replica num: " << max_replicas_per_key_;
                return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
            }
        }
    }

    std::vector<WriteCandidate> candidates;
    // find qualified segments across all clients
    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            auto p2p_client = std::static_pointer_cast<P2PClientMeta>(client);
            if (!p2p_client) {
                LOG(ERROR) << "unexpected client meta type";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return p2p_client->CollectWriteRouteCandidates(req, candidates);
        });

    WriteRouteResponse response;
    if (candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.size;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else {
        std::sort(candidates.begin(), candidates.end(),
                  [](const auto& a, const auto& b) {
                      return a.priority > b.priority;
                  });
        if (req.config.max_candidates !=
                WriteRouteRequestConfig::RETURN_ALL_CANDIDATES &&
            candidates.size() > req.config.max_candidates) {
            candidates.resize(req.config.max_candidates);
        }
        response.candidates = std::move(candidates);
    }
    return response;
}

auto P2PMasterService::BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
    -> BatchGetWriteRouteResponse {
    const size_t n = req.keys.size();
    BatchGetWriteRouteResponse response;
    response.responses.resize(n);
    response.error_codes.resize(n, ErrorCode::OK);

    if (req.keys.size() != req.sizes.size()) {
        std::fill(response.error_codes.begin(), response.error_codes.end(),
                  ErrorCode::INVALID_PARAMS);
        return response;
    }

    WriteRouteRequest single_req;
    single_req.client_id = req.client_id;
    single_req.config = req.config;
    for (size_t i = 0; i < n; ++i) {
        single_req.key = req.keys[i];
        single_req.size = req.sizes[i];
        auto result = GetWriteRoute(single_req);
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

// ===================== P2P-specific: replica sync =====================

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);
    auto client = std::static_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(req.client_id));
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return InnerAddReplica(accessor->GetShard(), req.key, req.client_id,
                           req.segment_id, req.size, client);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerAddReplica(
    MetadataShard& shard, const std::string& key, const UUID& client_id,
    const UUID& segment_id, size_t size,
    const std::shared_ptr<P2PClientMeta>& client) {
    auto segment_res = client->QuerySegment(segment_id);
    if (!segment_res.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << client_id
                   << ", segment_id: " << segment_id;
        return tl::make_unexpected(segment_res.error());
    }

    Replica new_replica(P2PProxyReplicaData(client, segment_res.value(), size),
                        ReplicaStatus::COMPLETE);

    auto it = shard.metadata.find(key);
    if (it != shard.metadata.end()) {
        auto& metadata = *it->second;
        if (max_replicas_per_key_ > 0 &&
            metadata.replicas_.size() >= max_replicas_per_key_) {
            LOG(WARNING) << "replica num exceeded"
                         << ", key: " << key << ", client_id: " << client_id
                         << ", segment_id: " << segment_id
                         << ", current replica num:" << max_replicas_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
        for (const auto& replica : metadata.replicas_) {
            if (!replica.is_p2p_proxy_replica()) {
                LOG(ERROR) << "unexpected replica type"
                           << ", key: " << key
                           << ", request client_id: " << client_id
                           << ", request segment_id: " << segment_id
                           << ", replica:" << replica;
                return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
            }
            auto seg_id = replica.get_segment_id();
            auto cli_id = replica.get_p2p_client_id();
            if (cli_id && seg_id && cli_id == client_id &&
                *seg_id == segment_id) {
                LOG(WARNING) << "replica has existed"
                             << ", key: " << key << ", client_id: " << client_id
                             << ", segment_id: " << segment_id;
                return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
            }
        }
        AddReplicaToSegmentIndex(shard, key, new_replica);
        OnReplicaAdded(new_replica);
        metadata.replicas_.push_back(std::move(new_replica));
    } else {
        std::vector<Replica> replicas;
        replicas.push_back(std::move(new_replica));
        auto new_meta =
            std::make_unique<ObjectMetadata>(size, std::move(replicas));
        auto emplace_it =
            shard.metadata.emplace(key, std::move(new_meta)).first;
        AddReplicaToSegmentIndex(shard, emplace_it->first,
                                 emplace_it->second->replicas_[0]);
        OnReplicaAdded(emplace_it->second->replicas_[0]);
    }
    return {};
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);
    return InnerRemoveReplica(accessor->GetShard(), req.key, req.client_id,
                              req.segment_id);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerRemoveReplica(
    MetadataShard& shard, const std::string& key, const UUID& client_id,
    const UUID& segment_id) {
    auto it = shard.metadata.find(key);
    if (it == shard.metadata.end()) {
        LOG(WARNING) << "object not found"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = *it->second;
    for (auto rit = metadata.replicas_.begin(); rit != metadata.replicas_.end();
         ++rit) {
        if (!rit->is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type"
                       << ", key: " << key << ", client_id: " << client_id
                       << ", segment_id: " << segment_id
                       << ", replica: " << *rit;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto seg_id = rit->get_segment_id();
        auto cli_id = rit->get_p2p_client_id();
        if (cli_id && seg_id && cli_id == client_id && *seg_id == segment_id) {
            RemoveReplicaFromSegmentIndex(shard, key, *rit);
            OnReplicaRemoved(*rit);
            metadata.replicas_.erase(rit);
            if (metadata.replicas_.empty()) {
                OnObjectRemoved(metadata);
                shard.metadata.erase(it);
            }
            return {};
        }
    }

    LOG(WARNING) << "replica not found"
                 << ", key: " << key << ", client_id: " << client_id
                 << ", segment_id: " << segment_id;
    return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
}

auto P2PMasterService::BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(req.segment_ids.size());

    RemoveReplicaRequest single_req;
    single_req.key = req.key;
    single_req.client_id = req.client_id;
    for (const auto& segment_id : req.segment_ids) {
        single_req.segment_id = segment_id;
        auto result = RemoveReplica(single_req);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                LOG(INFO) << "object not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else if (result.error() == ErrorCode::REPLICA_NOT_FOUND) {
                LOG(INFO) << "replica not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else {
                LOG(ERROR) << "failed to remove replica"
                           << ", key: " << req.key
                           << ", client_id: " << req.client_id
                           << ", segment_id: " << segment_id
                           << ", error: " << toString(result.error());
                results.push_back(tl::make_unexpected(result.error()));
            }
        } else {
            results.push_back({});
        }
    }
    return results;
}

auto P2PMasterService::BatchSyncReplica(const BatchSyncReplicaRequest& req)
    -> BatchSyncReplicaResponse {
    // Validate SoA array lengths are consistent
    if (req.add_keys.size() != req.add_sizes.size() ||
        req.add_keys.size() != req.add_segment_ids.size() ||
        req.remove_keys.size() != req.remove_segment_ids.size()) {
        LOG(ERROR) << "BatchSyncReplica: mismatched array sizes"
                   << ", add_keys=" << req.add_keys.size()
                   << ", add_sizes=" << req.add_sizes.size()
                   << ", add_segment_ids=" << req.add_segment_ids.size()
                   << ", remove_keys=" << req.remove_keys.size()
                   << ", remove_segment_ids=" << req.remove_segment_ids.size();
        BatchSyncReplicaResponse err_resp;
        err_resp.add_results.assign(req.add_keys.size(),
                                    ErrorCode::INVALID_PARAMS);
        err_resp.remove_results.assign(req.remove_keys.size(),
                                       ErrorCode::INVALID_PARAMS);
        return err_resp;
    }

    BatchSyncReplicaResponse response;
    response.add_results.resize(req.add_keys.size(), ErrorCode::OK);
    response.remove_results.resize(req.remove_keys.size(), ErrorCode::OK);

    // Resolve client once for all operations
    auto client = std::static_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(req.client_id));
    if (!client) {
        LOG(ERROR) << "BatchSyncReplica: client not found"
                   << ", client_id=" << req.client_id;
        std::fill(response.add_results.begin(), response.add_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        std::fill(response.remove_results.begin(),
                  response.remove_results.end(), ErrorCode::CLIENT_NOT_FOUND);
        return response;
    }

    // Group operations by shard index.
    std::unordered_map<size_t, std::vector<std::pair<size_t, bool>>>
        shard_groups;

    for (size_t i = 0; i < req.add_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.add_keys[i]);
        shard_groups[shard_idx].emplace_back(i, true);
    }
    for (size_t i = 0; i < req.remove_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.remove_keys[i]);
        shard_groups[shard_idx].emplace_back(i, false);
    }

    // Process each shard group with one lock acquisition
    for (auto& [shard_idx, ops] : shard_groups) {
        auto& shard = GetShard(shard_idx);
        MutexLocker lock(&shard.mutex);

        for (auto& [idx, is_add] : ops) {
            if (is_add) {
                auto result = InnerAddReplica(
                    shard, req.add_keys[idx], req.client_id,
                    req.add_segment_ids[idx], req.add_sizes[idx], client);
                if (!result.has_value()) {
                    response.add_results[idx] = result.error();
                }
            } else {
                auto result = InnerRemoveReplica(shard, req.remove_keys[idx],
                                                 req.client_id,
                                                 req.remove_segment_ids[idx]);
                if (!result.has_value() &&
                    result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                    result.error() != ErrorCode::REPLICA_NOT_FOUND) {
                    response.remove_results[idx] = result.error();
                }
            }
        }
    }

    return response;
}

auto P2PMasterService::SetSyncCompleted(UUID client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(WARNING) << "SetSyncCompleted: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(client);
    if (!p2p_client) {
        LOG(ERROR) << "SetSyncCompleted: client is not P2PClientMeta"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    p2p_client->SetSyncing(false);
    LOG(INFO) << "SetSyncCompleted: client_id=" << client_id;
    return {};
}

// ===================== Hooks =====================

void P2PMasterService::OnObjectAccessed(ObjectMetadata& metadata) {
    // do nothing
    (void)metadata;
}

// TODO: wanyue-wy
// For P2P structure, if a object has multiple replicas,
// we don't know which replica is hit.
void P2PMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    (void)metadata;
    MasterMetricManager::instance().inc_valid_get_nums();
}

void P2PMasterService::OnReplicaRemoved(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().dec_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().dec_file_cache_nums();
        }
    }
}

void P2PMasterService::OnReplicaAdded(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().inc_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().inc_file_cache_nums();
        }
    }
}

}  // namespace mooncake
