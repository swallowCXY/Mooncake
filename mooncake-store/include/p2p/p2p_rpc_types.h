#pragma once

#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>

#include "heartbeat_type.h"
#include "p2p_types.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

// =====================================================================
// Read route (GetReplicaList) config — P2P only
// =====================================================================

/**
 * @brief P2P specific configuration for read route
 */
struct P2PGetReplicaListConfigExtra {
    // exclude replicas whose segment contains any tag in tag_filters
    std::vector<std::string> tag_filters;
    // filter replicas whose segment priority is lower than priority_limit
    int priority_limit = 0;
};
YLT_REFL(P2PGetReplicaListConfigExtra, tag_filters, priority_limit);

/**
 * @brief Request config for getting replica list
 */
struct GetReplicaListRequestConfig {
    GetReplicaListRequestConfig() = default;
    GetReplicaListRequestConfig(size_t max_c) : max_candidates(max_c) {}

    // 0 means return all viable replica candidates;
    // otherwise, return at most max_candidates candidates
    static const size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PGetReplicaListConfigExtra> p2p_config;
};
YLT_REFL(GetReplicaListRequestConfig, max_candidates, p2p_config);

// config for filter replicas in read route
typedef GetReplicaListRequestConfig ReadRouteConfig;
typedef P2PGetReplicaListConfigExtra P2PReadRouteConfigExtra;

// =====================================================================
// Write route types — P2P only
// =====================================================================

/**
 * @brief Request config for write route
 */
struct WriteRouteRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    ObjectIterateStrategy strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;
    bool allow_local = true;   // whether to filter local client
    bool prefer_local = true;  // enhance the priority of local client
                               // works only when allow_local==true
    bool early_return = true;  // whether to return immediately once candidates
                               // meet conditions of config

    // segment level (TODO)
    // filter the segment with tag
    std::vector<std::string> tag_filters;
    // filter the segments whose priority is lower than priority_limit
    int priority_limit = 0;
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, allow_local,
         prefer_local, early_return, tag_filters, priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const WriteRouteRequestConfig& config) {
    os << "WriteRouteRequestConfig: { max_candidates: " << config.max_candidates
       << ", strategy: " << config.strategy
       << ", allow_local: " << (config.allow_local ? "true" : "false")
       << ", prefer_local: " << (config.prefer_local ? "true" : "false")
       << ", early_return: " << (config.early_return ? "true" : "false")
       << ", priority_limit: " << config.priority_limit << " }";
    return os;
}

/**
 * @brief Request structure for getting write route.
 */
struct WriteRouteRequest {
    std::string key;  // used for pre-filter with limitation of replica number
    UUID client_id;
    size_t size = 0;
    WriteRouteRequestConfig config;
};
YLT_REFL(WriteRouteRequest, key, client_id, size, config);

/**
 * @brief Candidate node for writing route
 */
struct WriteCandidate {
    P2PProxyDescriptor replica;
    size_t available_capacity = 0;
    int priority = 0;
};
YLT_REFL(WriteCandidate, replica, available_capacity, priority);

/**
 * @brief Response structure for getting write route.
 */
struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
};
YLT_REFL(WriteRouteResponse, candidates);

/**
 * @brief Request for batch write route lookup.
 */
struct BatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string> keys;
    std::vector<size_t> sizes;
    WriteRouteRequestConfig config;  // shared config for all keys
};
YLT_REFL(BatchGetWriteRouteRequest, client_id, keys, sizes, config);

/**
 * @brief Response for batch write route lookup.
 *        responses[i] and error_codes[i] correspond to keys[i] in the request.
 */
struct BatchGetWriteRouteResponse {
    std::vector<WriteRouteResponse> responses;  // valid when error_codes[i]==OK
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(BatchGetWriteRouteResponse, responses, error_codes);

// =====================================================================
// Replica sync types — P2P only
// =====================================================================

/**
 * @brief Request to add a replica.
 *        Master resolves ip_address/rpc_port from registered client info.
 */
struct AddReplicaRequest {
    std::string key;
    size_t size;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(AddReplicaRequest, key, size, client_id, segment_id);

/**
 * @brief Request to remove a replica
 */
struct RemoveReplicaRequest {
    std::string key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(RemoveReplicaRequest, key, client_id, segment_id);

/**
 * @brief Request to remove replicas from multiple segments in one call
 */
struct BatchRemoveReplicaRequest {
    std::string key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(BatchRemoveReplicaRequest, key, client_id, segment_ids);

/**
 * @brief Request to batch sync replicas (mixed ADD and REMOVE ops).
 *        Master only needs client_id + segment_id to identify replicas
 */
struct BatchSyncReplicaRequest {
    UUID client_id;
    // ADD operations
    std::vector<std::string> add_keys;
    std::vector<size_t> add_sizes;
    std::vector<UUID> add_segment_ids;
    // REMOVE operations
    std::vector<std::string> remove_keys;
    std::vector<UUID> remove_segment_ids;
};
YLT_REFL(BatchSyncReplicaRequest, client_id, add_keys, add_sizes,
         add_segment_ids, remove_keys, remove_segment_ids);

/**
 * @brief Response for batch sync replicas.
 */
struct BatchSyncReplicaResponse {
    std::vector<ErrorCode> add_results;
    std::vector<ErrorCode> remove_results;
};
YLT_REFL(BatchSyncReplicaResponse, add_results, remove_results);

// =====================================================================
// Client lifecycle RPC types — P2P only
// (Centralization mode uses Ping/PutStart etc. from shared rpc_types.h)
// =====================================================================

/**
 * @brief Request structure for Heartbeat operation.
 *        Client could set HeartbeatTasks for Master to run.
 */
struct HeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
};
YLT_REFL(HeartbeatRequest, client_id, tasks);

/**
 * @brief Response structure for Heartbeat operation.
 */
struct HeartbeatResponse {
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(HeartbeatResponse, status, view_version, task_results);

/**
 * @brief Request structure for RegisterClient operation.
 *        Client calls this on startup to register its UUID and local segments.
 *        P2P clients additionally provide ip_address and rpc_port.
 */
struct RegisterClientRequest {
    UUID client_id;
    std::vector<Segment> segments;
    DeploymentMode deployment_mode = DeploymentMode::CENTRALIZATION;

    // P2P only: network endpoint info
    std::optional<std::string> ip_address;
    std::optional<uint16_t> rpc_port;
};
YLT_REFL(RegisterClientRequest, client_id, segments, deployment_mode,
         ip_address, rpc_port);

/**
 * @brief Response structure for RegisterClient operation.
 */
struct RegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(RegisterClientResponse, view_version);

/**
 * @brief Request structure for QueryClientStatus operation.
 */
struct QueryClientStatusRequest {
    UUID client_id;
};
YLT_REFL(QueryClientStatusRequest, client_id);

/**
 * @brief Response structure for QueryClientStatus operation.
 */
struct QueryClientStatusResponse {
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
};
YLT_REFL(QueryClientStatusResponse, status);

}  // namespace mooncake
