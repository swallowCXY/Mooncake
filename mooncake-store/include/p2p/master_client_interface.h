#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Minimal shared interface that the ClientService base class uses to
 * talk to the master client.
 *
 * Only the methods directly invoked by ClientService base (non-subclass)
 * logic appear here. Architecture-specific master clients implement this:
 *   - CentralizedMasterClient wraps main's concrete MasterClient (Ping-based).
 *   - P2PMasterClient (flattened, C6) implements it directly (Heartbeat-based).
 *
 * Heartbeat/Ping/RegisterClient are NOT here — they are invoked only by the
 * respective subclass (P2PClientService / CentralizedClientService), so each
 * master client exposes its own protocol-specific methods directly.
 */
class MasterClientInterface {
   public:
    virtual ~MasterClientInterface() = default;

    virtual ErrorCode Connect(const std::string& master_addr) = 0;

    virtual tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids) = 0;

    virtual tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str) = 0;

    virtual tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() = 0;
};

}  // namespace mooncake
