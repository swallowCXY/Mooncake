#pragma once

#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "types.h"

namespace mooncake {

/**
 * @brief Iteration strategy for walking over clients / keys.
 */
enum class ObjectIterateStrategy {
    ORDERED = 0,          // Iterate in a deterministic, ordered fashion
    RANDOM,               // Iterate in a random order
    CAPACITY_PRIORITY,    // Iterate in order of available capacity
};

inline std::ostream& operator<<(std::ostream& os,
                                const ObjectIterateStrategy& strategy) noexcept {
    switch (strategy) {
        case ObjectIterateStrategy::ORDERED:
            os << "ORDERED";
            break;
        case ObjectIterateStrategy::RANDOM:
            os << "RANDOM";
            break;
        case ObjectIterateStrategy::CAPACITY_PRIORITY:
            os << "CAPACITY_PRIORITY";
            break;
        default:
            os << "UNKNOWN";
            break;
    }
    return os;
}

/**
 * @brief Client status from the P2P master's perspective.
 *
 * State machine: HEALTH -> DISCONNECTION (heartbeat timeout)
 *                DISCONNECTION -> HEALTH (heartbeat recovered)
 *                DISCONNECTION -> CRASHED (long-term timeout)
 *
 * NOTE: P2P deployment mode uses this enum; Centralization mode uses the
 * `ClientStatus` enum defined in shared types.h. The two are intentionally
 * separate to keep the central master untouched.
 */
enum class P2PClientStatus {
    UNDEFINED = 0,    // Client does not exist
    HEALTH,           // Normal operation
    DISCONNECTION,    // Heartbeat lost, waiting for recovery
    CRASHED,          // Terminal state, all metadata will be cleaned up
};

inline std::ostream& operator<<(std::ostream& os,
                                const P2PClientStatus& status) noexcept {
    static const std::unordered_map<P2PClientStatus, std::string_view>
        status_strings{{P2PClientStatus::UNDEFINED, "UNDEFINED"},
                       {P2PClientStatus::HEALTH, "HEALTH"},
                       {P2PClientStatus::DISCONNECTION, "DISCONNECTION"},
                       {P2PClientStatus::CRASHED, "CRASHED"}};
    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

/**
 * @brief Describes the location of a replica during key iteration.
 *        Used by TieredBackend::ForEachKeyBatch() / DataManager::ForEachKeyBatch().
 */
struct ReplicaLocation {
    std::string key;
    UUID tier_id;
    size_t size = 0;
};

}  // namespace mooncake
