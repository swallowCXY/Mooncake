#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "types.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {

struct RemoteBufferDesc {
    std::string segment_endpoint;
    uintptr_t addr;
    uint64_t size;
};

YLT_REFL(RemoteBufferDesc, segment_endpoint, addr, size);

struct RemoteReadRequest {
    std::string key;
    std::vector<RemoteBufferDesc> dest_buffers;
    int32_t device_id = kInvalidPhysicalDeviceId;
};

YLT_REFL(RemoteReadRequest, key, dest_buffers, device_id);

struct RemoteWriteRequest {
    std::string key;
    std::vector<RemoteBufferDesc> src_buffers;
    std::optional<UUID> target_tier_id;
    int32_t device_id = kInvalidPhysicalDeviceId;
};

YLT_REFL(RemoteWriteRequest, key, src_buffers, target_tier_id, device_id);

struct BatchRemoteReadRequest {
    std::vector<std::string> keys;
    std::vector<std::vector<RemoteBufferDesc>> dest_buffers_list;
    int32_t device_id = kInvalidPhysicalDeviceId;
};

YLT_REFL(BatchRemoteReadRequest, keys, dest_buffers_list, device_id);

struct BatchRemoteWriteRequest {
    std::vector<std::string> keys;
    std::vector<std::vector<RemoteBufferDesc>> src_buffers_list;
    std::vector<std::optional<UUID>> target_tier_ids;
    int32_t device_id = kInvalidPhysicalDeviceId;
};

YLT_REFL(BatchRemoteWriteRequest, keys, src_buffers_list, target_tier_ids,
         device_id);

}  // namespace mooncake