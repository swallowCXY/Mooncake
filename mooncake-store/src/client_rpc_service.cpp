#include "client_rpc_service.h"
#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

namespace {

// Lightweight data fingerprint: XOR of head(32B)/mid(32B)/tail(32B) + first 8B hex
static std::string DataFingerprint(const void* data, size_t size) {
    if (!data || size == 0) return "(nil)";
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint64_t fp = 0;
    auto xor_into = [&](const uint8_t* start, size_t len) {
        for (size_t i = 0; i < len; ++i)
            fp ^= (uint64_t)start[i] << ((i % 8) * 8);
    };
    size_t head = std::min(size, (size_t)32);
    xor_into(p, head);
    if (size > 64) xor_into(p + size / 2 - 16, 32);
    if (size > 32) xor_into(p + size - 32, std::min((size_t)32, size - 32));
    char hex[64];
    int pos = 0;
    for (int i = 0; i < std::min((int)size, 8); ++i)
        pos += snprintf(hex + pos, sizeof(hex) - pos, "%02x", p[i]);
    char result[128];
    snprintf(result, sizeof(result), "fp=0x%016lx head=[%s] size=%zu",
             (unsigned long)fp, hex, size);
    return std::string(result);
}

}  // namespace

ClientRpcService::ClientRpcService(DataManager& data_manager)
    : data_manager_(data_manager) {}

tl::expected<void, ErrorCode> ClientRpcService::ReadRemoteData(
    const RemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::ReadRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.dest_buffers.size());

    if (request.key.empty()) {
        LOG(ERROR) << "ReadRemoteData: empty key";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (request.dest_buffers.empty()) {
        LOG(ERROR) << "ReadRemoteData: empty destination buffers";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers (segment name validation is done in DataManager)
    for (const auto& buffer_desc : request.dest_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR)
                << "ReadRemoteData: invalid buffer (zero size or null address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Delegate to DataManager
    size_t total_dest_size = 0;
    for (const auto& buf : request.dest_buffers) total_dest_size += buf.size;
    LOG(INFO) << "[P2P_DIAG] READ_RECV_RPC key=" << request.key
              << " buf_count=" << request.dest_buffers.size()
              << " total_size=" << total_dest_size
              << " buf_sizes=["
              << [&]() {
                   std::string s;
                   for (size_t i = 0; i < request.dest_buffers.size(); ++i) {
                       if (i > 0) s += ",";
                       s += std::to_string(request.dest_buffers[i].size);
                   }
                   return s;
               }()
              << "]";

    auto result =
        data_manager_.ReadRemoteData(request.key, request.dest_buffers);

    if (!result.has_value()) {
        LOG(ERROR) << "ReadRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());

        // Rectify stale route when key not found
        if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
            data_manager_.RectifyReadRoute(request.key);
        }
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<UUID, ErrorCode> ClientRpcService::WriteRemoteData(
    const RemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.src_buffers.size());

    if (request.key.empty()) {
        LOG(ERROR) << "WriteRemoteData: empty key";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (request.src_buffers.empty()) {
        LOG(ERROR) << "WriteRemoteData: empty source buffers";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers (segment name validation is done in DataManager)
    for (const auto& buffer_desc : request.src_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR) << "WriteRemoteData: invalid buffer (zero size or null "
                          "address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Delegate to DataManager
    size_t total_src_size = 0;
    for (const auto& buf : request.src_buffers) total_src_size += buf.size;
    LOG(INFO) << "[P2P_DIAG] WRITE_RECV_RPC key=" << request.key
              << " buf_count=" << request.src_buffers.size()
              << " total_size=" << total_src_size
              << " buf_sizes=["
              << [&]() {
                   std::string s;
                   for (size_t i = 0; i < request.src_buffers.size(); ++i) {
                       if (i > 0) s += ",";
                       s += std::to_string(request.src_buffers[i].size);
                   }
                   return s;
               }()
              << "]";

    auto result = data_manager_.WriteRemoteData(
        request.key, request.src_buffers, request.target_tier_id);

    if (!result.has_value()) {
        LOG(ERROR) << "WriteRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return result;
}

void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service) {
    server.register_handler<&ClientRpcService::ReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::WriteRemoteData>(&service);
}

}  // namespace mooncake
