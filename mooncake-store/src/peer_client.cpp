#include "peer_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>

#include "client_rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

// Forward declaration for RPC method traits
class ClientRpcService;

// RPC method name traits for logging/metrics
template <auto Method>
struct RpcNameTraits;

template <>
struct RpcNameTraits<&ClientRpcService::ReadRemoteData> {
    static constexpr const char* value = "ReadRemoteData";
};

template <>
struct RpcNameTraits<&ClientRpcService::WriteRemoteData> {
    static constexpr const char* value = "WriteRemoteData";
};

template <>
struct RpcNameTraits<&ClientRpcService::BatchReadRemoteData> {
    static constexpr const char* value = "BatchReadRemoteData";
};

template <>
struct RpcNameTraits<&ClientRpcService::BatchWriteRemoteData> {
    static constexpr const char* value = "BatchWriteRemoteData";
};

PeerClient::PeerClient() {
    // Initialize client pools with proper configuration
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    const char* protocol_env = std::getenv("MC_RPC_PROTOCOL");
    if (protocol_env && std::string_view(protocol_env) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }
    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
}

PeerClient::~PeerClient() = default;

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> PeerClient::invoke_rpc(Args&&... args) {
    auto pool = client_accessor_.GetClientPool();
    if (!pool) {
        LOG(ERROR) << "Client pool not available, call Connect first";
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto start_time = std::chrono::steady_clock::now();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available for RPC: "
                           << RpcNameTraits<ServiceMethod>::value;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC call failed: "
                           << RpcNameTraits<ServiceMethod>::value
                           << ", error: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time);
            VLOG(1) << "RPC " << RpcNameTraits<ServiceMethod>::value
                    << " completed in " << latency.count() << " us";
            co_return result->result();
        }());
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>> PeerClient::invoke_batch_rpc(
    size_t input_size, Args&&... args) {
    auto pool = client_accessor_.GetClientPool();
    if (!pool) {
        LOG(ERROR) << "Client pool not available, call Connect first";
        return std::vector<tl::expected<ResultType, ErrorCode>>(
            input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
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
                LOG(ERROR) << "Client not available for RPC: "
                           << RpcNameTraits<ServiceMethod>::value;
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "Batch RPC call failed: "
                           << RpcNameTraits<ServiceMethod>::value
                           << ", error: " << result.error().msg;
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time);
            VLOG(1) << "Batch RPC " << RpcNameTraits<ServiceMethod>::value
                    << " completed in " << latency.count() << " us for "
                    << input_size << " items";
            co_return result->result();
        }());
}

tl::expected<void, ErrorCode> PeerClient::Connect(const std::string& endpoint) {
    ScopedVLogTimer timer(1, "PeerClient::Connect");
    timer.LogRequest("endpoint=", endpoint);

    std::lock_guard<std::shared_mutex> lock(connect_mutex_);

    if (endpoint_ == endpoint && client_accessor_.GetClientPool()) {
        // Already connected to this endpoint
        timer.LogResponse("error_code=", ErrorCode::OK);
        return {};
    }

    // Get client pool for this endpoint from client_pools manager
    auto client_pool = client_pools_->at(endpoint);

    // Set the client pool
    client_accessor_.SetClientPool(client_pool);
    endpoint_ = endpoint;

    LOG(INFO) << "PeerClient connected to endpoint: " << endpoint;
    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<void, ErrorCode> PeerClient::ReadRemoteData(
    const RemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "PeerClient::ReadRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.dest_buffers.size());

    auto result = invoke_rpc<&ClientRpcService::ReadRemoteData, void>(request);
    timer.LogResponse("error_code=",
                      result.has_value() ? ErrorCode::OK : result.error());
    return result;
}

tl::expected<void, ErrorCode> PeerClient::WriteRemoteData(
    const RemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "PeerClient::WriteRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.src_buffers.size());

    auto result = invoke_rpc<&ClientRpcService::WriteRemoteData, void>(request);
    timer.LogResponse("error_code=",
                      result.has_value() ? ErrorCode::OK : result.error());
    return result;
}

std::vector<tl::expected<void, ErrorCode>> PeerClient::BatchReadRemoteData(
    const BatchRemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "PeerClient::BatchReadRemoteData");
    timer.LogRequest("key_count=", request.keys.size());

    auto results =
        invoke_batch_rpc<&ClientRpcService::BatchReadRemoteData, void>(
            request.keys.size(), request);

    timer.LogResponse("processed_count=", request.keys.size());
    return results;
}

std::vector<tl::expected<void, ErrorCode>> PeerClient::BatchWriteRemoteData(
    const BatchRemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "PeerClient::BatchWriteRemoteData");
    timer.LogRequest("key_count=", request.keys.size());

    auto results =
        invoke_batch_rpc<&ClientRpcService::BatchWriteRemoteData, void>(
            request.keys.size(), request);

    timer.LogResponse("processed_count=", request.keys.size());
    return results;
}

}  // namespace mooncake
