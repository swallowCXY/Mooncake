#pragma once

#include "client_rpc_types.h"
#include "types.h"
#include <vector>
#include <shared_mutex>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/util/tl/expected.hpp>
#include <string>

namespace mooncake {

class ClientRpcService;

/**
 * @class PeerClient
 * @brief Client for RPC communication with peer clients
 *
 * This client is used to send RPC requests to remote peer clients,
 * calling their ClientRpcService methods for remote data access.
 */
class PeerClient {
   public:
    PeerClient();
    ~PeerClient();

    PeerClient(const PeerClient&) = delete;
    PeerClient& operator=(const PeerClient&) = delete;

    /**
     * @brief Connects to a peer client endpoint
     * @param endpoint Peer client address (IP:Port)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> Connect(const std::string& endpoint);

    /**
     * @brief Read remote data from peer client
     * @param request RemoteReadRequest containing key and destination buffers
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> ReadRemoteData(
        const RemoteReadRequest& request);

    /**
     * @brief Write remote data to peer client
     * @param request RemoteWriteRequest containing key, source buffers, and
     * tier_id
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> WriteRemoteData(
        const RemoteWriteRequest& request);

    /**
     * @brief Batch read remote data for multiple keys
     * @param request BatchRemoteReadRequest containing multiple keys and
     * buffers
     * @return Vector of expected results for each key
     */
    std::vector<tl::expected<void, ErrorCode>> BatchReadRemoteData(
        const BatchRemoteReadRequest& request);

    /**
     * @brief Batch write remote data for multiple keys
     * @param request BatchRemoteWriteRequest containing multiple keys, buffers,
     * and tier_ids
     * @return Vector of expected results for each key
     */
    std::vector<tl::expected<void, ErrorCode>> BatchWriteRemoteData(
        const BatchRemoteWriteRequest& request);

   private:
    /**
     * @brief Thread-safe accessor for client pool
     */
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

    /**
     * @brief Template method to invoke RPC calls
     * @tparam ServiceMethod RPC method pointer
     * @tparam ReturnType Return type of the RPC method
     * @tparam Args Argument types
     * @param args RPC arguments
     * @return Expected result or error code
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    tl::expected<ReturnType, ErrorCode> invoke_rpc(Args&&... args);

    /**
     * @brief Generic RPC invocation helper for batch operations
     * @tparam ServiceMethod Pointer to ClientRpcService member function
     * @tparam ResultType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param input_size Size of input batch for error handling
     * @param args Arguments to pass to the RPC call
     * @return Vector of results from the batch RPC call
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args);

    RpcClientAccessor client_accessor_;
    std::string endpoint_;
    mutable std::shared_mutex connect_mutex_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
};

}  // namespace mooncake
