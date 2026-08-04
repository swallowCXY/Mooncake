#include <gflags/gflags.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <string>
#include "client_config_builder.h"
#include "client_service.h"
#include "real_client.h"
#include "utils.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_bool(enable_offload, false, "Enable offload availability");
// ---- P2P deployment mode flags (additive) ----
DEFINE_string(deployment_mode, "Centralization",
              "Client type: 'Centralization' or 'P2P'");
DEFINE_string(tiered_backend_config, "",
              "Tiered backend config json. Empty means load from env "
              "MOONCAKE_TIERED_CONFIG");
DEFINE_uint32(client_rpc_port, 12345, "Client RPC service port (P2P mode)");
DEFINE_uint32(rpc_thread_num, 16, "Number of threads for P2P RPC service");
DEFINE_uint64(lock_shard_count, 1024,
              "Number of key lock shards for DataManager (P2P)");
DEFINE_string(route_cache_max_memory, "300 MB", "Max memory for RouteCache");
DEFINE_uint64(route_cache_ttl_ms, 5 * 60 * 1000, "TTL for RouteCache (ms)");
DEFINE_string(p2p_local_transfer_mode, "te",
              "Local transfer mode for P2P: memcpy|te");
DEFINE_uint64(local_memcpy_async_worker_num, 32,
              "Async memcpy worker num when p2p_local_transfer_mode=memcpy");
DEFINE_uint32(metrics_port, 9003, "Port for HTTP metrics server");
DEFINE_bool(enable_metrics_http, true, "Enable HTTP metrics endpoint");
DEFINE_uint64(async_sender_thread_count, 0,
              "Async route notifier sender thread count (0=disabled)");
DEFINE_uint64(async_max_batch_size, 2000, "Max ops per async batch");
DEFINE_uint64(async_route_queue_size, 0, "Async route notifier queue size");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::get_buffer_info_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);

    auto client_inst = RealClient::create();

    if (FLAGS_deployment_mode == "P2P") {
        // P2P deployment mode: local_buffer_size is 0 (P2P manages buffers).
        auto res = client_inst->setup_p2p_internal(
            FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
            FLAGS_device_names.empty()
                ? std::nullopt
                : std::optional<std::string>(FLAGS_device_names),
            FLAGS_master_server_address, FLAGS_tiered_backend_config,
            static_cast<uint16_t>(FLAGS_client_rpc_port),
            static_cast<uint32_t>(FLAGS_rpc_thread_num),
            static_cast<size_t>(FLAGS_lock_shard_count),
            string_to_byte_size(FLAGS_route_cache_max_memory),
            FLAGS_route_cache_ttl_ms, FLAGS_p2p_local_transfer_mode,
            static_cast<size_t>(FLAGS_local_memcpy_async_worker_num),
            static_cast<uint16_t>(FLAGS_metrics_port),
            FLAGS_enable_metrics_http,
            static_cast<size_t>(FLAGS_async_sender_thread_count),
            static_cast<size_t>(FLAGS_async_max_batch_size),
            static_cast<size_t>(FLAGS_async_route_queue_size),
            "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock");
        if (!res) {
            LOG(FATAL) << "Failed to setup P2P client: "
                       << toString(res.error());
            return -1;
        }
    } else {
        if (FLAGS_deployment_mode != "Centralization") {
            LOG(WARNING) << "Unknown deployment_mode '"
                         << FLAGS_deployment_mode
                         << "', defaulting to Centralization";
        }
        auto res = client_inst->setup_internal(
            FLAGS_host, FLAGS_metadata_server, global_segment_size, 0,
            FLAGS_protocol, FLAGS_device_names, FLAGS_master_server_address,
            nullptr, "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
            FLAGS_enable_offload);
        if (!res) {
            LOG(FATAL) << "Failed to setup client: " << toString(res.error());
            return -1;
        }
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, "127.0.0.1");
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting real client service on 127.0.0.1:" << FLAGS_port
              << " (mode=" << FLAGS_deployment_mode << ")";

    return server.start();
}
