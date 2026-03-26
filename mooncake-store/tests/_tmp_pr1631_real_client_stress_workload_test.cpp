#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "client_config_builder.h"
#include "p2p_rpc_types.h"
#include "real_client.h"
#include "rpc_types.h"
#include "types.h"

using namespace mooncake;

DEFINE_string(protocol, "tcp", "Transfer protocol: tcp|rdma");
DEFINE_string(master_address, "127.0.0.1:50051", "Master server address");
DEFINE_string(local_hostname, "127.0.0.1", "Local hostname or IP");
DEFINE_string(metadata_connection_string, "P2PHANDSHAKE",
              "Metadata connection string");
DEFINE_string(device_names, "", "RDMA device names");
DEFINE_string(
    tiered_backend_config,
    R"({"tiers":[{"type":"DRAM","capacity":1073741824,"priority":10}]})",
    "Tiered backend config json");

DEFINE_uint32(client_rpc_port, 12345, "Client RPC port in P2P mode");
DEFINE_uint32(rpc_thread_num, 16, "RPC thread number in P2P mode");

DEFINE_int32(node_id, 1, "Current node ID (1-based)");
DEFINE_int32(num_nodes, 1, "Total number of nodes in the test cluster");
DEFINE_int32(key_count, 1000, "Number of keys to preload on this node");
DEFINE_int32(num_threads, 8, "Number of worker threads");
DEFINE_int32(test_operation_nums, 1000, "Operations per thread");
DEFINE_int32(value_size, 1048576, "Value size in bytes");
DEFINE_int32(warmup_ops, 100, "Warmup ops per thread");
DEFINE_double(remote_read_ratio, 0.5,
              "Ratio of remote reads in stress-read mode");
DEFINE_int32(random_seed, 12345, "Random seed");
DEFINE_int64(start_timestamp_ms, 0,
             "Global start timestamp (ms since epoch) for read phase in "
             "preload-then-read mode. If 0, start immediately after preload.");
DEFINE_string(workload_mode, "preload_then_read",
              "Workload mode: preload_then_read | op_sequence | rw_correctness");
DEFINE_double(write_ratio, 0.3, "Write ratio in rw_correctness mode");
DEFINE_int32(op_sequence_max_rounds, 8,
             "Max rounds in op_sequence mode for each operation");
DEFINE_int32(rw_verify_retries, 50,
             "Max retries for read-back verification in rw_correctness mode");
DEFINE_int32(rw_verify_sleep_ms, 2,
             "Sleep milliseconds between read-back verification retries");
DEFINE_int32(rw_key_pool_size, 0,
             "Unique key pool size for rw_correctness. "
             "0 means use --key_count. Helps avoid P2P memory/segment "
             "exhaustion when repeatedly writing new keys.");

DEFINE_string(client_mode, "p2p",
              "Client mode: p2p | centralized");
DEFINE_int64(global_segment_size, 2147483648LL,
             "Global segment size in bytes for centralized mode "
             "(default 2GB, must be >= key_count * value_size).");

namespace {

std::shared_ptr<RealClient> g_client;
// Only log the first correctness mismatch to avoid log flooding.
std::atomic<bool> g_rw_correctness_mismatch_logged{false};

struct OperationResult {
    double latency_us = 0.0;
    bool success = false;
    bool expected_remote = false;
    bool query_success = false;
};

struct ThreadStats {
    std::vector<OperationResult> reads;
    uint64_t total_reads = 0;
    uint64_t successful_reads = 0;
    uint64_t query_failures = 0;
    uint64_t get_failures = 0;
    uint64_t local_reads = 0;
    uint64_t remote_reads = 0;
    uint64_t local_successful_reads = 0;
    uint64_t remote_successful_reads = 0;
    double successful_read_time_us = 0.0;
};

struct MixedThreadStats {
    std::vector<double> read_latencies_us;
    std::vector<double> write_latencies_us;
    uint64_t read_attempts = 0;
    uint64_t write_attempts = 0;
    uint64_t successful_reads = 0;
    uint64_t successful_writes = 0;
    uint64_t exist_calls = 0;
    uint64_t exist_failures = 0;
    uint64_t read_failures = 0;
    uint64_t write_failures = 0;
    uint64_t correctness_failures = 0;
    double successful_read_time_us = 0.0;
    double successful_write_time_us = 0.0;
};



std::string generate_key(int node_id, int idx) {
    return "node_" + std::to_string(node_id) + "_obj_" + std::to_string(idx);
}

bool initialize_real_client() {
    auto client = RealClient::create();

    int rc = 0;
    if (FLAGS_client_mode == "p2p") {
        auto config = ClientConfigBuilder::build_p2p_real_client(
            FLAGS_local_hostname,
            FLAGS_metadata_connection_string,
            FLAGS_protocol,
            FLAGS_device_names.empty()
                ? std::nullopt
                : std::optional<std::string>(FLAGS_device_names),
            FLAGS_master_address,
            FLAGS_tiered_backend_config,
            /*local_buffer_size=*/0,
            /*transfer_engine=*/nullptr,
            /*ipc_socket_path=*/"",
            static_cast<uint16_t>(FLAGS_client_rpc_port),
            static_cast<uint32_t>(FLAGS_rpc_thread_num));
        rc = client->setup(config);
    } else if (FLAGS_client_mode == "centralized") {
        auto config = ClientConfigBuilder::build_centralized_real_client(
            FLAGS_local_hostname,
            FLAGS_metadata_connection_string,
            FLAGS_protocol,
            FLAGS_device_names.empty()
                ? std::nullopt
                : std::optional<std::string>(FLAGS_device_names),
            FLAGS_master_address,
            static_cast<uint64_t>(FLAGS_global_segment_size),
            /*local_buffer_size=*/0,
            /*transfer_engine=*/nullptr,
            /*ipc_socket_path=*/"",
            /*enable_offload=*/false);
        rc = client->setup(config);
    } else {
        LOG(ERROR) << "Unknown client_mode: " << FLAGS_client_mode
                   << ", expected 'p2p' or 'centralized'";
        return false;
    }
    if (rc != 0) {
        LOG(ERROR) << "RealClient setup failed, rc=" << rc;
        return false;
    }

    g_client = std::move(client);
    return true;
}

void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p80, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p80 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    const size_t size = latencies.size();

    auto idx = [&](double ratio) -> size_t {
        return static_cast<size_t>(std::ceil((size * ratio) - 1));
    };

    p50 = latencies[idx(0.50)];
    p80 = latencies[idx(0.80)];
    p95 = latencies[idx(0.95)];
    p99 = latencies[idx(0.99)];
}

void print_read_results(const std::vector<ThreadStats>& thread_stats,
                        double duration_s) {
    uint64_t total_reads = 0;
    uint64_t successful_reads = 0;
    uint64_t query_failures = 0;
    uint64_t get_failures = 0;
    uint64_t local_reads = 0;
    uint64_t remote_reads = 0;
    uint64_t local_success = 0;
    uint64_t remote_success = 0;

    std::vector<double> all_latencies;
    std::vector<double> local_latencies;
    std::vector<double> remote_latencies;

    for (const auto& stats : thread_stats) {
        total_reads += stats.total_reads;
        successful_reads += stats.successful_reads;
        query_failures += stats.query_failures;
        get_failures += stats.get_failures;
        local_reads += stats.local_reads;
        remote_reads += stats.remote_reads;
        local_success += stats.local_successful_reads;
        remote_success += stats.remote_successful_reads;

        for (const auto& op : stats.reads) {
            if (!op.success) continue;
            all_latencies.push_back(op.latency_us);
            if (op.expected_remote) {
                remote_latencies.push_back(op.latency_us);
            } else {
                local_latencies.push_back(op.latency_us);
            }
        }
    }

    double all_p50, all_p80, all_p95, all_p99;
    double local_p50, local_p80, local_p95, local_p99;
    double remote_p50, remote_p80, remote_p95, remote_p99;
    double successful_read_time_us = 0.0;

    calculate_percentiles(all_latencies, all_p50, all_p80, all_p95, all_p99);
    calculate_percentiles(local_latencies, local_p50, local_p80, local_p95,
                          local_p99);
    calculate_percentiles(remote_latencies, remote_p50, remote_p80, remote_p95,
                          remote_p99);
    for (const auto& stats : thread_stats) {
        successful_read_time_us += stats.successful_read_time_us;
    }

    const double reads_per_sec =
        successful_read_time_us > 0
            ? successful_reads * 1e6 / successful_read_time_us
            : 0.0;
    const double data_mb_per_sec =
        successful_read_time_us > 0
            ? (successful_reads * FLAGS_value_size) /
                  ((successful_read_time_us / 1e6) * 1024 * 1024)
            : 0.0;

    LOG(INFO) << "=== RealClient Multi-Node Stress Results ===";
    LOG(INFO) << "Node ID: " << FLAGS_node_id;
    LOG(INFO) << "Duration(s): " << duration_s;
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Value size(bytes): " << FLAGS_value_size;
    LOG(INFO) << "Total reads: " << total_reads;
    LOG(INFO) << "Successful reads: " << successful_reads;
    LOG(INFO) << "Query failures: " << query_failures;
    LOG(INFO) << "Get failures: " << get_failures;
    LOG(INFO) << "Success rate(%): "
              << (total_reads == 0 ? 0.0
                                   : 100.0 * successful_reads / total_reads);

    LOG(INFO) << "Local reads: " << local_reads
              << ", local successful reads: " << local_success;
    LOG(INFO) << "Remote reads: " << remote_reads
              << ", remote successful reads: " << remote_success;

    LOG(INFO) << "Read active time(s): " << (successful_read_time_us / 1e6);
    LOG(INFO) << "Reads/sec (by read time): " << reads_per_sec;
    LOG(INFO) << "Data throughput(MB/s, by read time): " << data_mb_per_sec;

    LOG(INFO) << "All latency(us) p50/p80/p95/p99 = "
              << all_p50 << "/" << all_p80 << "/" << all_p95 << "/" << all_p99;
    LOG(INFO) << "Local latency(us) p50/p80/p95/p99 = "
              << local_p50 << "/" << local_p80 << "/" << local_p95 << "/"
              << local_p99;
    LOG(INFO) << "Remote latency(us) p50/p80/p95/p99 = "
              << remote_p50 << "/" << remote_p80 << "/" << remote_p95 << "/"
              << remote_p99;
}

bool preload_keys() {
    std::vector<char> buffer(FLAGS_value_size, 'A');
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "register_buffer failed in preload";
        return false;
    }

    // Choose write configuration based on client_mode:
    // - P2P mode: use WriteRouteRequestConfig (write via routing service)
    // - Centralized mode: use ReplicateConfig (centralized replication)
    // put_from is a zero-copy style API: it reads directly from the user-registered
    // buffer; both modes support it.
    WriteConfig write_cfg = [&]() -> WriteConfig {
        if (FLAGS_client_mode == "p2p") {
            WriteRouteRequestConfig cfg;
            cfg.max_candidates = 1;
            cfg.allow_local = true;
            cfg.prefer_local = true;
            cfg.early_return = true;
            return cfg;
        } else {
            ReplicateConfig cfg;
            cfg.replica_num = 1;
            return cfg;
        }
    }();

    for (int i = 0; i < FLAGS_key_count; ++i) {
        auto key = generate_key(FLAGS_node_id, i);
        std::fill(buffer.begin(), buffer.end(),
                  static_cast<char>('A' + (i % 26)));

        int rc = g_client->put_from(key, buffer.data(), buffer.size(),
                                    write_cfg);
        if (rc != 0) {
            LOG(ERROR) << "put_from failed for key=" << key << ", rc=" << rc;
            g_client->unregister_buffer(buffer.data());
            return false;
        }
    }

    g_client->unregister_buffer(buffer.data());

    LOG(INFO) << "Preload complete, node=" << FLAGS_node_id
              << ", keys=" << FLAGS_key_count;
    return true;
}

void stress_read_worker(int thread_id, const std::vector<std::string>& local_keys,
                        const std::vector<int>& remote_node_ids,
                        std::atomic<bool>& stop_flag, ThreadStats& stats) {
    std::vector<char> buffer(FLAGS_value_size, 0);
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id << ": register_buffer failed";
        return;
    }

    std::mt19937 rng(FLAGS_random_seed + thread_id);
    std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

    auto pick_index = [&](size_t size) -> size_t {
        std::uniform_int_distribution<size_t> dist(0, size - 1);
        return dist(rng);
    };

    ReadRouteConfig read_cfg;

    const int total_ops = FLAGS_warmup_ops + FLAGS_test_operation_nums;
    const bool has_remote_nodes = !remote_node_ids.empty();
    const int total_remote_keys =
        has_remote_nodes ? static_cast<int>(remote_node_ids.size()) * FLAGS_key_count : 0;

    for (int i = 0; i < total_ops && !stop_flag.load(); ++i) {
        const bool choose_remote =
            has_remote_nodes &&
            (ratio_dist(rng) < FLAGS_remote_read_ratio);

        int target_node = FLAGS_node_id;
        int key_idx = 0;

        if (choose_remote) {
            // Perform a global random selection over the key space across all
            // remote nodes:
            // total_remote_keys = remote_node_ids.size() * key_count
            if (total_remote_keys <= 0) {
                // Should not happen, but keep a defensive guard.
                continue;
            }
            std::uniform_int_distribution<int> remote_key_dist(0, total_remote_keys - 1);
            int gidx = remote_key_dist(rng);  // Global remote-key index

            int remote_node_slot = gidx / FLAGS_key_count;   // Which remote node (slot)
            int remote_key_idx = gidx % FLAGS_key_count;     // Key index within that node

            target_node = remote_node_ids[static_cast<size_t>(remote_node_slot)];
            key_idx = remote_key_idx;
        } else {
            // Pure local read: pick a random key from this node's key space.
            key_idx = static_cast<int>(
                pick_index(static_cast<size_t>(FLAGS_key_count)));
        }
        const std::string key = generate_key(target_node, key_idx);

        auto start = std::chrono::high_resolution_clock::now();
        int64_t size = g_client->get_into(key, buffer.data(), buffer.size(),
                                          read_cfg);
        auto end = std::chrono::high_resolution_clock::now();

        if (i >= FLAGS_warmup_ops) {
            OperationResult result;
            result.latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                    .count();
            result.expected_remote = choose_remote;
            result.success = size >= 0;
            result.query_success = result.success;  // get_into already covers query+get
            stats.reads.push_back(result);

            stats.total_reads++;
            if (choose_remote) {
                stats.remote_reads++;
            } else {
                stats.local_reads++;
            }

            if (result.success) {
                stats.successful_reads++;
                stats.successful_read_time_us += result.latency_us;
                if (choose_remote) {
                    stats.remote_successful_reads++;
                } else {
                    stats.local_successful_reads++;
                }
            } else {
                stats.get_failures++;
            }
        }
    }

    g_client->unregister_buffer(buffer.data());
}

bool stress_read() {
    // Local keys: node_<node_id>_obj_0..key_count-1
    // Remote node list: derived from num_nodes and node_id.
    std::vector<int> remote_node_ids;
    remote_node_ids.reserve(std::max(0, FLAGS_num_nodes - 1));
    for (int nid = 1; nid <= FLAGS_num_nodes; ++nid) {
        if (nid == FLAGS_node_id) continue;
        remote_node_ids.push_back(nid);
    }

    LOG(INFO) << "Stress-read config: node_id=" << FLAGS_node_id
              << ", num_nodes=" << FLAGS_num_nodes
              << ", local_keys=" << FLAGS_key_count
              << ", remote_nodes=" << remote_node_ids.size();

    std::vector<std::thread> workers;
    std::vector<ThreadStats> thread_stats(FLAGS_num_threads);
    std::atomic<bool> stop_flag{false};

    auto start = std::chrono::high_resolution_clock::now();

    // local_keys is not used at the moment; keep it as a placeholder to avoid
    // larger signature changes.
    std::vector<std::string> dummy_local_keys;

    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(stress_read_worker, i, std::cref(dummy_local_keys),
                             std::cref(remote_node_ids), std::ref(stop_flag),
                             std::ref(thread_stats[i]));
    }

    for (auto& t : workers) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    const double duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count() /
        1000.0;

    print_read_results(thread_stats, duration_s);
    return true;
}

WriteConfig build_write_config(bool strict_visibility = false) {
    if (FLAGS_client_mode == "p2p") {
        WriteRouteRequestConfig cfg;
        cfg.max_candidates = 1;
        cfg.allow_local = true;
        cfg.prefer_local = true;
        // For correctness-sensitive workloads, wait for stronger visibility.
        cfg.early_return = !strict_visibility;
        return cfg;
    }
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    return cfg;
}

void print_mixed_results(const std::vector<MixedThreadStats>& thread_stats,
                         const std::string& title) {
    uint64_t read_attempts = 0, write_attempts = 0, successful_reads = 0,
             successful_writes = 0, exist_calls = 0, exist_failures = 0,
             read_failures = 0, write_failures = 0, correctness_failures = 0;
    double successful_read_time_us = 0.0, successful_write_time_us = 0.0;
    std::vector<double> read_latencies;
    std::vector<double> write_latencies;

    for (const auto& st : thread_stats) {
        read_attempts += st.read_attempts;
        write_attempts += st.write_attempts;
        successful_reads += st.successful_reads;
        successful_writes += st.successful_writes;
        exist_calls += st.exist_calls;
        exist_failures += st.exist_failures;
        read_failures += st.read_failures;
        write_failures += st.write_failures;
        correctness_failures += st.correctness_failures;
        successful_read_time_us += st.successful_read_time_us;
        successful_write_time_us += st.successful_write_time_us;
        read_latencies.insert(read_latencies.end(), st.read_latencies_us.begin(),
                              st.read_latencies_us.end());
        write_latencies.insert(write_latencies.end(), st.write_latencies_us.begin(),
                               st.write_latencies_us.end());
    }

    double rp50, rp80, rp95, rp99, wp50, wp80, wp95, wp99;
    calculate_percentiles(read_latencies, rp50, rp80, rp95, rp99);
    calculate_percentiles(write_latencies, wp50, wp80, wp95, wp99);

    const double reads_per_sec =
        successful_read_time_us > 0 ? successful_reads * 1e6 / successful_read_time_us : 0.0;
    const double writes_per_sec =
        successful_write_time_us > 0 ? successful_writes * 1e6 / successful_write_time_us : 0.0;
    const double read_mb_per_sec =
        successful_read_time_us > 0
            ? (successful_reads * FLAGS_value_size) /
                  ((successful_read_time_us / 1e6) * 1024 * 1024)
            : 0.0;
    const double write_mb_per_sec =
        successful_write_time_us > 0
            ? (successful_writes * FLAGS_value_size) /
                  ((successful_write_time_us / 1e6) * 1024 * 1024)
            : 0.0;

    LOG(INFO) << "=== " << title << " ===";
    LOG(INFO) << "read_attempts=" << read_attempts
              << ", successful_reads=" << successful_reads
              << ", read_failures=" << read_failures;
    LOG(INFO) << "write_attempts=" << write_attempts
              << ", successful_writes=" << successful_writes
              << ", write_failures=" << write_failures;
    LOG(INFO) << "exist_calls=" << exist_calls
              << ", exist_failures=" << exist_failures
              << ", correctness_failures=" << correctness_failures;
    LOG(INFO) << "Read throughput(ops/s, by read time)=" << reads_per_sec
              << ", Read throughput(MB/s, by read time)=" << read_mb_per_sec;
    LOG(INFO) << "Write throughput(ops/s, by write time)=" << writes_per_sec
              << ", Write throughput(MB/s, by write time)=" << write_mb_per_sec;
    LOG(INFO) << "Read latency(us) p50/p80/p95/p99 = "
              << rp50 << "/" << rp80 << "/" << rp95 << "/" << rp99;
    LOG(INFO) << "Write latency(us) p50/p80/p95/p99 = "
              << wp50 << "/" << wp80 << "/" << wp95 << "/" << wp99;
}

void op_sequence_worker(int thread_id, const std::vector<int>& remote_node_ids,
                        MixedThreadStats& stats) {
    std::vector<char> buffer(FLAGS_value_size, 0);
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id << ": register_buffer failed";
        stats.correctness_failures++;
        return;
    }
    WriteConfig write_cfg = build_write_config();
    ReadRouteConfig read_cfg;

    std::mt19937 rng(FLAGS_random_seed + 97 * thread_id);
    std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);
    const bool has_remote_nodes = !remote_node_ids.empty();
    const int total_remote_keys =
        has_remote_nodes ? static_cast<int>(remote_node_ids.size()) * FLAGS_key_count : 0;

    for (int i = 0; i < FLAGS_test_operation_nums; ++i) {
        const bool choose_remote =
            has_remote_nodes && (ratio_dist(rng) < FLAGS_remote_read_ratio);
        int target_node = FLAGS_node_id;
        int key_idx = 0;
        if (choose_remote && total_remote_keys > 0) {
            std::uniform_int_distribution<int> remote_key_dist(0, total_remote_keys - 1);
            int gidx = remote_key_dist(rng);
            int remote_node_slot = gidx / FLAGS_key_count;
            int remote_key_idx = gidx % FLAGS_key_count;
            target_node = remote_node_ids[static_cast<size_t>(remote_node_slot)];
            key_idx = remote_key_idx;
        } else {
            std::uniform_int_distribution<int> local_dist(0, FLAGS_key_count - 1);
            key_idx = local_dist(rng);
        }
        const std::string key = generate_key(target_node, key_idx);

        bool done = false;
        for (int round = 0; round < FLAGS_op_sequence_max_rounds && !done; ++round) {
            stats.exist_calls++;
            int exist_before = g_client->isExist(key);
            if (exist_before < 0) {
                stats.exist_failures++;
                continue;
            }

            if (exist_before == 1) {
                stats.read_attempts++;
                auto rs = std::chrono::high_resolution_clock::now();
                int64_t size = g_client->get_into(key, buffer.data(), buffer.size(), read_cfg);
                auto re = std::chrono::high_resolution_clock::now();
                const double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                              re - rs)
                                              .count();
                if (size >= 0) {
                    stats.successful_reads++;
                    stats.successful_read_time_us += latency_us;
                    stats.read_latencies_us.push_back(latency_us);
                    done = true;
                } else {
                    stats.read_failures++;
                }
            } else {
                std::fill(buffer.begin(), buffer.end(),
                          static_cast<char>('a' + ((thread_id + i + round) % 26)));
                stats.write_attempts++;
                auto ws = std::chrono::high_resolution_clock::now();
                int rc = g_client->put_from(key, buffer.data(), buffer.size(), write_cfg);
                auto we = std::chrono::high_resolution_clock::now();
                const double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                              we - ws)
                                              .count();
                if (rc == 0) {
                    stats.successful_writes++;
                    stats.successful_write_time_us += latency_us;
                    stats.write_latencies_us.push_back(latency_us);
                } else {
                    stats.write_failures++;
                }

                stats.exist_calls++;
                int exist_after = g_client->isExist(key);
                if (exist_after < 0) {
                    stats.exist_failures++;
                    continue;
                }
            }
        }
    }

    g_client->unregister_buffer(buffer.data());
}

bool operation_sequence_workload() {
    std::vector<int> remote_node_ids;
    remote_node_ids.reserve(std::max(0, FLAGS_num_nodes - 1));
    for (int nid = 1; nid <= FLAGS_num_nodes; ++nid) {
        if (nid == FLAGS_node_id) continue;
        remote_node_ids.push_back(nid);
    }

    std::vector<std::thread> workers;
    std::vector<MixedThreadStats> stats(FLAGS_num_threads);
    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(op_sequence_worker, i, std::cref(remote_node_ids),
                             std::ref(stats[i]));
    }
    for (auto& t : workers) t.join();

    print_mixed_results(stats, "Operation Sequence Workload");
    return true;
}

void rw_correctness_worker(int thread_id, MixedThreadStats& stats) {
    std::vector<char> read_buffer(FLAGS_value_size, 0);
    std::vector<char> write_buffer(FLAGS_value_size, 0);
    if (g_client->register_buffer(read_buffer.data(), read_buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id << ": register_buffer failed";
        stats.correctness_failures++;
        return;
    }
    if (g_client->register_buffer(write_buffer.data(), write_buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id
                   << ": register_buffer for write buffer failed";
        stats.correctness_failures++;
        g_client->unregister_buffer(read_buffer.data());
        return;
    }

    const std::string key_prefix =
        "rwcheck_node_" + std::to_string(FLAGS_node_id) + "_thread_" +
        std::to_string(thread_id);
    WriteConfig write_cfg = build_write_config(/*strict_visibility=*/true);
    ReadRouteConfig read_cfg;

    char expected = 0;
    bool has_written = false;
    int write_version = 0;
    std::string current_key;
    std::mt19937 rng(FLAGS_random_seed + 131 * thread_id);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    const int key_pool_size =
        std::max(1, static_cast<int>(FLAGS_rw_key_pool_size > 0
                                          ? FLAGS_rw_key_pool_size
                                          : FLAGS_key_count));

    for (int i = 0; i < FLAGS_test_operation_nums; ++i) {
        const bool do_write = !has_written || (dist(rng) < FLAGS_write_ratio);
        if (do_write) {
            // Candidate key/value are derived from a bounded key pool, so that
            // P2P correctness tests don't exhaust DramCacheTier space.
            const int key_idx = write_version++ % key_pool_size;
            const char candidate_expected =
                static_cast<char>((key_idx % 251) + 1);
            const std::string candidate_key =
                key_prefix + "_k_" + std::to_string(key_idx);

            std::fill(write_buffer.begin(), write_buffer.end(),
                      candidate_expected);
            stats.write_attempts++;
            auto ws = std::chrono::high_resolution_clock::now();
            int rc = g_client->put_from(
                candidate_key, write_buffer.data(), write_buffer.size(),
                write_cfg);
            auto we = std::chrono::high_resolution_clock::now();
            const double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                          we - ws)
                                          .count();
            if (rc == 0) {
                has_written = true;
                expected = candidate_expected;
                current_key = candidate_key;
                stats.successful_writes++;
                stats.successful_write_time_us += latency_us;
                stats.write_latencies_us.push_back(latency_us);
            } else {
                stats.write_failures++;
                continue;
            }
        }

        const int kReadVerifyRetries = std::max(1, FLAGS_rw_verify_retries);
        const int kReadVerifySleepMs = std::max(0, FLAGS_rw_verify_sleep_ms);
        if (!has_written || current_key.empty()) {
            continue;
        }
        bool verified = false;
        bool read_ok = false;
        for (int retry = 0; retry < kReadVerifyRetries && !verified; ++retry) {
            stats.read_attempts++;
            std::fill(read_buffer.begin(), read_buffer.end(), 0);
            auto rs = std::chrono::high_resolution_clock::now();
            int64_t size =
                g_client->get_into(current_key, read_buffer.data(),
                                   read_buffer.size(), read_cfg);
            auto re = std::chrono::high_resolution_clock::now();
            const double latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(re - rs).count();

            if (size < 0 || size != FLAGS_value_size) {
                stats.read_failures++;
                if (retry + 1 < kReadVerifyRetries) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(kReadVerifySleepMs));
                }
                continue;
            }

            read_ok = true;
            stats.successful_reads++;
            stats.successful_read_time_us += latency_us;
            stats.read_latencies_us.push_back(latency_us);

            bool mismatch = false;
            int mismatch_idx = -1;
            unsigned char actual_byte = 0;
            for (int j = 0; j < FLAGS_value_size; ++j) {
                if (read_buffer[static_cast<size_t>(j)] != expected) {
                    mismatch = true;
                    mismatch_idx = j;
                    actual_byte =
                        static_cast<unsigned char>(read_buffer[static_cast<size_t>(j)]);
                    break;
                }
            }
            if (!mismatch) {
                verified = true;
                break;
            } else {
                if (!g_rw_correctness_mismatch_logged.exchange(true)) {
                    LOG(ERROR)
                        << "rw_correctness mismatch (first occurrence): "
                        << "thread_id=" << thread_id
                        << ", key=" << current_key
                        << ", expected=" << static_cast<int>(static_cast<unsigned char>(expected))
                        << ", actual["
                        << mismatch_idx << "]=" << static_cast<int>(actual_byte);
                }
            }

            if (retry + 1 < kReadVerifyRetries) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(kReadVerifySleepMs));
            }
        }

        if (read_ok && !verified) {
            stats.correctness_failures++;
        }
    }
    g_client->unregister_buffer(write_buffer.data());
    g_client->unregister_buffer(read_buffer.data());
}

bool concurrent_rw_correctness_workload() {
    std::vector<std::thread> workers;
    std::vector<MixedThreadStats> stats(FLAGS_num_threads);
    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(rw_correctness_worker, i, std::ref(stats[i]));
    }
    for (auto& t : workers) t.join();

    print_mixed_results(stats, "Concurrent Read/Write Correctness");
    uint64_t total_correctness_failures = 0;
    uint64_t total_read_failures = 0;
    uint64_t total_write_failures = 0;
    for (const auto& st : stats) {
        total_correctness_failures += st.correctness_failures;
        total_read_failures += st.read_failures;
        total_write_failures += st.write_failures;
    }
    return total_correctness_failures == 0 && total_read_failures == 0 &&
           total_write_failures == 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    LOG(INFO) << "Starting RealClient stress workload test"
              << ", node_id=" << FLAGS_node_id
              << ", master=" << FLAGS_master_address
              << ", local_hostname=" << FLAGS_local_hostname;

    if (FLAGS_remote_read_ratio < 0.0 || FLAGS_remote_read_ratio > 1.0) {
        LOG(ERROR) << "remote_read_ratio must be in [0, 1]";
        return 1;
    }
    if (FLAGS_write_ratio < 0.0 || FLAGS_write_ratio > 1.0) {
        LOG(ERROR) << "write_ratio must be in [0, 1]";
        return 1;
    }

    if (FLAGS_num_nodes <= 0) {
        LOG(ERROR) << "num_nodes must be > 0";
        return 1;
    }
    if (FLAGS_node_id <= 0 || FLAGS_node_id > FLAGS_num_nodes) {
        LOG(ERROR) << "node_id must be in [1, num_nodes], got node_id="
                   << FLAGS_node_id << ", num_nodes=" << FLAGS_num_nodes;
        return 1;
    }

    if (!initialize_real_client()) {
        LOG(ERROR) << "Failed to initialize RealClient";
        return 1;
    }

    bool ok = false;
    if (FLAGS_workload_mode == "preload_then_read") {
        ok = preload_keys();
        if (!ok) {
            LOG(ERROR) << "preload-then-read: preload phase failed";
        } else {
            LOG(INFO) << "preload-then-read: starting stress-read phase";
            ok = stress_read();
        }
    } else if (FLAGS_workload_mode == "op_sequence") {
        LOG(INFO) << "Starting op_sequence workload";
        ok = operation_sequence_workload();
    } else if (FLAGS_workload_mode == "rw_correctness") {
        LOG(INFO) << "Starting rw_correctness workload";
        ok = concurrent_rw_correctness_workload();
    } else {
        LOG(ERROR) << "Unknown workload_mode: " << FLAGS_workload_mode;
        ok = false;
    }

    g_client.reset();
    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
