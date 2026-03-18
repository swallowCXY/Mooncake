// real_client_stress_workload_test_v038.cpp
//
// v0.3.8-compatible adaptation of real_client_stress_workload_test.cpp.
//
// Key API differences from pr-1631:
//   ┌─────────────────┬──────────────────────────────────────────┬──────────────────────────────────────┐
//   │ Feature         │ pr-1631                                  │ v0.3.8                               │
//   ├─────────────────┼──────────────────────────────────────────┼──────────────────────────────────────┤
//   │ Client init     │ ClientConfigBuilder + setup(config)       │ setup_real(flat params)              │
//   │ Write API       │ put_from(key, buf, size, WriteConfig)     │ put_from(key, buf, size, ReplicateConfig) │
//   │ Read API        │ get_into(key, buf, size, ReadRouteConfig) │ get_into(key, buf, size)             │
//   │ Mode            │ p2p | centralized                        │ centralized only                     │
//   │ Headers         │ client_config_builder.h, p2p_rpc_types.h │ real_client.h + replica.h (via it)   │
//   └─────────────────┴──────────────────────────────────────────┴──────────────────────────────────────┘
//
// Usage (two-node example):
//   # Start master
//   ./mooncake_master --deployment_mode=Centralization \
//     --rpc_port=50051 --enable_http_metadata_server=true \
//     --http_metadata_server_port=8080
//
//   # Node 1 (preload + stress-read in same process)
//   START_MS=$(date -d '+60 seconds' +%s%3N)
//   ./real_client_stress_workload_test_v038 \
//     --local_hostname=10.0.0.11:12345 \
//     --master_address=10.0.0.1:50051 \
//     --metadata_connection_string=http://10.0.0.1:8080 \
//     --protocol=tcp \
//     --node_id=1 --num_nodes=2 \
//     --key_count=1000 --value_size=1048576 \
//     --num_threads=8 --test_operation_nums=1000 \
//     --remote_read_ratio=0.5 \
//     --global_segment_size=2147483648 \
//     --start_timestamp_ms=$START_MS
//
//   # Node 2 (same START_MS, different node_id and local_hostname)
//   ./real_client_stress_workload_test_v038 \
//     --local_hostname=10.0.0.12:12346 \
//     --node_id=2 --num_nodes=2 ...

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

// v0.3.8: real_client.h includes rpc_types.h which includes replica.h,
// so ReplicateConfig is available without a separate include.
// No client_config_builder.h or p2p_rpc_types.h needed.
#include "real_client.h"

using namespace mooncake;

// ── Network / master ────────────────────────────────────────────────────────
DEFINE_string(protocol, "tcp", "Transfer protocol: tcp|rdma");
DEFINE_string(master_address, "127.0.0.1:50051", "Master server address");
DEFINE_string(local_hostname, "127.0.0.1:12345",
              "Local hostname:port (v0.3.8 requires port, "
              "e.g. 10.0.0.11:12345)");
DEFINE_string(metadata_connection_string, "http://127.0.0.1:8080",
              "Metadata connection string for centralized mode, "
              "e.g. http://master-ip:8080");
DEFINE_string(device_names, "", "RDMA device names (leave empty for TCP)");

// ── Memory ───────────────────────────────────────────────────────────────────
DEFINE_int64(global_segment_size, 2147483648LL,
             "Global segment size (bytes) registered with the cluster. "
             "Must be >= key_count * value_size * replica_num. Default 2 GB.");
DEFINE_int64(local_buffer_size, 16777216LL,
             "Internal client buffer size (bytes) for put operations. "
             "Default 16 MB.");

// ── Benchmark ────────────────────────────────────────────────────────────────
DEFINE_int32(node_id, 1, "Current node ID (1-based)");
DEFINE_int32(num_nodes, 1, "Total number of nodes in the test cluster");
DEFINE_int32(key_count, 1000, "Number of keys to preload on this node");
DEFINE_int32(num_threads, 8, "Number of worker threads");
DEFINE_int32(test_operation_nums, 1000, "Operations per thread");
DEFINE_int32(value_size, 1048576, "Value size in bytes (default 1 MB)");
DEFINE_int32(warmup_ops, 100, "Warmup ops per thread (excluded from stats)");
DEFINE_double(remote_read_ratio, 0.5,
              "Fraction of reads targeting remote nodes [0,1]");
DEFINE_int32(random_seed, 12345, "Random seed");
DEFINE_int64(start_timestamp_ms, 0,
             "Global start timestamp (ms since Unix epoch) for the read phase. "
             "If 0, start immediately after preload. "
             "Set the same value on all nodes to synchronize read start.");

namespace {

std::shared_ptr<RealClient> g_client;

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
};

std::string generate_key(int node_id, int idx) {
    return "node_" + std::to_string(node_id) + "_obj_" + std::to_string(idx);
}

// ── Client initialization ────────────────────────────────────────────────────
// v0.3.8: uses RealClient::setup_real() with flat parameters.
// pr-1631 equivalent uses ClientConfigBuilder + setup(config).
bool initialize_real_client() {
    auto client = RealClient::create();

    int rc = client->setup_real(
        FLAGS_local_hostname,             // "host:port" format required in v0.3.8
        FLAGS_metadata_connection_string, // HTTP metadata server URL
        static_cast<size_t>(FLAGS_global_segment_size),
        static_cast<size_t>(FLAGS_local_buffer_size),
        FLAGS_protocol,
        FLAGS_device_names,
        FLAGS_master_address);

    if (rc != 0) {
        LOG(ERROR) << "RealClient setup_real failed, rc=" << rc;
        return false;
    }

    g_client = std::move(client);
    return true;
}

// ── Percentile calculation ───────────────────────────────────────────────────
void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p90, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p90 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    const size_t n = latencies.size();

    auto idx = [&](double ratio) -> size_t {
        return static_cast<size_t>(std::ceil((n * ratio) - 1));
    };

    p50 = latencies[idx(0.50)];
    p90 = latencies[idx(0.90)];
    p95 = latencies[idx(0.95)];
    p99 = latencies[idx(0.99)];
}

// ── Result printing ──────────────────────────────────────────────────────────
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

    double all_p50, all_p90, all_p95, all_p99;
    double local_p50, local_p90, local_p95, local_p99;
    double remote_p50, remote_p90, remote_p95, remote_p99;

    calculate_percentiles(all_latencies, all_p50, all_p90, all_p95, all_p99);
    calculate_percentiles(local_latencies, local_p50, local_p90, local_p95,
                          local_p99);
    calculate_percentiles(remote_latencies, remote_p50, remote_p90, remote_p95,
                          remote_p99);

    const double reads_per_sec =
        duration_s > 0 ? successful_reads / duration_s : 0.0;
    const double data_mb_per_sec =
        duration_s > 0
            ? (successful_reads * FLAGS_value_size) / (duration_s * 1024 * 1024)
            : 0.0;

    LOG(INFO) << "=== RealClient Multi-Node Stress Results (v0.3.8) ===";
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
    LOG(INFO) << "Reads/sec: " << reads_per_sec;
    LOG(INFO) << "Data throughput(MB/s): " << data_mb_per_sec;
    LOG(INFO) << "All latency(us) p50/p90/p95/p99 = "
              << all_p50 << "/" << all_p90 << "/" << all_p95 << "/" << all_p99;
    LOG(INFO) << "Local latency(us) p50/p90/p95/p99 = "
              << local_p50 << "/" << local_p90 << "/" << local_p95 << "/"
              << local_p99;
    LOG(INFO) << "Remote latency(us) p50/p90/p95/p99 = "
              << remote_p50 << "/" << remote_p90 << "/" << remote_p95 << "/"
              << remote_p99;
}

// ── Preload ──────────────────────────────────────────────────────────────────
// v0.3.8: put_from(key, void* buf, size_t size, const ReplicateConfig&)
// pr-1631: put_from(key, void* buf, size_t size, WriteConfig variant)
bool preload_keys() {
    std::vector<char> buffer(FLAGS_value_size, 'A');
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "register_buffer failed in preload";
        return false;
    }

    // v0.3.8: ReplicateConfig is passed directly (no WriteConfig variant).
    ReplicateConfig write_cfg;
    write_cfg.prefer_alloc_in_same_node = true; 
    for (int i = 0; i < FLAGS_key_count; ++i) {
        const auto key = generate_key(FLAGS_node_id, i);
        std::fill(buffer.begin(), buffer.end(),
                  static_cast<char>('A' + (i % 26)));

        const int rc =
            g_client->put_from(key, buffer.data(), buffer.size(), write_cfg);
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

// ── Stress-read worker ───────────────────────────────────────────────────────
// v0.3.8: get_into(key, void* buf, size_t size) — no ReadRouteConfig param.
// pr-1631: get_into(key, void* buf, size_t size, ReadRouteConfig)
void stress_read_worker(int thread_id,
                        const std::vector<std::string>& /*local_keys*/,
                        const std::vector<int>& remote_node_ids,
                        std::atomic<bool>& stop_flag, ThreadStats& stats) {
    std::vector<char> buffer(FLAGS_value_size, 0);
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id << ": register_buffer failed";
        return;
    }

    std::mt19937 rng(FLAGS_random_seed + thread_id);
    std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

    auto pick_index = [&](size_t sz) -> size_t {
        std::uniform_int_distribution<size_t> dist(0, sz - 1);
        return dist(rng);
    };

    const int total_ops = FLAGS_warmup_ops + FLAGS_test_operation_nums;
    const bool has_remote_nodes = !remote_node_ids.empty();
    const int total_remote_keys =
        has_remote_nodes
            ? static_cast<int>(remote_node_ids.size()) * FLAGS_key_count
            : 0;

    for (int i = 0; i < total_ops && !stop_flag.load(); ++i) {
        const bool choose_remote =
            has_remote_nodes && (ratio_dist(rng) < FLAGS_remote_read_ratio);

        int target_node = FLAGS_node_id;
        int key_idx = 0;

        if (choose_remote) {
            if (total_remote_keys <= 0) continue;
            // Uniform random across ALL remote keys (not per-node).
            std::uniform_int_distribution<int> remote_key_dist(
                0, total_remote_keys - 1);
            const int gidx = remote_key_dist(rng);
            const int remote_node_slot = gidx / FLAGS_key_count;
            const int remote_key_idx   = gidx % FLAGS_key_count;
            target_node = remote_node_ids[static_cast<size_t>(remote_node_slot)];
            key_idx = remote_key_idx;
        } else {
            key_idx = static_cast<int>(
                pick_index(static_cast<size_t>(FLAGS_key_count)));
        }

        const std::string key = generate_key(target_node, key_idx);

        const auto t_start = std::chrono::high_resolution_clock::now();
        // v0.3.8: get_into has no ReadRouteConfig parameter.
        const int64_t bytes_read =
            g_client->get_into(key, buffer.data(), buffer.size());
        const auto t_end = std::chrono::high_resolution_clock::now();

        if (i >= FLAGS_warmup_ops) {
            OperationResult result;
            result.latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    t_end - t_start)
                    .count();
            result.expected_remote = choose_remote;
            result.success = bytes_read >= 0;
            result.query_success = result.success;
            stats.reads.push_back(result);

            stats.total_reads++;
            if (choose_remote) {
                stats.remote_reads++;
            } else {
                stats.local_reads++;
            }

            if (result.success) {
                stats.successful_reads++;
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

// ── Stress-read coordinator ──────────────────────────────────────────────────
bool stress_read() {
    // Build remote node list from node_id + num_nodes (no shared file needed).
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
    std::vector<std::string> dummy_local_keys;

    const auto t_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(stress_read_worker, i,
                             std::cref(dummy_local_keys),
                             std::cref(remote_node_ids), std::ref(stop_flag),
                             std::ref(thread_stats[i]));
    }

    for (auto& t : workers) t.join();

    const auto t_end = std::chrono::high_resolution_clock::now();
    const double duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start)
            .count() /
        1000.0;

    print_read_results(thread_stats, duration_s);
    return true;
}

}  // namespace

// ── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    LOG(INFO) << "Starting RealClient stress workload test (v0.3.8)"
              << ", node_id=" << FLAGS_node_id
              << ", master=" << FLAGS_master_address
              << ", local_hostname=" << FLAGS_local_hostname;

    if (FLAGS_remote_read_ratio < 0.0 || FLAGS_remote_read_ratio > 1.0) {
        LOG(ERROR) << "remote_read_ratio must be in [0, 1]";
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

    // Preload phase: write this node's keys into the cluster.
    bool ok = preload_keys();
    if (!ok) {
        LOG(ERROR) << "preload-then-read: preload phase failed";
    } else {
        // Optional: wait until a globally-agreed start time so all nodes
        // begin the read phase at the same moment (requires NTP sync).
        if (FLAGS_start_timestamp_ms > 0) {
            using clock = std::chrono::system_clock;
            const auto now = clock::now();
            const int64_t now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch())
                    .count();
            if (FLAGS_start_timestamp_ms > now_ms) {
                const int64_t wait_ms = FLAGS_start_timestamp_ms - now_ms;
                LOG(INFO) << "preload-then-read: preload done, waiting "
                          << wait_ms
                          << " ms to align global start (start_ts_ms="
                          << FLAGS_start_timestamp_ms << ", now_ms=" << now_ms
                          << ")";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(wait_ms));
            } else {
                LOG(WARNING)
                    << "preload-then-read: start_timestamp_ms ("
                    << FLAGS_start_timestamp_ms
                    << ") is in the past, starting stress-read immediately";
            }
        }
        LOG(INFO) << "preload-then-read: starting stress-read phase";
        ok = stress_read();
    }

    // Keep the process alive for 30 s so other nodes can still issue remote
    // reads against this node's data before cleanup.
    if (ok) {
        LOG(INFO) << "Test finished successfully, keeping process alive for "
                     "30 s before cleanup to allow in-flight remote reads";
        std::this_thread::sleep_for(std::chrono::seconds(30));
    } else {
        LOG(WARNING) << "Test failed, skipping post-test delay.";
    }

    g_client.reset();
    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
