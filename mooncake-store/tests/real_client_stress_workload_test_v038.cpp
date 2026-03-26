// real_client_stress_workload_test_v038.cpp
//
// v0.3.8-compatible adaptation of real_client_stress_workload_test.cpp.
//
// Notes (API differences vs newer branches):
// - Init: RealClient::setup_real(flat params) (no ClientConfigBuilder).
// - Write: put_from(key, buf, size, ReplicateConfig).
// - Read:  get_into(key, buf, size) (no ReadRouteConfig).
//
// Usage (example you actually run: master in container + 3 clients):
//
//   # Start master (host network; adjust image/tag as needed)
//   nerdctl --namespace k8s.io run -d --name mooncake-node-cxy380 --network host \
//     --privileged \
//     --device /dev/infiniband/uverbs0 \
//     --device /dev/infiniband/uverbs1 \
//     -v /sys/class/infiniband:/sys/class/infiniband:ro \
//     -v /sys/class/infiniband_verbs:/sys/class/infiniband_verbs:ro \
//     localhost/mooncake-p2p-test:v380 \
//     /vllm-workspace/Mooncake/build/mooncake-store/src/mooncake_master \
//       --rpc_address=0.0.0.0 \
//       --port=50051 \
//       --rpc_thread_num=16 \
//       --enable_http_metadata_server=true \
//       --http_metadata_server_host=0.0.0.0 \
//       --http_metadata_server_port=8080 \
//       --default_kv_lease_ttl=600000 \
//       --default_kv_soft_pin_ttl=3600000
//
//   # Enter container (optional; to run clients in the same env)
//   nerdctl --namespace k8s.io exec -it mooncake-node-cxy380 bash
//
//   export MC_STORE_MEMCPY=1
//   START_MS=$(($(date +%s%3N) + 80000))  # use the same START_MS on all nodes
//
//   # Node 1
//   /vllm-workspace/Mooncake/build/mooncake-store/tests/real_client_stress_workload_test_v038 \
//     --local_hostname=192.168.200.25:12345 \
//     --master_address=192.168.200.25:50051 \
//     --metadata_connection_string=http://192.168.200.25:8080/metadata \
//     --protocol=tcp \
//     --node_id=1 --num_nodes=3 \
//     --key_count=1000 --value_size=1048576 \
//     --num_threads=4 --test_operation_nums=500 \
//     --remote_read_ratio=0.5 \
//     --global_segment_size=2147483648 \
//     --start_timestamp_ms=$START_MS
//
//   # Node 2 (same START_MS)
//   /vllm-workspace/Mooncake/build/mooncake-store/tests/real_client_stress_workload_test_v038 \
//     --local_hostname=192.168.200.15:12345 \
//     --master_address=192.168.200.25:50051 \
//     --metadata_connection_string=http://192.168.200.25:8080/metadata \
//     --protocol=tcp \
//     --node_id=2 --num_nodes=3 \
//     --key_count=1000 --value_size=1048576 \
//     --num_threads=4 --test_operation_nums=500 \
//     --remote_read_ratio=0.5 \
//     --global_segment_size=2147483648 \
//     --start_timestamp_ms=$START_MS
//
//   # Node 3 (same START_MS)
//   /vllm-workspace/Mooncake/build/mooncake-store/tests/real_client_stress_workload_test_v038 \
//     --local_hostname=192.168.200.27:12345 \
//     --master_address=192.168.200.25:50051 \
//     --metadata_connection_string=http://192.168.200.25:8080/metadata \
//     --protocol=tcp \
//     --node_id=3 --num_nodes=3 \
//     --key_count=1000 --value_size=1048576 \
//     --num_threads=4 --test_operation_nums=500 \
//     --remote_read_ratio=0.5 \
//     --global_segment_size=2147483648 \
//     --start_timestamp_ms=$START_MS

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

// v0.3.8: real_client.h pulls in the types we need (including ReplicateConfig).
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

// ── Workloads ─────────────────────────────────────────────────────────────
DEFINE_string(workload_mode, "preload_then_read",
              "Workload mode: preload_then_read | op_sequence | rw_correctness");
DEFINE_double(write_ratio, 0.3,
               "Write ratio in rw_correctness mode");
DEFINE_int32(op_sequence_max_rounds, 8,
              "Max rounds in op_sequence mode for each operation");
DEFINE_int32(rw_verify_retries, 50,
              "Max retries for read-back verification in rw_correctness mode");
DEFINE_int32(rw_verify_sleep_ms, 2,
              "Sleep milliseconds between read-back verification retries");
DEFINE_int32(rw_key_pool_size, 0,
              "Unique key pool size for rw_correctness. "
              "0 means use --key_count.");

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

// ── Client initialization ────────────────────────────────────────────────────
// v0.3.8: RealClient::setup_real() with flat parameters.
bool initialize_real_client() {
    auto client = RealClient::create();

    int rc = client->setup_real(
        FLAGS_local_hostname,             // v0.3.8 requires "host:port"
        FLAGS_metadata_connection_string, // HTTP metadata endpoint URL
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
                           double& p80, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p80 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    const size_t n = latencies.size();

    auto idx = [&](double ratio) -> size_t {
        return static_cast<size_t>(std::ceil((n * ratio) - 1));
    };

    p50 = latencies[idx(0.50)];
    p80 = latencies[idx(0.80)];
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

// ── Preload ──────────────────────────────────────────────────────────────────
// v0.3.8: put_from(key, buf, size, ReplicateConfig)
bool preload_keys() {
    std::vector<char> buffer(FLAGS_value_size, 'A');
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "register_buffer failed in preload";
        return false;
    }

    ReplicateConfig write_cfg;
    // v0.3.8 test: current RealClient rejects prefer_alloc_in_same_node.
    // Keep default behavior (false) to allow put_from/preload to succeed.
    write_cfg.prefer_alloc_in_same_node = false;
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
// v0.3.8: get_into(key, buf, size) (no routing config parameter).
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
            // Uniform random over all remote keys (across all remote nodes).
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

// ── Stress-read coordinator ──────────────────────────────────────────────────
bool stress_read() {
    // Derive remote node IDs from (node_id, num_nodes).
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

ReplicateConfig build_write_config() {
    ReplicateConfig cfg;
    // Keep default behavior; prefer_alloc_in_same_node is not supported in
    // current RealClient.
    cfg.prefer_alloc_in_same_node = false;
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
        return;
    }

    ReplicateConfig write_cfg = build_write_config();

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
                int64_t size = g_client->get_into(key, buffer.data(), buffer.size());
                auto re = std::chrono::high_resolution_clock::now();
                const double latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(re - rs).count();

                if (size >= 0) {
                    stats.successful_reads++;
                    stats.successful_read_time_us += latency_us;
                    stats.read_latencies_us.push_back(latency_us);
                    done = true;
                } else {
                    stats.read_failures++;
                }
            } else {
                // not exists -> put -> exist again
                const char write_val =
                    static_cast<char>(((thread_id + i + round) % 251) + 1);
                std::fill(buffer.begin(), buffer.end(), write_val);

                stats.write_attempts++;
                auto ws = std::chrono::high_resolution_clock::now();
                int rc = g_client->put_from(key, buffer.data(), buffer.size(), write_cfg);
                auto we = std::chrono::high_resolution_clock::now();
                const double latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(we - ws).count();

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
        "rwcheck_node_" + std::to_string(FLAGS_node_id) + "_thread_" + std::to_string(thread_id);

    const int key_pool_size =
        std::max(1, FLAGS_rw_key_pool_size > 0 ? FLAGS_rw_key_pool_size : FLAGS_key_count);

    ReplicateConfig write_cfg = build_write_config();
    char expected = 0;
    bool has_written = false;
    int write_version = 0;
    std::string current_key;

    std::mt19937 rng(FLAGS_random_seed + 131 * thread_id);
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    for (int i = 0; i < FLAGS_test_operation_nums; ++i) {
        const bool do_write = !has_written || (dist(rng) < FLAGS_write_ratio);
        if (do_write) {
            const int key_idx = write_version++ % key_pool_size;
            const char candidate_expected =
                static_cast<char>((key_idx % 251) + 1);
            const std::string candidate_key =
                key_prefix + "_k_" + std::to_string(key_idx);

            std::fill(write_buffer.begin(), write_buffer.end(), candidate_expected);
            stats.write_attempts++;

            auto ws = std::chrono::high_resolution_clock::now();
            int rc = g_client->put_from(
                candidate_key, write_buffer.data(), write_buffer.size(), write_cfg);
            auto we = std::chrono::high_resolution_clock::now();
            const double latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(we - ws).count();

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
        if (!has_written || current_key.empty()) continue;

        bool verified = false;
        bool read_ok = false;

        const unsigned char expected_uc = static_cast<unsigned char>(expected);
        for (int retry = 0; retry < kReadVerifyRetries && !verified; ++retry) {
            stats.read_attempts++;
            std::fill(read_buffer.begin(), read_buffer.end(), 0);

            auto rs = std::chrono::high_resolution_clock::now();
            int64_t size =
                g_client->get_into(current_key, read_buffer.data(), read_buffer.size());
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
                if (static_cast<unsigned char>(read_buffer[static_cast<size_t>(j)]) !=
                    expected_uc) {
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
            }

            if (!g_rw_correctness_mismatch_logged.exchange(true)) {
                LOG(ERROR) << "rw_correctness mismatch (first occurrence): "
                           << "thread_id=" << thread_id
                           << ", key=" << current_key
                           << ", expected=" << static_cast<int>(expected_uc)
                           << ", actual[" << mismatch_idx << "]="
                           << static_cast<int>(actual_byte);
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

    bool ok = false;
    if (FLAGS_workload_mode == "preload_then_read") {
        // Preload phase: write this node's keys into the cluster.
        ok = preload_keys();
        if (!ok) {
            LOG(ERROR) << "preload-then-read: preload phase failed";
        } else {
            // Optional: wait until a shared start time so nodes begin reads together.
            // (requires reasonably synchronized clocks).
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

            // Keep alive briefly so other nodes can still issue remote reads.
            if (ok) {
                LOG(INFO)
                    << "Test finished successfully, keeping process alive for 120s "
                       "before cleanup to allow in-flight remote reads";
                std::this_thread::sleep_for(std::chrono::seconds(120));
            } else {
                LOG(WARNING) << "Test failed, skipping post-test delay.";
            }
        }
    } else if (FLAGS_workload_mode == "op_sequence") {
        if (FLAGS_start_timestamp_ms > 0) {
            using clock = std::chrono::system_clock;
            const auto now = clock::now();
            const int64_t now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch())
                    .count();
            if (FLAGS_start_timestamp_ms > now_ms) {
                const int64_t wait_ms = FLAGS_start_timestamp_ms - now_ms;
                LOG(INFO) << "op_sequence: waiting " << wait_ms
                          << " ms to align global start";
                std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
            }
        }
        LOG(INFO) << "Starting op_sequence workload";
        ok = operation_sequence_workload();
    } else if (FLAGS_workload_mode == "rw_correctness") {
        if (FLAGS_start_timestamp_ms > 0) {
            using clock = std::chrono::system_clock;
            const auto now = clock::now();
            const int64_t now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch())
                    .count();
            if (FLAGS_start_timestamp_ms > now_ms) {
                const int64_t wait_ms = FLAGS_start_timestamp_ms - now_ms;
                LOG(INFO) << "rw_correctness: waiting " << wait_ms
                          << " ms to align global start";
                std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
            }
        }
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
