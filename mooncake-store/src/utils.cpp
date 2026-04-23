#include "utils.h"

#include <Slab.h>
#include <cstring>
#include <cstdio>
#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>

#include <random>
#ifdef USE_ASCEND_DIRECT
#include "acl/acl.h"
#endif

#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

namespace {

double read_proc_kb_field_mb(const std::string& path, const char* field_prefix) {
    FILE* fp = std::fopen(path.c_str(), "r");
    if (fp == nullptr) {
        return -1.0;
    }
    const size_t prefix_len = std::strlen(field_prefix);
    char line[512];
    while (std::fgets(line, sizeof(line), fp) != nullptr) {
        if (std::strncmp(line, field_prefix, prefix_len) == 0) {
            long value_kb = 0;
            if (std::sscanf(line + prefix_len, " %ld kB", &value_kb) == 1) {
                std::fclose(fp);
                return static_cast<double>(value_kb) / 1024.0;
            }
        }
    }
    std::fclose(fp);
    return -1.0;
}

std::string build_linux_memory_snapshot() {
    std::ostringstream oss;
    const auto pid = static_cast<long>(::getpid());
    const auto tid = get_current_tid();
    const std::string proc_status_path = "/proc/self/status";
    const std::string task_status_path =
        (tid > 0)
            ? ("/proc/self/task/" + std::to_string(tid) + "/status")
            : std::string();
    const std::string smaps_rollup_path = "/proc/self/smaps_rollup";

    struct rusage usage {};
    long minflt = -1;
    long majflt = -1;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        minflt = usage.ru_minflt;
        majflt = usage.ru_majflt;
    }

    oss << "pid=" << pid << ", tid=" << tid
        << ", rss_mb=" << get_current_rss_mb()
        << ", vmrss_mb="
        << read_proc_kb_field_mb(proc_status_path, "VmRSS:")
        << ", vmhwm_mb="
        << read_proc_kb_field_mb(proc_status_path, "VmHWM:")
        << ", rss_anon_mb="
        << read_proc_kb_field_mb(proc_status_path, "RssAnon:")
        << ", rss_file_mb="
        << read_proc_kb_field_mb(proc_status_path, "RssFile:")
        << ", rss_shmem_mb="
        << read_proc_kb_field_mb(proc_status_path, "RssShmem:");

    if (!task_status_path.empty()) {
        oss << ", task_vmrss_mb="
            << read_proc_kb_field_mb(task_status_path, "VmRSS:")
            << ", task_vmhwm_mb="
            << read_proc_kb_field_mb(task_status_path, "VmHWM:");
    }

    oss << ", smaps_rss_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "Rss:")
        << ", smaps_pss_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "Pss:")
        << ", smaps_anon_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "Anonymous:")
        << ", smaps_private_dirty_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "Private_Dirty:")
        << ", smaps_shared_clean_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "Shared_Clean:")
        << ", smaps_anon_huge_mb="
        << read_proc_kb_field_mb(smaps_rollup_path, "AnonHugePages:")
        << ", minflt=" << minflt
        << ", majflt=" << majflt;
    return oss.str();
}

}  // namespace

bool isPortAvailable(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bool available = (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    close(sock);
    return available;
}

// AutoPortBinder implementation
AutoPortBinder::AutoPortBinder(int min_port, int max_port)
    : socket_fd_(-1), port_(-1) {
    static std::random_device rand_gen;
    std::mt19937 gen(rand_gen());
    std::uniform_int_distribution<> rand_dist(min_port, max_port);

    for (int attempt = 0; attempt < 20; ++attempt) {
        int port = rand_dist(gen);

        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) continue;

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (bind(socket_fd_, (sockaddr *)&addr, sizeof(addr)) == 0) {
            port_ = port;
            break;
        } else {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
}

AutoPortBinder::~AutoPortBinder() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
    }
}

void *allocate_buffer_allocator_memory(size_t total_size,
                                       const std::string &protocol,
                                       size_t alignment) {
    const size_t default_alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (alignment == default_alignment && total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
#ifdef USE_ASCEND_DIRECT
    if (protocol == "ascend" && total_size > 0) {
        void *buffer = nullptr;
        auto ret = aclrtMallocHost(&buffer, total_size);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to allocate memory: " << ret;
            return nullptr;
        }
        return buffer;
    }
#endif
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

void free_memory(const std::string &protocol, void *ptr) {
#ifdef USE_ASCEND_DIRECT
    if (protocol == "ascend") {
        aclrtFreeHost(ptr);
        return;
    }
#endif
    free(ptr);
}

double get_current_rss_mb() {
#if defined(__linux__)
    long rss_pages = 0;
    FILE* statm = std::fopen("/proc/self/statm", "r");
    if (statm != nullptr) {
        if (std::fscanf(statm, "%*ld %ld", &rss_pages) == 1 &&
            rss_pages >= 0) {
            const long page_size = sysconf(_SC_PAGESIZE);
            std::fclose(statm);
            if (page_size > 0) {
                return static_cast<double>(rss_pages) * page_size /
                       (1024.0 * 1024.0);
            }
        } else {
            std::fclose(statm);
        }
    }
#endif

    struct rusage usage {};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
#if defined(__APPLE__)
        return static_cast<double>(usage.ru_maxrss) / (1024.0 * 1024.0);
#else
        return static_cast<double>(usage.ru_maxrss) / 1024.0;
#endif
    }
    return -1.0;
}

long get_current_tid() {
#if defined(__linux__)
    return static_cast<long>(::syscall(SYS_gettid));
#else
    return -1;
#endif
}

double get_current_thread_rss_mb() {
#if defined(__linux__)
    const long tid = get_current_tid();
    if (tid > 0) {
        const std::string status_path =
            "/proc/self/task/" + std::to_string(tid) + "/status";
        return read_proc_kb_field_mb(status_path, "VmRSS:");
    }
#endif
    return -1.0;
}

std::string get_rss_snapshot_for_current_thread() {
#if defined(__linux__)
    return build_linux_memory_snapshot();
#else
    std::ostringstream oss;
    oss << "pid=" << static_cast<long>(::getpid())
        << ", tid=" << get_current_tid()
        << ", rss_mb=" << get_current_rss_mb();
    return oss.str();
#endif
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::vector<std::string> splitString(const std::string &str, char delimiter,
                                     bool trim_spaces, bool keep_empty) {
    std::vector<std::string> result;

    boost::split(
        result, str, boost::is_any_of(std::string(1, delimiter)),
        keep_empty ? boost::token_compress_off : boost::token_compress_on);

    if (trim_spaces) {
        for (auto &token : result) {
            boost::trim(token);
        }
    }

    return result;
}

tl::expected<std::string, int> httpGet(const std::string &url) {
    coro_http::coro_http_client client;
    auto res = client.get(url);
    if (res.status == 200) {
        return std::string(res.resp_body);
    }
    return tl::unexpected(res.status);
}

int getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        ::close(sock);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

int64_t time_gen() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string GetEnvStringOr(const char *name, const std::string &default_value) {
    const char *env_val = std::getenv(name);
    return env_val ? std::string(env_val) : default_value;
}

}  // namespace mooncake
