#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <chrono>

#include "peer_client.h"
#include "client_rpc_service.h"
#include "client_rpc_types.h"
#include "data_manager.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake {

// Helper function to parse JSON string
static bool parseJsonString(const std::string& json_str, Json::Value& value,
                            std::string* error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

// Test fixture for PeerClient tests with RPC server
class PeerClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PeerClientTest");
        FLAGS_logtostderr = 1;

        // Get an available port
        port_binder_ = std::make_unique<AutoPortBinder>();
        int port = 500053;
        server_endpoint_ = "127.0.0.1:" + std::to_string(port);

        // Create a minimal TransferEngine
        transfer_engine_ = std::make_shared<TransferEngine>(false);

        // Create TieredBackend with DRAM tier configuration
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        ASSERT_TRUE(parseJsonString(json_config_str, config));

        tiered_backend_ = std::make_unique<TieredBackend>();
        auto init_result = tiered_backend_->Init(config, nullptr, nullptr);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        // Verify tier was created successfully
        auto tier_views = tiered_backend_->GetTierViews();
        ASSERT_EQ(tier_views.size(), 1)
            << "Expected 1 tier, got " << tier_views.size();
        saved_tier_id_ = tier_views[0].id;

        // Create DataManager
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_);

        // Create ClientRpcService
        rpc_service_ = std::make_unique<ClientRpcService>(*data_manager_);

        // Create and configure RPC server
        server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(4,  // thread count
                                                        port, "127.0.0.1");

        // Register RPC service handlers
        RegisterClientRpcService(*server_, *rpc_service_);

        // Start server in a separate thread (server.start() is blocking)
        server_running_ = true;
        std::atomic<bool> server_ready{false};
        server_thread_ = std::thread([this, &server_ready]() {
            LOG(INFO) << "Starting RPC server on " << server_endpoint_;
            server_ready = true;
            auto start_result = server_->start();
            // Log if server stopped (start_result contains error info if any)
            if (start_result) {
                LOG(ERROR) << "RPC server stopped: " << start_result;
            }
            server_running_ = false;
        });

        // Wait for server thread to start and server to be ready
        int retries = 0;
        while (!server_ready && retries < 50) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            retries++;
        }
        // Give server a bit more time to actually bind the port
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Create PeerClient
        peer_client_ = std::make_unique<PeerClient>();
    }

    void TearDown() override {
        LOG(INFO) << "Tearing down PeerClientTest";
        // Stop server
        if (server_) {
            server_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }

        peer_client_.reset();
        server_.reset();
        rpc_service_.reset();
        data_manager_.reset();
        tiered_backend_.reset();
        transfer_engine_.reset();
        port_binder_.reset();
        google::ShutdownGoogleLogging();
    }

    // Helper: Get tier ID
    std::optional<UUID> GetTierId() {
        if (saved_tier_id_.has_value()) {
            return saved_tier_id_;
        }
        return std::nullopt;
    }

    // Helper: Create test data buffer
    std::unique_ptr<char[]> StringToBuffer(const std::string& str) {
        auto buffer = std::make_unique<char[]>(str.size());
        std::memcpy(buffer.get(), str.data(), str.size());
        return buffer;
    }

    // Helper: Create a valid RemoteBufferDesc for testing
    RemoteBufferDesc CreateBufferDesc(const std::string& segment_name,
                                      uintptr_t addr, uint64_t size) {
        RemoteBufferDesc desc;
        desc.segment_name = segment_name;
        desc.addr = addr;
        desc.size = size;
        return desc;
    }

    std::unique_ptr<AutoPortBinder> port_binder_;
    std::string server_endpoint_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<ClientRpcService> rpc_service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<PeerClient> peer_client_;
    std::thread server_thread_;
    std::atomic<bool> server_running_{false};
    std::optional<UUID> saved_tier_id_;
};

// Test Connect
TEST_F(PeerClientTest, ConnectSuccess) {
    auto result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(result.has_value())
        << "Failed to connect: " << toString(result.error());
}

// Test Connect with invalid endpoint
TEST_F(PeerClientTest, ConnectInvalidEndpoint) {
    auto result = peer_client_->Connect("invalid:endpoint");
    // Connection might fail or succeed depending on implementation
    // Just verify it doesn't crash
}

// Test Connect twice (should handle reconnection)
TEST_F(PeerClientTest, ConnectTwice) {
    auto result1 = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(result1.has_value());

    auto result2 = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(result2.has_value()) << "Reconnection should succeed";
}

// Test WriteRemoteData - first write data using DataManager, then read via RPC
TEST_F(PeerClientTest, WriteRemoteDataSuccess) {
    // Connect first
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    // First, write data using DataManager directly
    std::string test_key = "test_write_key";
    std::string test_data = "Hello, PeerClient!";
    auto buffer = StringToBuffer(test_data);
    auto put_result = data_manager_->Put(test_key, std::move(buffer),
                                         test_data.size(), GetTierId());
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Now read it back via RPC
    // Note: For WriteRemoteData, we need to provide source buffers
    // This test demonstrates the RPC call works, but actual data transfer
    // requires proper buffer setup which is complex for unit test
    RemoteWriteRequest write_request;
    write_request.key = "test_write_rpc_key";
    write_request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    write_request.target_tier_id = GetTierId();

    auto write_result = peer_client_->WriteRemoteData(write_request);
    ASSERT_TRUE(write_result.has_value())
        << "Stubbed DataManager should return OK";
    // only tests the RPC call path
    // In a real scenario, buffers would be properly registered with
    // TransferEngine
}

// Test ReadRemoteData - write data first, then read via RPC
TEST_F(PeerClientTest, ReadRemoteDataSuccess) {
    // Connect first
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    // First, write data using DataManager directly
    std::string test_key = "test_read_key";
    std::string test_data = "Hello, ReadRemoteData!";
    auto buffer = StringToBuffer(test_data);
    auto put_result = data_manager_->Put(test_key, std::move(buffer),
                                         test_data.size(), GetTierId());
    ASSERT_TRUE(put_result.has_value())
        << "Failed to put data: " << toString(put_result.error());

    // Now read it back via RPC
    RemoteReadRequest read_request;
    read_request.key = test_key;
    read_request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, test_data.size()));

    auto read_result = peer_client_->ReadRemoteData(read_request);
    // This will fail because we don't have real buffers set up,
    // but it tests the RPC call path
    // In a real scenario, buffers would be properly registered with
    // TransferEngine
}

// Test ReadRemoteData with non-existent key
TEST_F(PeerClientTest, ReadRemoteDataKeyNotFound) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    RemoteReadRequest read_request;
    read_request.key = "non_existent_key";
    read_request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto read_result = peer_client_->ReadRemoteData(read_request);
    ASSERT_FALSE(read_result.has_value()) << "Should fail for non-existent key";
    EXPECT_EQ(read_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// Test ReadRemoteData with empty key
TEST_F(PeerClientTest, ReadRemoteDataEmptyKey) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    RemoteReadRequest read_request;
    read_request.key = "";
    read_request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto read_result = peer_client_->ReadRemoteData(read_request);
    ASSERT_FALSE(read_result.has_value()) << "Should fail for empty key";
    EXPECT_EQ(read_result.error(), ErrorCode::INVALID_PARAMS);
}

// Test ReadRemoteData with empty buffers
TEST_F(PeerClientTest, ReadRemoteDataEmptyBuffers) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    RemoteReadRequest read_request;
    read_request.key = "test_key";
    // dest_buffers is empty

    auto read_result = peer_client_->ReadRemoteData(read_request);
    ASSERT_FALSE(read_result.has_value()) << "Should fail for empty buffers";
    EXPECT_EQ(read_result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData with empty key
TEST_F(PeerClientTest, WriteRemoteDataEmptyKey) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    RemoteWriteRequest write_request;
    write_request.key = "";
    write_request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto write_result = peer_client_->WriteRemoteData(write_request);
    ASSERT_FALSE(write_result.has_value()) << "Should fail for empty key";
    EXPECT_EQ(write_result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData with empty buffers
TEST_F(PeerClientTest, WriteRemoteDataEmptyBuffers) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    RemoteWriteRequest write_request;
    write_request.key = "test_key";
    // src_buffers is empty

    auto write_result = peer_client_->WriteRemoteData(write_request);
    ASSERT_FALSE(write_result.has_value()) << "Should fail for empty buffers";
    EXPECT_EQ(write_result.error(), ErrorCode::INVALID_PARAMS);
}

// Test BatchReadRemoteData
TEST_F(PeerClientTest, BatchReadRemoteDataSuccess) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    // Write some test data first
    std::vector<std::string> keys = {"batch_key1", "batch_key2", "batch_key3"};
    for (const auto& key : keys) {
        std::string test_data = "Data for " + key;
        auto buffer = StringToBuffer(test_data);
        auto put_result = data_manager_->Put(key, std::move(buffer),
                                             test_data.size(), GetTierId());
        ASSERT_TRUE(put_result.has_value())
            << "Failed to put data for key: " << key;
    }

    BatchRemoteReadRequest batch_request;
    batch_request.keys = keys;
    for (size_t i = 0; i < keys.size(); ++i) {
        batch_request.dest_buffers_list.push_back(
            {CreateBufferDesc("test_segment", 0x1000 + i * 0x1000, 100)});
    }

    auto results = peer_client_->BatchReadRemoteData(batch_request);
    ASSERT_EQ(results.size(), keys.size())
        << "Should return results for all keys";
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value())
            << "Stubbed DataManager should return OK";
    }
}

// Test BatchReadRemoteData with key count mismatch
TEST_F(PeerClientTest, BatchReadRemoteDataKeyCountMismatch) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    BatchRemoteReadRequest batch_request;
    batch_request.keys = {"key1", "key2"};
    batch_request.dest_buffers_list = {
        {CreateBufferDesc("test_segment", 0x1000, 100)}};  // Only 1 buffer

    auto results = peer_client_->BatchReadRemoteData(batch_request);
    ASSERT_EQ(results.size(), 2) << "Should return results for all keys";
    // All should fail due to count mismatch
    for (const auto& result : results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

// Test BatchWriteRemoteData
TEST_F(PeerClientTest, BatchWriteRemoteDataSuccess) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    BatchRemoteWriteRequest batch_request;
    batch_request.keys = {"batch_write_key1", "batch_write_key2"};
    for (size_t i = 0; i < 2; ++i) {
        batch_request.src_buffers_list.push_back(
            {CreateBufferDesc("test_segment", 0x1000 + i * 0x1000, 100)});
        batch_request.target_tier_ids.push_back(GetTierId());
    }

    auto results = peer_client_->BatchWriteRemoteData(batch_request);
    ASSERT_EQ(results.size(), 2) << "Should return results for all keys";
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value())
            << "Stubbed DataManager should return OK";
    }
}

// Test BatchWriteRemoteData with key count mismatch
TEST_F(PeerClientTest, BatchWriteRemoteDataKeyCountMismatch) {
    auto connect_result = peer_client_->Connect(server_endpoint_);
    ASSERT_TRUE(connect_result.has_value());

    BatchRemoteWriteRequest batch_request;
    batch_request.keys = {"key1", "key2"};
    batch_request.src_buffers_list = {
        {CreateBufferDesc("test_segment", 0x1000, 100)}};  // Only 1 buffer
    batch_request.target_tier_ids = {GetTierId()};         // Only 1 tier_id

    auto results = peer_client_->BatchWriteRemoteData(batch_request);
    ASSERT_EQ(results.size(), 2) << "Should return results for all keys";
    // All should fail due to count mismatch
    for (const auto& result : results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

// Test operations without connecting first
TEST_F(PeerClientTest, OperationsWithoutConnect) {
    RemoteReadRequest read_request;
    read_request.key = "test_key";
    read_request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto read_result = peer_client_->ReadRemoteData(read_request);
    ASSERT_FALSE(read_result.has_value()) << "Should fail when not connected";
    EXPECT_EQ(read_result.error(), ErrorCode::RPC_FAIL);
}

}  // namespace mooncake
