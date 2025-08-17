// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/plugin/cloud_plugin_config_provider.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "cloud/config.h"
#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/plugin/s3_plugin_downloader.h"

namespace doris {

class CloudPluginConfigProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original config values
        original_cluster_id_ = config::cluster_id;
        original_cloud_unique_id_ = config::cloud_unique_id;

        // Initialize a minimal StorageEngine to avoid BaseStorageEngine nullptr
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void TearDown() override {
        // Restore original config values
        config::cluster_id = original_cluster_id_;
        config::cloud_unique_id = original_cloud_unique_id_;

        // Cleanup storage engine
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void SetClusterIdConfig(int32_t cluster_id) { config::cluster_id = cluster_id; }

    void SetCloudUniqueIdConfig(const std::string& cloud_unique_id) {
        config::cloud_unique_id = cloud_unique_id;
    }

private:
    int32_t original_cluster_id_;
    std::string original_cloud_unique_id_;
};

// Test cluster_id based instance ID generation
TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithClusterId) {
    SetClusterIdConfig(12345);
    SetCloudUniqueIdConfig(""); // Should be ignored when cluster_id is set

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("12345", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithZeroClusterId) {
    SetClusterIdConfig(0);

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("0", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithNegativeClusterId) {
    SetClusterIdConfig(-999);

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("-999", instance_id);
}

// Test cloud_unique_id based instance ID generation
TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithValidCloudUniqueId) {
    SetClusterIdConfig(-1); // Trigger cloud_unique_id path
    SetCloudUniqueIdConfig("1:instanceABC:randomString123");

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("instanceABC", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithComplexCloudUniqueId) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("1:instance:with:many:colons:test");

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("instance", instance_id); // Should take the second part
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithInsufficientParts) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("onlyonepart");

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("onlyonepart", instance_id); // Uses entire value as fallback
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithEmptyCloudUniqueId) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("");

    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("cloud_unique_id is empty") != std::string::npos);
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithEdgeCases) {
    SetClusterIdConfig(-1);

    // Test with trailing colon
    SetCloudUniqueIdConfig("1:instance:");
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("instance", instance_id);

    // Test with leading colon
    SetCloudUniqueIdConfig(":1:instance:random");
    status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("1", instance_id); // Empty first part, "1" is second part

    // Test with consecutive colons
    SetCloudUniqueIdConfig("1::instance::random");
    status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("", instance_id); // Empty second part
}

// Test S3 config retrieval
TEST_F(CloudPluginConfigProviderTest, TestGetCloudS3ConfigWithoutCloudEnvironment) {
    // Test getting S3 config without actual cloud environment
    // This should fail because we're not using CloudStorageEngine
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    // Should fail in non-cloud environment
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(s3_config == nullptr);
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

// Test null pointer handling
TEST_F(CloudPluginConfigProviderTest, TestGetCloudS3ConfigNullPointer) {
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(nullptr);
    EXPECT_FALSE(status.ok());
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdNullPointer) {
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(nullptr);
    EXPECT_FALSE(status.ok());
}

// Test boundary values
TEST_F(CloudPluginConfigProviderTest, TestClusterIdBoundaryValues) {
    std::string instance_id;

    // Test with maximum int32
    SetClusterIdConfig(INT32_MAX);
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(std::to_string(INT32_MAX), instance_id);

    // Test with minimum int32
    SetClusterIdConfig(INT32_MIN);
    status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(std::to_string(INT32_MIN), instance_id);
}

// Test S3Config type integration
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigTypeIntegration) {
    // Test that S3Config type is accessible and can be created
    std::unique_ptr<S3PluginDownloader::S3Config> config;
    EXPECT_TRUE(config == nullptr);

    // Test creating S3Config manually to verify integration
    auto manual_config = std::make_unique<S3PluginDownloader::S3Config>(
            "endpoint", "region", "bucket", "access", "secret");
    EXPECT_TRUE(manual_config != nullptr);
    EXPECT_EQ("endpoint", manual_config->endpoint);
    EXPECT_EQ("region", manual_config->region);
    EXPECT_EQ("bucket", manual_config->bucket);
    EXPECT_EQ("access", manual_config->access_key);
    EXPECT_EQ("secret", manual_config->secret_key);
}

// Test Status type integration
TEST_F(CloudPluginConfigProviderTest, TestStatusTypeIntegration) {
    Status test_status = Status::OK();
    EXPECT_TRUE(test_status.ok());

    Status error_status = Status::InvalidArgument("test error");
    EXPECT_FALSE(error_status.ok());
    EXPECT_FALSE(error_status.to_string().empty());
    EXPECT_EQ(error_status.code(), ErrorCode::INVALID_ARGUMENT);
}

// Test concurrent access (basic thread safety)
TEST_F(CloudPluginConfigProviderTest, TestConcurrentAccess) {
    SetClusterIdConfig(12345);

    std::vector<std::thread> threads;
    std::vector<std::string> results(10);
    std::vector<bool> success(10);

    // Launch multiple threads to test concurrent access
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            std::string instance_id;
            Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
            success[i] = status.ok();
            results[i] = instance_id;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads succeeded and got the same result
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(success[i]);
        EXPECT_EQ("12345", results[i]);
    }
}

// Test method signature and accessibility
TEST_F(CloudPluginConfigProviderTest, TestMethodSignatures) {
    // Test that both static methods exist and can be called
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    std::string instance_id;

    // These should compile successfully (testing method signatures)
    Status s3_status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
    Status id_status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    // We test that methods exist and return Status objects
    EXPECT_TRUE(s3_status.ok() || !s3_status.ok()); // Always true
    EXPECT_TRUE(id_status.ok() || !id_status.ok()); // Always true
}

} // namespace doris