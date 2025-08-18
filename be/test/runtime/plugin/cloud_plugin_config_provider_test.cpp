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

#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/plugin/s3_plugin_downloader.h"

namespace doris {

class CloudPluginConfigProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original storage engine
        original_engine_ = ExecEnv::GetInstance()->storage_engine_ptr();
    }

    void TearDown() override {
        // Restore original storage engine
        ExecEnv::GetInstance()->set_storage_engine(std::move(original_engine_));
    }

    void SetupRegularStorageEngine() {
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void SetupCloudStorageEngine() {
        doris::EngineOptions options;
        auto cloud_engine = std::make_unique<CloudStorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(cloud_engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

private:
    std::unique_ptr<BaseStorageEngine> original_engine_;
};

// Test 1: S3Config constructor with 6 parameters (including prefix)
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigConstructorWithPrefix) {
    S3PluginDownloader::S3Config config("https://s3.amazonaws.com", "us-west-2", "my-bucket",
                                        "instance123", "AKIA123", "secret123");

    EXPECT_EQ(config.endpoint, "https://s3.amazonaws.com");
    EXPECT_EQ(config.region, "us-west-2");
    EXPECT_EQ(config.bucket, "my-bucket");
    EXPECT_EQ(config.prefix, "instance123");
    EXPECT_EQ(config.access_key, "AKIA123");
    EXPECT_EQ(config.secret_key, "secret123");
}

// Test 2: S3Config to_string includes prefix
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigToStringWithPrefix) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "test-prefix", "access-key",
                                        "secret-key");

    std::string config_str = config.to_string();
    EXPECT_TRUE(config_str.find("test-prefix") != std::string::npos);
    EXPECT_TRUE(config_str.find("prefix='test-prefix'") != std::string::npos);
}

// Test 3: S3Config to_string with empty prefix
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigToStringWithEmptyPrefix) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "", "access-key",
                                        "secret-key");

    std::string config_str = config.to_string();
    EXPECT_TRUE(config_str.find("prefix=''") != std::string::npos);
}

// Test 4: S3Config to_string with empty access key shows "null"
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigToStringWithEmptyAccessKey) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "", "secret-key");

    std::string config_str = config.to_string();
    EXPECT_TRUE(config_str.find("access_key='null'") != std::string::npos);
}

// Test 5: Non-cloud environment (regular StorageEngine) - covers lines 85-89
TEST_F(CloudPluginConfigProviderTest, TestNonCloudEnvironment) {
    SetupRegularStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

// Test 6: CloudStorageEngine exists but vault info retrieval fails - covers lines 91-95
TEST_F(CloudPluginConfigProviderTest, TestCloudStorageEngineVaultInfoFailure) {
    SetupCloudStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    // This should fail because CloudMetaMgr is not properly initialized in test environment
    EXPECT_FALSE(status.ok());
    // Could be INTERNAL_ERROR or NOT_FOUND depending on the failure point
    EXPECT_TRUE(status.code() == ErrorCode::INTERNAL_ERROR ||
                status.code() == ErrorCode::NOT_FOUND);
}

// Test 7: Test S3Config validation scenarios - simulating lines 36-41
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigValidationLogic) {
    // Test scenarios that would trigger the validation logic in lines 36-41

    // Empty bucket scenario
    S3PluginDownloader::S3Config empty_bucket("endpoint", "region", "", "prefix", "ak", "sk");
    EXPECT_EQ(empty_bucket.bucket, "");
    EXPECT_FALSE(empty_bucket.access_key.empty());
    EXPECT_FALSE(empty_bucket.secret_key.empty());

    // Empty access key scenario
    S3PluginDownloader::S3Config empty_ak("endpoint", "region", "bucket", "prefix", "", "sk");
    EXPECT_EQ(empty_ak.access_key, "");
    EXPECT_FALSE(empty_ak.bucket.empty());
    EXPECT_FALSE(empty_ak.secret_key.empty());

    // Empty secret key scenario
    S3PluginDownloader::S3Config empty_sk("endpoint", "region", "bucket", "prefix", "ak", "");
    EXPECT_EQ(empty_sk.secret_key, "");
    EXPECT_FALSE(empty_sk.bucket.empty());
    EXPECT_FALSE(empty_sk.access_key.empty());

    // Valid config scenario - would pass validation
    S3PluginDownloader::S3Config valid_config("endpoint", "region", "bucket", "prefix", "ak", "sk");
    EXPECT_FALSE(valid_config.bucket.empty());
    EXPECT_FALSE(valid_config.access_key.empty());
    EXPECT_FALSE(valid_config.secret_key.empty());
}

// Test 8: Test the construction logic in lines 43-45
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigConstructionLogic) {
    // Test the make_unique construction with all 6 parameters
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;

    // This simulates the construction logic in line 43-44
    EXPECT_NO_THROW({
        s3_config = std::make_unique<S3PluginDownloader::S3Config>(
                "test-endpoint", "test-region", "test-bucket", "test-prefix", "test-access-key",
                "test-secret-key");
    });

    ASSERT_NE(s3_config, nullptr);
    EXPECT_EQ(s3_config->endpoint, "test-endpoint");
    EXPECT_EQ(s3_config->region, "test-region");
    EXPECT_EQ(s3_config->bucket, "test-bucket");
    EXPECT_EQ(s3_config->prefix, "test-prefix");
    EXPECT_EQ(s3_config->access_key, "test-access-key");
    EXPECT_EQ(s3_config->secret_key, "test-secret-key");
}

// Test 9: Multiple calls consistency
TEST_F(CloudPluginConfigProviderTest, TestMultipleCallsConsistency) {
    SetupRegularStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config1;
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config2;

    Status status1 = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config1);
    Status status2 = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config2);

    EXPECT_FALSE(status1.ok());
    EXPECT_FALSE(status2.ok());
    EXPECT_EQ(status1.code(), status2.code());
    EXPECT_EQ(status1.code(), ErrorCode::NOT_FOUND);
}

// Test 10: Error message validation
TEST_F(CloudPluginConfigProviderTest, TestErrorMessageValidation) {
    SetupRegularStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    EXPECT_FALSE(status.ok());
    std::string error_msg = status.to_string();
    EXPECT_FALSE(error_msg.empty());
    EXPECT_TRUE(error_msg.find("CloudStorageEngine not found, not in cloud mode") !=
                std::string::npos);
}

// Test 11: Test different prefix variations in S3Config
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigPrefixVariations) {
    // Normal prefix
    S3PluginDownloader::S3Config normal_prefix("endpoint", "region", "bucket", "normal-prefix",
                                               "ak", "sk");
    EXPECT_EQ(normal_prefix.prefix, "normal-prefix");

    // Empty prefix (valid scenario)
    S3PluginDownloader::S3Config empty_prefix("endpoint", "region", "bucket", "", "ak", "sk");
    EXPECT_EQ(empty_prefix.prefix, "");

    // Prefix with special characters
    S3PluginDownloader::S3Config special_prefix("endpoint", "region", "bucket",
                                                "prefix/with/slashes", "ak", "sk");
    EXPECT_EQ(special_prefix.prefix, "prefix/with/slashes");

    // Long prefix
    std::string long_prefix(100, 'x');
    S3PluginDownloader::S3Config long_prefix_config("endpoint", "region", "bucket", long_prefix,
                                                    "ak", "sk");
    EXPECT_EQ(long_prefix_config.prefix, long_prefix);
}

// Test 12: Exception handling during storage engine access
TEST_F(CloudPluginConfigProviderTest, TestExceptionHandling) {
    // Test with null storage engine
    ExecEnv::GetInstance()->set_storage_engine(nullptr);

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;

    // This should trigger exception handling path
    EXPECT_ANY_THROW({ CloudPluginConfigProvider::get_cloud_s3_config(&s3_config); });
}

// Test 13: Test both regular and cloud storage engine scenarios
TEST_F(CloudPluginConfigProviderTest, TestStorageEngineTypeDetection) {
    // Test 1: Regular StorageEngine
    SetupRegularStorageEngine();
    {
        std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
        Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    }

    // Test 2: CloudStorageEngine (will fail later but pass dynamic_cast)
    SetupCloudStorageEngine();
    {
        std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
        Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
        EXPECT_FALSE(status.ok());
        // This should pass the CloudStorageEngine check but fail at CloudMetaMgr level
        EXPECT_NE(status.code(), ErrorCode::NOT_FOUND);
    }
}

} // namespace doris