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

#include <string>
#include <variant>

#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

class CloudPluginConfigProviderTest : public testing::Test {
protected:
    void TearDown() override { ExecEnv::GetInstance()->set_storage_engine(nullptr); }

    void SetupRegularStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions()));
    }

    void SetupCloudStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<CloudStorageEngine>(EngineOptions()));
    }

    void TestConfigValidation(const std::string& bucket, const std::string& ak,
                              const std::string& sk, bool should_pass) {
        S3PluginDownloader::S3Config test_config("endpoint", "region", bucket, "prefix", ak, sk);

        bool is_valid = !test_config.bucket.empty() && !test_config.access_key.empty() &&
                        !test_config.secret_key.empty();
        EXPECT_EQ(is_valid, should_pass);

        if (!is_valid) {
            std::string error_msg = fmt::format(
                    "Incomplete S3 configuration: bucket={}, access_key={}, secret_key={}",
                    test_config.bucket, test_config.access_key.empty() ? "empty" : "***",
                    test_config.secret_key.empty() ? "empty" : "***");
            EXPECT_TRUE(error_msg.find("Incomplete S3 configuration") != std::string::npos);
        }
    }
};

// Test non-cloud environment
TEST_F(CloudPluginConfigProviderTest, TestNonCloudEnvironment) {
    SetupRegularStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

// Test cloud environment config failure
TEST_F(CloudPluginConfigProviderTest, TestCloudEnvironmentConfigFailure) {
    SetupCloudStorageEngine();

    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    EXPECT_FALSE(status.ok());
}

// Test config validation logic
TEST_F(CloudPluginConfigProviderTest, TestConfigValidation) {
    TestConfigValidation("bucket", "access_key", "secret_key", true);
    TestConfigValidation("", "access_key", "secret_key", false);
    TestConfigValidation("bucket", "", "secret_key", false);
    TestConfigValidation("bucket", "access_key", "", false);
    TestConfigValidation("", "", "", false);
}

// Test _get_default_storage_vault_info private method
TEST_F(CloudPluginConfigProviderTest, TestGetDefaultStorageVaultInfoNonCloud) {
    SetupRegularStorageEngine();

    S3PluginDownloader::S3Config config("", "", "", "", "", "");
    Status status = CloudPluginConfigProvider::_get_default_storage_vault_info(&config);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

// Test private method in cloud environment
TEST_F(CloudPluginConfigProviderTest, TestGetDefaultStorageVaultInfoCloud) {
    SetupCloudStorageEngine();

    S3PluginDownloader::S3Config config("", "", "", "", "", "");
    Status status = CloudPluginConfigProvider::_get_default_storage_vault_info(&config);

    EXPECT_FALSE(status.ok());
}

// Test S3Config construction
TEST_F(CloudPluginConfigProviderTest, TestS3ConfigConstruction) {
    auto config = std::make_unique<S3PluginDownloader::S3Config>(
            "endpoint", "region", "bucket", "prefix", "access_key", "secret_key");

    EXPECT_EQ(config->endpoint, "endpoint");
    EXPECT_EQ(config->region, "region");
    EXPECT_EQ(config->bucket, "bucket");
    EXPECT_EQ(config->prefix, "prefix");
    EXPECT_EQ(config->access_key, "access_key");
    EXPECT_EQ(config->secret_key, "secret_key");
}

} // namespace doris