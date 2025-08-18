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
        original_cluster_id_ = config::cluster_id;
        original_cloud_unique_id_ = config::cloud_unique_id;

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void TearDown() override {
        config::cluster_id = original_cluster_id_;
        config::cloud_unique_id = original_cloud_unique_id_;
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

TEST_F(CloudPluginConfigProviderTest, TestClusterIdPath) {
    SetClusterIdConfig(12345);
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("12345", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestCloudUniqueIdParsingSuccess) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("1:instanceABC:random");
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("instanceABC", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestCloudUniqueIdParsingFallback) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("onlyonepart");
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("onlyonepart", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestEmptyCloudUniqueId) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("");
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
}

TEST_F(CloudPluginConfigProviderTest, TestStringParsingLoop) {
    SetClusterIdConfig(-1);
    SetCloudUniqueIdConfig("1:2:3:4:5");
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("2", instance_id);
}

TEST_F(CloudPluginConfigProviderTest, TestS3ConfigNonCloudEnvironment) {
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
}

} // namespace doris