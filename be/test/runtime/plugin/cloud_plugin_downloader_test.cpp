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

#include "runtime/plugin/cloud_plugin_downloader.h"

#include <gtest/gtest.h>

#include <string>

#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize a minimal StorageEngine to avoid BaseStorageEngine nullptr
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void TearDown() override {
        // Cleanup storage engine
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }
};

TEST_F(CloudPluginDownloaderTest, TestUnsupportedPluginTypes) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.jar", "/tmp/test.jar",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestEmptyPluginName) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("cannot be empty") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestSupportedPluginTypes) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestPluginTypeToString) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.xml", "/tmp/test.xml",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("hadoop_conf") != std::string::npos);
}

} // namespace doris