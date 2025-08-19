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
#include "runtime/exec_env.h"

namespace doris {

class CloudPluginDownloaderTest : public testing::Test {
    // Simple tests, most don't need ExecEnv setup
};

// Test unsupported plugin types
TEST_F(CloudPluginDownloaderTest, TestUnsupportedPluginTypes) {
    std::string local_path;

    // CONNECTORS type
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.jar", "/tmp/test.jar",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);

    // HADOOP_CONF type
    status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.xml", "/tmp/test.xml",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);

    // Invalid type
    CloudPluginDownloader::PluginType invalid_type =
            static_cast<CloudPluginDownloader::PluginType>(999);
    status = CloudPluginDownloader::download_from_cloud(invalid_type, "test.jar", "/tmp/test.jar",
                                                        &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
}

// Test empty plugin name
TEST_F(CloudPluginDownloaderTest, TestEmptyPluginName) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("plugin_name cannot be empty") != std::string::npos);
}

// Test S3 config retrieval failure
TEST_F(CloudPluginDownloaderTest, TestS3ConfigFailure) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "test.jar", "/tmp/test.jar",
            &local_path);

    EXPECT_FALSE(status.ok());
}

// Test _plugin_type_to_string method
TEST_F(CloudPluginDownloaderTest, TestPluginTypeToString) {
    EXPECT_EQ(CloudPluginDownloader::_plugin_type_to_string(
                      CloudPluginDownloader::PluginType::JDBC_DRIVERS),
              "jdbc_drivers");

    EXPECT_EQ(CloudPluginDownloader::_plugin_type_to_string(
                      CloudPluginDownloader::PluginType::JAVA_UDF),
              "java_udf");
}

} // namespace doris