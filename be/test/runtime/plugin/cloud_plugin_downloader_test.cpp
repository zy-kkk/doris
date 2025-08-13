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

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CloudPluginDownloaderTest, TestPluginTypeEnum) {
    // Test that all PluginType enum values exist
    CloudPluginDownloader::PluginType type1 = CloudPluginDownloader::PluginType::JDBC_DRIVERS;
    CloudPluginDownloader::PluginType type2 = CloudPluginDownloader::PluginType::JAVA_UDF;
    CloudPluginDownloader::PluginType type3 = CloudPluginDownloader::PluginType::CONNECTORS;
    CloudPluginDownloader::PluginType type4 = CloudPluginDownloader::PluginType::HADOOP_CONF;

    // Basic enum comparison
    EXPECT_NE(type1, type2);
    EXPECT_NE(type2, type3);
    EXPECT_NE(type3, type4);
}

TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudParameterValidation) {
    std::string downloaded_path;

    // Test with empty plugin name
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "", "/local/path", &downloaded_path,
            "md5hash");
    EXPECT_FALSE(status1.ok());
    EXPECT_TRUE(status1.is_invalid_argument());

    // Test with null output parameter
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/local/path", nullptr,
            "md5hash");
    EXPECT_FALSE(status2.ok());
}

TEST_F(CloudPluginDownloaderTest, TestUnsupportedPluginTypes) {
    std::string downloaded_path;

    // Test with unsupported plugin types - should return InvalidArgument
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.jar", "/tmp/test.jar",
            &downloaded_path, "md5hash");
    EXPECT_FALSE(status1.ok());
    EXPECT_TRUE(status1.is_invalid_argument());

    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.conf", "/tmp/test.conf",
            &downloaded_path, "md5hash");
    EXPECT_FALSE(status2.ok());
    EXPECT_TRUE(status2.is_invalid_argument());
}

TEST_F(CloudPluginDownloaderTest, TestSupportedPluginTypesWithoutCloudConfig) {
    std::string downloaded_path;

    // Test with supported plugin types but no cloud config
    // Should fail at cloud config retrieval stage, not parameter validation
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "valid.jar", "/tmp/valid.jar",
            &downloaded_path, "md5hash");
    EXPECT_FALSE(status1.ok());
    // Should fail due to cloud config issues, not parameter validation
    EXPECT_FALSE(status1.is_invalid_argument());

    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "driver.jar", "/tmp/driver.jar",
            &downloaded_path, "md5hash");
    EXPECT_FALSE(status2.ok());
    // Should fail due to cloud config issues, not parameter validation
    EXPECT_FALSE(status2.is_invalid_argument());
}

} // namespace doris