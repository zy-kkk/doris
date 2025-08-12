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

#include "runtime/cloud_plugin_downloader.h"

#include <gtest/gtest.h>

#include <string>

#include "cloud/config.h"
#include "common/config.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _original_cluster_id = config::cluster_id;
        _original_cloud_unique_id = config::cloud_unique_id;
    }

    void TearDown() override {
        config::cluster_id = _original_cluster_id;
        config::cloud_unique_id = _original_cloud_unique_id;
    }

private:
    int32_t _original_cluster_id;
    std::string _original_cloud_unique_id;
};

TEST_F(CloudPluginDownloaderTest, TestPluginTypeToString) {
    using PluginType = CloudPluginDownloader::PluginType;
    EXPECT_EQ(CloudPluginDownloader::plugin_type_to_string(PluginType::JDBC_DRIVERS),
              "jdbc_drivers");
    EXPECT_EQ(CloudPluginDownloader::plugin_type_to_string(PluginType::JAVA_UDF), "java_udf");
    EXPECT_EQ(CloudPluginDownloader::plugin_type_to_string(PluginType::CONNECTORS), "connectors");
    EXPECT_EQ(CloudPluginDownloader::plugin_type_to_string(PluginType::HADOOP_CONF), "hadoop_conf");
    EXPECT_EQ(CloudPluginDownloader::plugin_type_to_string(static_cast<PluginType>(99)), "unknown");
}

TEST_F(CloudPluginDownloaderTest, TestBuildS3PathForFile) {
    config::cluster_id = -1;
    config::cloud_unique_id = "1:my-instance:rand";
    S3PluginDownloader::S3Config s3_config {"", "", "my-bucket", "", ""};

    std::string result = CloudPluginDownloader::build_s3_path(
            s3_config, CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql.jar");
    EXPECT_EQ(result, "s3://my-bucket/my-instance/plugins/jdbc_drivers/mysql.jar");
}

TEST_F(CloudPluginDownloaderTest, TestBuildS3PathForDirectory) {
    config::cluster_id = 12345;
    S3PluginDownloader::S3Config s3_config {"", "", "another-bucket", "", ""};

    std::string result = CloudPluginDownloader::build_s3_path(
            s3_config, CloudPluginDownloader::PluginType::CONNECTORS, "");
    EXPECT_EQ(result, "s3://another-bucket/12345/plugins/connectors/");
}

TEST_F(CloudPluginDownloaderTest, TestBuildS3PathWithEmptyInstanceId) {
    config::cluster_id = -1;
    config::cloud_unique_id = "";
    S3PluginDownloader::S3Config s3_config {"", "", "default-bucket", "", ""};

    std::string result = CloudPluginDownloader::build_s3_path(
            s3_config, CloudPluginDownloader::PluginType::HADOOP_CONF, "core-site.xml");
    EXPECT_EQ(result, "s3://default-bucket/default/plugins/hadoop_conf/core-site.xml");
}

} // namespace doris