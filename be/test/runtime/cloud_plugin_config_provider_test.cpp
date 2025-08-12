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

#include "runtime/cloud_plugin_config_provider.h"

#include <gtest/gtest.h>

#include <string>

#include "cloud/config.h"
#include "common/config.h"

namespace doris {

class CloudPluginConfigProviderTest : public ::testing::Test {
public:
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

TEST_F(CloudPluginConfigProviderTest, TestBuildPluginPathWithClusterId) {
    config::cluster_id = 12345;
    config::cloud_unique_id = "should-be-ignored";
    std::string result = CloudPluginConfigProvider::build_plugin_path("jdbc_drivers", "mysql.jar");
    EXPECT_EQ(result, "12345/plugins/jdbc_drivers/mysql.jar");
}

TEST_F(CloudPluginConfigProviderTest, TestBuildPluginPathWithDefaultInstanceId) {
    config::cluster_id = -1;
    config::cloud_unique_id = "";
    std::string result = CloudPluginConfigProvider::build_plugin_path("jdbc_drivers", "mysql.jar");
    EXPECT_EQ(result, "default/plugins/jdbc_drivers/mysql.jar");
}

TEST_F(CloudPluginConfigProviderTest, TestBuildPluginPathWithParsedCloudUniqueId) {
    config::cluster_id = -1;
    config::cloud_unique_id = "1:my-instance-id:some-random-string";
    std::string result = CloudPluginConfigProvider::build_plugin_path("java_udf", "my-udf.jar");
    EXPECT_EQ(result, "my-instance-id/plugins/java_udf/my-udf.jar");
}

TEST_F(CloudPluginConfigProviderTest, TestBuildPluginPathWithUnparsableCloudUniqueId) {
    config::cluster_id = -1;
    config::cloud_unique_id = "unparsable-id";
    std::string result = CloudPluginConfigProvider::build_plugin_path("connectors", "");
    EXPECT_EQ(result, "unparsable-id/plugins/connectors");
}

TEST_F(CloudPluginConfigProviderTest, TestBuildPluginPathWithEmptyPluginName) {
    config::cluster_id = -1;
    config::cloud_unique_id = "1:my-instance-id:some-random-string";
    std::string result = CloudPluginConfigProvider::build_plugin_path("hadoop_conf", "");
    EXPECT_EQ(result, "my-instance-id/plugins/hadoop_conf");
}

} // namespace doris