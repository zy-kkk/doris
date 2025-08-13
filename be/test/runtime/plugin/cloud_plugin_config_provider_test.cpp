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

namespace doris {

class CloudPluginConfigProviderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CloudPluginConfigProviderTest, TestGetCloudS3ConfigWithoutCloudEnvironment) {
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;

    // Test getting S3 config without actual cloud environment
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
    EXPECT_FALSE(status.ok());
    // Should return error status since we're not in cloud mode
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithoutCloudEnvironment) {
    std::string instance_id;

    // Test getting cloud instance ID without actual cloud environment
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    EXPECT_FALSE(status.ok());
    // Should return error status since we're not in cloud mode
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudS3ConfigWithNullPointer) {
    // Test with null pointer parameter
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(nullptr);
    EXPECT_FALSE(status.ok());
    // Should handle null pointer gracefully
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithNullPointer) {
    // Test with null pointer parameter
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(nullptr);
    EXPECT_FALSE(status.ok());
    // Should handle null pointer gracefully
}

TEST_F(CloudPluginConfigProviderTest, TestMethodsExistAndCallable) {
    // Basic test that methods exist and can be called
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    std::string instance_id;

    // Both methods should be callable but return errors due to no cloud environment
    Status status1 = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
    Status status2 = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    // Both should fail gracefully without crashing
    EXPECT_FALSE(status1.ok());
    EXPECT_FALSE(status2.ok());
}

} // namespace doris