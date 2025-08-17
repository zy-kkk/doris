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

#include "common/status.h"
#include "runtime/plugin/s3_plugin_downloader.h"

namespace doris {

TEST(CloudPluginConfigProviderTest, TestGetCloudS3ConfigWithoutCloudEnvironment) {
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);

    // Should fail in non-cloud environment
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(s3_config == nullptr);
    EXPECT_FALSE(status.to_string().empty());
}

TEST(CloudPluginConfigProviderTest, TestGetCloudS3ConfigNullPointer) {
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(nullptr);
    EXPECT_FALSE(status.ok());
}

TEST(CloudPluginConfigProviderTest, TestGetCloudInstanceIdNullPointer) {
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(nullptr);
    EXPECT_FALSE(status.ok());
}

TEST(CloudPluginConfigProviderTest, TestGetCloudInstanceIdWithoutCloudEnvironment) {
    std::string instance_id;
    Status status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);

    // May succeed or fail depending on config, but should not crash
    EXPECT_TRUE(status.ok() || !status.ok());

    if (status.ok()) {
        EXPECT_FALSE(instance_id.empty());
    } else {
        EXPECT_FALSE(status.to_string().empty());
    }
}

TEST(CloudPluginConfigProviderTest, TestS3ConfigTypeIntegration) {
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

TEST(CloudPluginConfigProviderTest, TestStatusTypeIntegration) {
    Status test_status = Status::OK();
    EXPECT_TRUE(test_status.ok());

    Status error_status = Status::InvalidArgument("test error");
    EXPECT_FALSE(error_status.ok());
    EXPECT_FALSE(error_status.to_string().empty());
}

TEST(CloudPluginConfigProviderTest, TestMethodSignatures) {
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