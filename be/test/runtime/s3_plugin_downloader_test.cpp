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

#include "runtime/s3_plugin_downloader.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace doris {

class S3PluginDownloaderTest : public ::testing::Test {
public:
    S3PluginDownloaderTest()
            : s3_config_("s3.test-endpoint.com", "us-west-2", "test-bucket", "ak", "sk") {}

    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "s3_plugin_test";
        std::filesystem::create_directories(test_dir_);
        local_path_ = (test_dir_ / "test-plugin.jar").string();
    }

    void TearDown() override {
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

protected:
    std::filesystem::path test_dir_;
    S3PluginDownloader::S3Config s3_config_;
    std::string local_path_;
};

TEST_F(S3PluginDownloaderTest, TestS3ConfigCreation) {
    EXPECT_EQ(s3_config_.endpoint, "s3.test-endpoint.com");
    EXPECT_EQ(s3_config_.region, "us-west-2");
    EXPECT_EQ(s3_config_.bucket, "test-bucket");
    EXPECT_EQ(s3_config_.access_key, "ak");
    EXPECT_EQ(s3_config_.secret_key, "sk");
}

TEST_F(S3PluginDownloaderTest, TestS3ConfigToString) {
    std::string expected_str =
            "S3Config{endpoint='s3.test-endpoint.com', region='us-west-2', "
            "bucket='test-bucket', access_key='***'}";
    EXPECT_EQ(s3_config_.to_string(), expected_str);

    S3PluginDownloader::S3Config config_no_ak("s3.test-endpoint.com", "us-west-2", "test-bucket",
                                              "", "sk");
    std::string expected_str_no_ak =
            "S3Config{endpoint='s3.test-endpoint.com', region='us-west-2', "
            "bucket='test-bucket', access_key='null'}";
    EXPECT_EQ(config_no_ak.to_string(), expected_str_no_ak);
}

} // namespace doris