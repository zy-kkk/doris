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

#include "runtime/plugin/s3_plugin_downloader.h"

#include <gtest/gtest.h>

#include <string>

#include "common/status.h"

namespace doris {

TEST(S3PluginDownloaderTest, TestS3ConfigCreation) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    EXPECT_EQ("http://s3.amazonaws.com", config.endpoint);
    EXPECT_EQ("us-west-2", config.region);
    EXPECT_EQ("test-bucket", config.bucket);
    EXPECT_EQ("access-key", config.access_key);
    EXPECT_EQ("secret-key", config.secret_key);
}

TEST(S3PluginDownloaderTest, TestS3ConfigToString) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    std::string config_str = config.to_string();

    // Should contain basic info but mask secret info
    EXPECT_TRUE(config_str.find("s3.amazonaws.com") != std::string::npos);
    EXPECT_TRUE(config_str.find("us-west-2") != std::string::npos);
    EXPECT_TRUE(config_str.find("test-bucket") != std::string::npos);
    EXPECT_TRUE(config_str.find("***") != std::string::npos ||
                config_str.find("null") !=
                        std::string::npos); // Access key should be masked or null
    EXPECT_FALSE(config_str.find("access-key") !=
                 std::string::npos); // Actual key should not appear
}

TEST(S3PluginDownloaderTest, TestS3ConfigWithEmptyValues) {
    S3PluginDownloader::S3Config empty_config("", "", "", "", "");

    EXPECT_EQ("", empty_config.endpoint);
    EXPECT_EQ("", empty_config.region);
    EXPECT_EQ("", empty_config.bucket);
    EXPECT_EQ("", empty_config.access_key);
    EXPECT_EQ("", empty_config.secret_key);

    // to_string should still work with empty values
    std::string config_str = empty_config.to_string();
    EXPECT_FALSE(config_str.empty());
}

TEST(S3PluginDownloaderTest, TestConstructorWithValidConfig) {
    // Test that constructor doesn't throw with valid config
    S3PluginDownloader::S3Config config("http://localhost:9000", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(config); });
}

TEST(S3PluginDownloaderTest, TestConstructorWithEmptyConfig) {
    // Test that constructor handles empty config gracefully
    S3PluginDownloader::S3Config empty_config("", "", "", "", "");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(empty_config); });
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithInvalidS3Path) {
    S3PluginDownloader::S3Config config("http://localhost", "us-west-2", "test-bucket", "key",
                                        "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    std::string target_path = "/tmp/test.jar";

    // Test with invalid S3 path - should fail but not crash
    Status status = downloader.download_file("invalid-s3-path", target_path, &local_path);

    EXPECT_FALSE(status.ok());
    // Should not crash and return meaningful error
    EXPECT_FALSE(status.to_string().empty());
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithNullPointer) {
    S3PluginDownloader::S3Config config("http://localhost", "us-west-2", "test-bucket", "key",
                                        "secret");
    S3PluginDownloader downloader(config);

    std::string target_path = "/tmp/test.jar";

    // Test with null pointer for local_path - should handle gracefully
    Status status = downloader.download_file("s3://bucket/file.jar", target_path, nullptr);

    EXPECT_FALSE(status.ok());
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithInvalidLocalPath) {
    S3PluginDownloader::S3Config config("http://localhost", "us-west-2", "test-bucket", "key",
                                        "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;

    // Test with invalid local path (directory that doesn't exist and can't be created)
    Status status = downloader.download_file("s3://bucket/file.jar",
                                             "/root/nonexistent/path/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().empty());
}

TEST(S3PluginDownloaderTest, TestS3ConfigCopyConstruction) {
    S3PluginDownloader::S3Config original("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                          "access-key", "secret-key");

    // Test copy construction
    S3PluginDownloader::S3Config copied = original;

    EXPECT_EQ(original.endpoint, copied.endpoint);
    EXPECT_EQ(original.region, copied.region);
    EXPECT_EQ(original.bucket, copied.bucket);
    EXPECT_EQ(original.access_key, copied.access_key);
    EXPECT_EQ(original.secret_key, copied.secret_key);
}

TEST(S3PluginDownloaderTest, TestS3ConfigDefaultValues) {
    // Test S3Config with minimal values
    S3PluginDownloader::S3Config minimal_config("endpoint", "", "bucket", "", "");

    EXPECT_EQ("endpoint", minimal_config.endpoint);
    EXPECT_EQ("", minimal_config.region);
    EXPECT_EQ("bucket", minimal_config.bucket);
    EXPECT_EQ("", minimal_config.access_key);
    EXPECT_EQ("", minimal_config.secret_key);

    // to_string should still work
    EXPECT_FALSE(minimal_config.to_string().empty());
}

TEST(S3PluginDownloaderTest, TestMultipleDownloaderInstances) {
    // Test that multiple downloaders can be created with different configs
    S3PluginDownloader::S3Config config1("endpoint1", "region1", "bucket1", "key1", "secret1");
    S3PluginDownloader::S3Config config2("endpoint2", "region2", "bucket2", "key2", "secret2");

    EXPECT_NO_THROW({
        S3PluginDownloader downloader1(config1);
        S3PluginDownloader downloader2(config2);
    });
}

TEST(S3PluginDownloaderTest, TestS3ConfigToStringMasking) {
    // Test different access key scenarios for masking
    S3PluginDownloader::S3Config config_with_key("http://s3.test", "region", "bucket", "mykey",
                                                 "mysecret");
    S3PluginDownloader::S3Config config_empty_key("http://s3.test", "region", "bucket", "",
                                                  "mysecret");

    std::string str_with_key = config_with_key.to_string();
    std::string str_empty_key = config_empty_key.to_string();

    // With key should mask
    EXPECT_TRUE(str_with_key.find("***") != std::string::npos);
    EXPECT_FALSE(str_with_key.find("mykey") != std::string::npos);

    // Empty key should show null
    EXPECT_TRUE(str_empty_key.find("null") != std::string::npos);
}

TEST(S3PluginDownloaderTest, TestStatusTypeIntegration) {
    // Test that Status integrates properly with S3PluginDownloader
    S3PluginDownloader::S3Config config("invalid", "invalid", "invalid", "invalid", "invalid");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("invalid", "/tmp/test.jar", &local_path);

    // Test Status methods work correctly
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().empty());
}

} // namespace doris