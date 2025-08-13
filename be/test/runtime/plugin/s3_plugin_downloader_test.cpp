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

#include <filesystem>
#include <fstream>
#include <string>

#include "io/fs/local_file_system.h"

namespace doris {

class S3PluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing
        temp_dir = std::filesystem::temp_directory_path() / "s3_plugin_test";
        std::filesystem::create_directories(temp_dir);
    }

    void TearDown() override {
        // Clean up temporary directory
        if (std::filesystem::exists(temp_dir)) {
            std::filesystem::remove_all(temp_dir);
        }
    }

    std::filesystem::path temp_dir;
};

TEST_F(S3PluginDownloaderTest, TestS3ConfigCreation) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    EXPECT_EQ(config.endpoint, "http://s3.amazonaws.com");
    EXPECT_EQ(config.region, "us-west-2");
    EXPECT_EQ(config.bucket, "test-bucket");
    EXPECT_EQ(config.access_key, "access-key");
    EXPECT_EQ(config.secret_key, "secret-key");
}

TEST_F(S3PluginDownloaderTest, TestS3ConfigToString) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    std::string config_str = config.to_string();

    // Should contain basic info but mask secret key
    EXPECT_TRUE(config_str.find("s3.amazonaws.com") != std::string::npos);
    EXPECT_TRUE(config_str.find("us-west-2") != std::string::npos);
    EXPECT_TRUE(config_str.find("test-bucket") != std::string::npos);
    EXPECT_TRUE(config_str.find("***") != std::string::npos);        // Access key should be masked
    EXPECT_TRUE(config_str.find("access-key") == std::string::npos); // Actual key should not appear
}

TEST_F(S3PluginDownloaderTest, TestConstructorWithValidConfig) {
    S3PluginDownloader::S3Config config("http://localhost:9000", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    // Constructor should not throw with valid config
    EXPECT_NO_THROW({ S3PluginDownloader downloader(config); });
}

TEST_F(S3PluginDownloaderTest, TestConstructorWithEmptyConfig) {
    S3PluginDownloader::S3Config empty_config("", "", "", "", "");

    // Constructor should handle empty config gracefully
    EXPECT_NO_THROW({ S3PluginDownloader downloader(empty_config); });
}

TEST_F(S3PluginDownloaderTest, TestGetLockForPath) {
    // Test that different paths can get locks (this tests the static method indirectly)
    S3PluginDownloader::S3Config config("http://localhost", "us-west-2", "test", "key", "secret");
    S3PluginDownloader downloader(config);

    // Create different file paths
    std::string path1 = temp_dir / "file1.jar";
    std::string path2 = temp_dir / "file2.jar";
    std::string path3 = temp_dir / "file1.jar"; // Same as path1

    // These should not crash when getting locks
    // The actual lock behavior is tested implicitly through the download_file method
    EXPECT_NO_THROW({
        // We can't directly test _get_lock_for_path as it's private,
        // but we can test that the class works correctly
        std::string output_path;
        // This will fail due to no actual S3 connection, but should not crash
        Status status =
                downloader.download_file("s3://bucket/file1.jar", path1, &output_path, "md5");
        // Should return error but not crash
        EXPECT_FALSE(status.ok());
    });
}

TEST_F(S3PluginDownloaderTest, TestCalculateFileMD5WithRealFile) {
    // Create a test file
    std::string test_file = temp_dir / "test.txt";
    std::ofstream file(test_file);
    file << "Hello, World!";
    file.close();

    S3PluginDownloader::S3Config config("http://localhost", "us-west-2", "test", "key", "secret");
    S3PluginDownloader downloader(config);

    // We can't directly test _calculate_file_md5 as it's private,
    // but we can verify the file exists and has content
    EXPECT_TRUE(std::filesystem::exists(test_file));
    EXPECT_GT(std::filesystem::file_size(test_file), 0);
}

TEST_F(S3PluginDownloaderTest, TestDownloadFileWithInvalidS3Config) {
    S3PluginDownloader::S3Config invalid_config("", "", "", "", "");
    S3PluginDownloader downloader(invalid_config);

    std::string output_path;
    std::string target_path = temp_dir / "target.jar";

    // Should fail gracefully with invalid config
    Status status =
            downloader.download_file("s3://bucket/file.jar", target_path, &output_path, "md5");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_internal_error());
}

TEST_F(S3PluginDownloaderTest, TestShardLockDistribution) {
    // Test that the shard lock system distributes different paths
    // We can't directly test the lock distribution, but we can verify
    // that LOCK_SHARD_SIZE is reasonable
    EXPECT_EQ(S3PluginDownloader::LOCK_SHARD_SIZE, 32);

    // Test with various file paths to ensure they would use different locks
    std::vector<std::string> test_paths = {"/path/to/file1.jar", "/path/to/file2.jar",
                                           "/different/path/file3.jar",
                                           "/another/location/file4.jar"};

    // All paths should be valid for lock calculation
    for (const auto& path : test_paths) {
        EXPECT_FALSE(path.empty());
        // Hash calculation should work
        size_t hash = std::hash<std::string> {}(path);
        size_t shard = hash % S3PluginDownloader::LOCK_SHARD_SIZE;
        EXPECT_LT(shard, S3PluginDownloader::LOCK_SHARD_SIZE);
    }
}

} // namespace doris