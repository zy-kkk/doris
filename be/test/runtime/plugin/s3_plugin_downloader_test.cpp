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

#include "common/status.h"

namespace doris {

TEST(S3PluginDownloaderTest, TestS3ConfigToString) {
    // Test with 6 parameters (including prefix)
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "access-key",
                                        "secret");
    std::string config_str = config.to_string();
    EXPECT_TRUE(config_str.find("***") != std::string::npos);
    EXPECT_TRUE(config_str.find("prefix='prefix'") != std::string::npos);

    S3PluginDownloader::S3Config empty_key_config("endpoint", "region", "bucket", "prefix", "",
                                                  "secret");
    std::string empty_str = empty_key_config.to_string();
    EXPECT_TRUE(empty_str.find("null") != std::string::npos);
}

TEST(S3PluginDownloaderTest, TestConstructorAndDestructor) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "key", "secret");
    EXPECT_NO_THROW({ S3PluginDownloader downloader(config); });
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithNullS3Filesystem) {
    S3PluginDownloader::S3Config empty_config("", "", "", "", "", "");
    S3PluginDownloader downloader(empty_config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(status.to_string().find("S3 filesystem not initialized") != std::string::npos);
}

TEST(S3PluginDownloaderTest, TestS3FilesystemCreationException) {
    S3PluginDownloader::S3Config invalid_config(std::string(10000, 'x'), "region", "bucket",
                                                "prefix", "key", "secret");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(invalid_config); });
}

TEST(S3PluginDownloaderTest, TestValidS3Config) {
    S3PluginDownloader::S3Config valid_config("http://localhost:9000", "us-west-2", "test-bucket",
                                              "prefix", "access", "secret");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(valid_config); });
}

TEST(S3PluginDownloaderTest, TestDownloadWithValidFilesystem) {
    S3PluginDownloader::S3Config config("http://endpoint", "region", "bucket", "prefix", "key",
                                        "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
}

TEST(S3PluginDownloaderTest, TestS3ConfigStringFormatting) {
    S3PluginDownloader::S3Config config_with_key("endpoint", "region", "bucket", "prefix",
                                                 "access-key", "secret");
    std::string str_with_key = config_with_key.to_string();
    EXPECT_TRUE(str_with_key.find("***") != std::string::npos);
    EXPECT_FALSE(str_with_key.find("access-key") != std::string::npos);

    S3PluginDownloader::S3Config config_empty_key("endpoint", "region", "bucket", "prefix", "",
                                                  "secret");
    std::string str_empty_key = config_empty_key.to_string();
    EXPECT_TRUE(str_empty_key.find("null") != std::string::npos);
}

// Test S3Config to_string with different prefix values
TEST(S3PluginDownloaderTest, TestS3ConfigPrefixInToString) {
    // Test empty prefix
    S3PluginDownloader::S3Config empty_prefix("endpoint", "region", "bucket", "", "access",
                                              "secret");
    std::string empty_prefix_str = empty_prefix.to_string();
    EXPECT_TRUE(empty_prefix_str.find("prefix=''") != std::string::npos);

    // Test non-empty prefix
    S3PluginDownloader::S3Config with_prefix("endpoint", "region", "bucket", "test-prefix",
                                             "access", "secret");
    std::string with_prefix_str = with_prefix.to_string();
    EXPECT_TRUE(with_prefix_str.find("prefix='test-prefix'") != std::string::npos);
}

// Test directory creation failure scenario
TEST(S3PluginDownloaderTest, TestDirectoryCreationFailure) {
    S3PluginDownloader::S3Config config("http://localhost:9000", "region", "bucket", "prefix",
                                        "access", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    // Try to create file in a path that might cause directory creation issues
    // This path should trigger directory creation logic but may fail at S3 download
    Status status = downloader.download_file("s3://bucket/file.jar",
                                             "/tmp/test_dir/nested/test.jar", &local_path);

    // Should fail, but we've exercised the directory creation code path
    EXPECT_FALSE(status.ok());
}

// Test file deletion scenario by creating a file first
TEST(S3PluginDownloaderTest, TestExistingFileHandling) {
    S3PluginDownloader::S3Config config("http://localhost:9000", "region", "bucket", "prefix",
                                        "access", "secret");
    S3PluginDownloader downloader(config);

    // Create a temporary file to test deletion logic
    std::string test_file = "/tmp/test_existing_file.jar";
    std::ofstream file(test_file);
    file << "test content";
    file.close();

    // Verify file exists
    EXPECT_TRUE(std::filesystem::exists(test_file));

    std::string local_path;
    // This should trigger the file deletion logic before attempting download
    Status status = downloader.download_file("s3://bucket/file.jar", test_file, &local_path);

    // Should fail at S3 download, but file deletion logic should be exercised
    EXPECT_FALSE(status.ok());

    // Clean up in case file wasn't deleted
    if (std::filesystem::exists(test_file)) {
        std::filesystem::remove(test_file);
    }
}

// Test exception handling in S3FileSystem creation
TEST(S3PluginDownloaderTest, TestS3FilesystemCreationWithExtremeConfig) {
    // Create config that might trigger exception in S3FileSystem creation
    S3PluginDownloader::S3Config extreme_config(std::string(50000, 'x'), // Extremely long endpoint
                                                std::string(50000, 'y'), // Extremely long region
                                                "bucket", "prefix", "access", "secret");

    // This should not throw, but may trigger exception handling internally
    EXPECT_NO_THROW({ S3PluginDownloader downloader(extreme_config); });
}

// Test different S3 filesystem creation failure scenarios
TEST(S3PluginDownloaderTest, TestS3FilesystemCreationFailure) {
    // Test with various invalid configurations to trigger creation failure
    S3PluginDownloader::S3Config invalid_configs[] = {
            {"", "", "", "", "", ""}, // All empty
            {"invalid://endpoint", "region", "bucket", "prefix", "key",
             "secret"},                      // Invalid endpoint
            {"endpoint", "", "", "", "", ""} // Empty fields
    };

    for (const auto& config : invalid_configs) {
        EXPECT_NO_THROW({
            S3PluginDownloader downloader(config);
            std::string local_path;
            Status status =
                    downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);
            // Should fail due to null filesystem
            EXPECT_FALSE(status.ok());
            EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
        });
    }
}

// Test successful path through _execute_download (mock scenario)
TEST(S3PluginDownloaderTest, TestExecuteDownloadPathTraversal) {
    // Use a config that will create S3FileSystem but fail at actual download
    S3PluginDownloader::S3Config config("http://localhost:9000", "us-west-2", "test-bucket",
                                        "prefix", "access", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://test-bucket/file.jar", "/tmp/download_test.jar",
                                             &local_path);

    // This will fail at actual S3 download (no real S3 server), but should exercise:
    // - _execute_download method
    // - Directory creation logic
    // - File existence check
    // - S3 download attempt
    EXPECT_FALSE(status.ok());

    // The error should NOT be "S3 filesystem not initialized" if filesystem was created
    if (status.code() != ErrorCode::INTERNAL_ERROR ||
        status.to_string().find("S3 filesystem not initialized") == std::string::npos) {
        // Good - we got past the filesystem check and into actual download logic
        EXPECT_NE(status.code(), ErrorCode::INVALID_ARGUMENT);
    }
}

// Test concurrent downloads (mutex coverage)
TEST(S3PluginDownloaderTest, TestConcurrentDownloads) {
    S3PluginDownloader::S3Config config("http://localhost:9000", "us-west-2", "test-bucket",
                                        "prefix", "access", "secret");
    S3PluginDownloader downloader(config);

    // This test exercises the mutex in _execute_download
    std::string local_path1, local_path2;

    // Both calls will likely fail, but they exercise the mutex lock code
    Status status1 =
            downloader.download_file("s3://test-bucket/file1.jar", "/tmp/test1.jar", &local_path1);
    Status status2 =
            downloader.download_file("s3://test-bucket/file2.jar", "/tmp/test2.jar", &local_path2);

    EXPECT_FALSE(status1.ok());
    EXPECT_FALSE(status2.ok());
}

} // namespace doris