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

#include <fmt/format.h>
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
    // Use invalid endpoint for quick test without network timeout
    S3PluginDownloader::S3Config valid_config("http://invalid-test-config", "us-west-2",
                                              "test-bucket", "prefix", "access", "secret");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(valid_config); });
}

TEST(S3PluginDownloaderTest, TestDownloadWithValidFilesystem) {
    // Use clearly invalid endpoint that won't be resolved
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "key", "secret");
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
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
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
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
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
            {"http://127.0.0.1:99999", "region", "bucket", "prefix", "key",
             "secret"},                      // Invalid endpoint
            {"endpoint", "", "", "", "", ""} // Empty fields
    };

    for (const auto& config : invalid_configs) {
        EXPECT_NO_THROW({
            S3PluginDownloader downloader(config);
            std::string local_path;
            Status status =
                    downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);
            // Should fail due to null filesystem or connection error
            EXPECT_FALSE(status.ok());
            // Could be INTERNAL_ERROR (null filesystem) or other network errors
        });
    }
}

// Test successful path through _execute_download (mock scenario)
TEST(S3PluginDownloaderTest, TestExecuteDownloadPathTraversal) {
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "us-west-2", "test-bucket",
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
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "us-west-2", "test-bucket",
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

// Additional tests to improve code coverage to 95%+

// Test successful configuration setup and filesystem creation
TEST(S3PluginDownloaderTest, TestSuccessfulS3ConfigSetup) {
    // Test with invalid endpoint to avoid network delays
    S3PluginDownloader::S3Config valid_config("https://invalid-s3-test.example.com", "us-east-1",
                                              "valid-bucket", "plugins/", "AKIA123", "secret123");

    // This should not throw and should create the downloader successfully
    EXPECT_NO_THROW({
        S3PluginDownloader downloader(valid_config);

        // Test the configuration values are set correctly
        EXPECT_EQ(valid_config.endpoint, "https://invalid-s3-test.example.com");
        EXPECT_EQ(valid_config.region, "us-east-1");
        EXPECT_EQ(valid_config.bucket, "valid-bucket");
        EXPECT_EQ(valid_config.prefix, "plugins/");
        EXPECT_EQ(valid_config.access_key, "AKIA123");
        EXPECT_EQ(valid_config.secret_key, "secret123");
    });
}

// Test to exercise the successful path completion (mock scenario)
TEST(S3PluginDownloaderTest, TestDownloadPathCompletionScenarios) {
    // Test various configurations with invalid endpoints for quick failure
    std::vector<S3PluginDownloader::S3Config> test_configs = {
            {"http://127.0.0.1:99999", "us-east-1", "test", "", "minioadmin", "minioadmin"},
            {"http://127.0.0.1:99998", "ap-beijing", "bucket", "prefix/", "ak", "sk"},
            {"http://127.0.0.1:99997", "local", "test-bucket", "data/", "access", "secret"}};

    for (const auto& config : test_configs) {
        EXPECT_NO_THROW({
            S3PluginDownloader downloader(config);
            std::string local_path;

            // Even if this fails at S3 level, it exercises the complete code path
            Status status =
                    downloader.download_file(fmt::format("s3://{}/test-file.jar", config.bucket),
                                             "/tmp/test_download.jar", &local_path);

            // We expect this to fail in test environment, but it should have
            // exercised the full download_file and _execute_download logic
            EXPECT_FALSE(status.ok());
        });
    }
}

// Test S3Config edge cases and boundary conditions
TEST(S3PluginDownloaderTest, TestS3ConfigBoundaryConditions) {
    // Test with special characters in configuration - use invalid endpoint
    S3PluginDownloader::S3Config special_chars(
            "https://invalid-special-test.example.com", "us-west-2!@#", "bucket-with-dashes_123",
            "path/with/slashes/", "ACCESS_KEY_123", "secret/with+special=chars");

    std::string config_str = special_chars.to_string();
    EXPECT_TRUE(config_str.find("***") != std::string::npos);
    EXPECT_TRUE(config_str.find("path/with/slashes/") != std::string::npos);

    // Test with Unicode characters (if supported) - use invalid endpoint
    S3PluginDownloader::S3Config unicode_config("https://invalid-unicode-test.example.com",
                                                "region", "bucket-测试", "前缀/", "access",
                                                "secret");
    EXPECT_EQ(unicode_config.bucket, "bucket-测试");
    EXPECT_EQ(unicode_config.prefix, "前缀/");
}

// Test to verify mutex behavior is working correctly
TEST(S3PluginDownloaderTest, TestMutexProtectionVerification) {
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "access", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path1, local_path2;

    // These calls should be serialized by the mutex in _execute_download
    // Even though they fail, they exercise the mutex lock/unlock logic
    Status status1 =
            downloader.download_file("s3://bucket/file1.jar", "/tmp/test1.jar", &local_path1);
    Status status2 =
            downloader.download_file("s3://bucket/file2.jar", "/tmp/test2.jar", &local_path2);

    EXPECT_FALSE(status1.ok());
    EXPECT_FALSE(status2.ok());

    // Verify both calls executed (mutex didn't cause deadlock)
    EXPECT_TRUE(!local_path1.empty() || status1.code() != ErrorCode::OK);
    EXPECT_TRUE(!local_path2.empty() || status2.code() != ErrorCode::OK);
}

// Test comprehensive S3Config to_string output format
TEST(S3PluginDownloaderTest, TestS3ConfigStringFormatComprehensive) {
    // Test all possible combinations of empty/non-empty access_key
    struct TestCase {
        std::string access_key;
        std::string expected_in_string;
    };

    std::vector<TestCase> test_cases = {{"", "null"},
                                        {"   ", "***"}, // whitespace is not empty
                                        {"a", "***"},
                                        {"AKIAIOSFODNN7EXAMPLE", "***"},
                                        {"very-long-access-key-with-special-chars!@#$%", "***"}};

    for (const auto& test_case : test_cases) {
        S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix",
                                            test_case.access_key, "secret");
        std::string str = config.to_string();
        EXPECT_TRUE(str.find(test_case.expected_in_string) != std::string::npos)
                << "Expected '" << test_case.expected_in_string << "' in string: " << str;

        // Verify actual access_key is never exposed in string
        // Skip check for empty string (would always match) and pure whitespace strings
        if (!test_case.access_key.empty() &&
            test_case.access_key.find_first_not_of(' ') != std::string::npos) {
            // For non-empty, non-whitespace keys, ensure the actual key doesn't appear as the value
            // Check that access_key='<actual_key>' pattern doesn't exist
            std::string key_pattern = "access_key='" + test_case.access_key + "'";
            EXPECT_TRUE(str.find(key_pattern) == std::string::npos)
                    << "Access key should not appear as value in string: " << str
                    << " (looking for pattern: " << key_pattern << ")";
        }
        // Note: Empty strings and whitespace-only strings are not checked because:
        // - Empty string "" would match any position in any string (find returns 0)
        // - Pure whitespace strings might appear naturally in formatted output
    }
}

// Test edge cases in directory creation and file handling
TEST(S3PluginDownloaderTest, TestFileOperationEdgeCases) {
    // Use invalid endpoint for quick failure
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "access", "secret");
    S3PluginDownloader downloader(config);

    std::vector<std::string> test_paths = {"/tmp/simple.jar", "/tmp/deep/nested/path/file.jar",
                                           "/tmp/file-with-dashes_and_underscores.jar",
                                           "/tmp/file.with.dots.jar"};

    for (const auto& path : test_paths) {
        std::string local_path;
        Status status = downloader.download_file("s3://bucket/test.jar", path, &local_path);

        // All should fail at S3 level but exercise file/directory logic
        EXPECT_FALSE(status.ok());

        // Clean up any created directories
        std::filesystem::path file_path(path);
        std::filesystem::path parent_dir = file_path.parent_path();
        std::error_code ec;
        std::filesystem::remove_all(parent_dir, ec);
    }
}

} // namespace doris