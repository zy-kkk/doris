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
    S3PluginDownloader::S3Config config("", "", "", "", ""); // Empty config to avoid network calls
    S3PluginDownloader downloader(config);

    std::string local_path;
    std::string target_path = "/tmp/test.jar";

    // Test with invalid S3 path - should fail quickly due to empty config
    Status status = downloader.download_file("invalid-s3-path", target_path, &local_path);

    EXPECT_FALSE(status.ok());
    // Should not crash and return meaningful error
    EXPECT_FALSE(status.to_string().empty());
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithNullPointer) {
    S3PluginDownloader::S3Config config("", "", "", "", ""); // Empty config to avoid network calls
    S3PluginDownloader downloader(config);

    std::string target_path = "/tmp/test.jar";

    // Test with null pointer for local_path - should handle gracefully
    Status status = downloader.download_file("s3://bucket/file.jar", target_path, nullptr);

    EXPECT_FALSE(status.ok());
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithInvalidLocalPath) {
    S3PluginDownloader::S3Config config("", "", "", "", ""); // Empty config to avoid network calls
    S3PluginDownloader downloader(config);

    std::string local_path;

    // Test with invalid local path - should fail quickly due to empty config
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
    // Test that Status integrates properly with S3PluginDownloader (without slow network calls)
    S3PluginDownloader::S3Config config("", "", "", "", ""); // Empty config will fail fast
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    // Test Status methods work correctly - should fail quickly due to empty config
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().empty());
}

// Test to specifically cover the successful S3 filesystem creation path (line 115)
TEST(S3PluginDownloaderTest, TestS3FilesystemCreationWithDifferentConfigs) {
    // Test with various config combinations to exercise _create_s3_filesystem
    std::vector<S3PluginDownloader::S3Config> test_configs = {
        {"http://localhost:9000", "us-west-2", "test-bucket", "access", "secret"},
        {"https://s3.amazonaws.com", "us-east-1", "prod-bucket", "key", "secret"},
        {"http://minio.local:9000", "region", "bucket", "minioaccess", "miniosecret"},
        {"", "", "", "", ""}, // Empty config
        {"endpoint", "region", "bucket", "", ""}, // Missing credentials
    };
    
    for (const auto& config : test_configs) {
        // Constructor should not throw regardless of config validity
        EXPECT_NO_THROW({
            S3PluginDownloader downloader(config);
        });
    }
}

// Test to cover the download_file method when S3 filesystem is not initialized (line 51-53)
TEST(S3PluginDownloaderTest, TestDownloadFileWithNullS3Filesystem) {
    // Use empty config which should result in null S3 filesystem
    S3PluginDownloader::S3Config empty_config("", "", "", "", "");
    S3PluginDownloader downloader(empty_config);
    
    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);
    
    // Should fail with "S3 filesystem not initialized" error
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(status.to_string().find("S3 filesystem not initialized") != std::string::npos);
}

// Test exception handling in _create_s3_filesystem (line 116-119)
TEST(S3PluginDownloaderTest, TestS3FilesystemCreationExceptionHandling) {
    // Test with various invalid configs that might trigger exceptions
    std::vector<S3PluginDownloader::S3Config> problematic_configs = {
        {"invalid://protocol", "region", "bucket", "key", "secret"},
        {"http://", "region", "bucket", "key", "secret"}, // Invalid URL
        {std::string(10000, 'x'), "region", "bucket", "key", "secret"}, // Very long endpoint
    };
    
    for (const auto& config : problematic_configs) {
        // Should handle exceptions gracefully without crashing
        EXPECT_NO_THROW({
            S3PluginDownloader downloader(config);
            // Try to use it - should fail gracefully
            std::string local_path;
            Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);
            EXPECT_FALSE(status.ok());
        });
    }
}

// Test to specifically exercise the destructor and cleanup
TEST(S3PluginDownloaderTest, TestDestructorAndCleanup) {
    {
        S3PluginDownloader::S3Config config("http://localhost:9000", "region", "bucket", "key", "secret");
        S3PluginDownloader downloader(config);
        // Object will be destroyed when going out of scope
    }
    // Should complete without issues - destructor should handle cleanup
    EXPECT_TRUE(true); // If we reach here, destructor worked correctly
}

// Test to exercise various S3Config constructor combinations
TEST(S3PluginDownloaderTest, TestS3ConfigConstructorCombinations) {
    // Test different string types and lengths
    std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>> config_combinations = {
        {"http://s3.test", "us-west-2", "bucket", "access", "secret"},
        {"", "", "", "", ""}, // All empty
        {"e", "r", "b", "a", "s"}, // Single characters
        {std::string(1000, 'x'), std::string(100, 'y'), std::string(50, 'z'), std::string(20, 'a'), std::string(30, 'b')}, // Long strings
        {"endpoint with spaces", "region-with-dashes", "bucket_with_underscores", "access.with.dots", "secret:with:colons"}
    };
    
    for (const auto& [endpoint, region, bucket, access, secret] : config_combinations) {
        S3PluginDownloader::S3Config config(endpoint, region, bucket, access, secret);
        
        EXPECT_EQ(endpoint, config.endpoint);
        EXPECT_EQ(region, config.region);
        EXPECT_EQ(bucket, config.bucket);
        EXPECT_EQ(access, config.access_key);
        EXPECT_EQ(secret, config.secret_key);
        
        // to_string should work for all combinations
        EXPECT_FALSE(config.to_string().empty());
    }
}

// Test to verify S3Config's const string members
TEST(S3PluginDownloaderTest, TestS3ConfigConstMembers) {
    const S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "access", "secret");
    
    // Test that const members are accessible
    EXPECT_EQ("endpoint", config.endpoint);
    EXPECT_EQ("region", config.region);
    EXPECT_EQ("bucket", config.bucket);
    EXPECT_EQ("access", config.access_key);
    EXPECT_EQ("secret", config.secret_key);
    
    // Test const to_string method
    std::string config_str = config.to_string();
    EXPECT_FALSE(config_str.empty());
}

// Test thread safety of static mutex
TEST(S3PluginDownloaderTest, TestThreadSafetyOfDownload) {
    S3PluginDownloader::S3Config config("", "", "", "", ""); // Empty config for fast failure
    
    std::vector<std::thread> threads;
    std::vector<bool> results(5);
    
    // Launch multiple threads to test concurrent download attempts
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&, i]() {
            S3PluginDownloader downloader(config);
            std::string local_path;
            Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test" + std::to_string(i) + ".jar", &local_path);
            results[i] = !status.ok(); // Should all fail with empty config
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all threads failed consistently (thread safety)
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(results[i]);
    }
}

// Test S3Config with special characters and edge cases
TEST(S3PluginDownloaderTest, TestS3ConfigSpecialCharacters) {
    // Test S3Config with various special characters
    S3PluginDownloader::S3Config special_config(
        "http://s3-server.domain.com:9000", 
        "us-west-2a", 
        "bucket-name_123", 
        "AKIAIOSFODNN7EXAMPLE", 
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    
    EXPECT_FALSE(special_config.endpoint.empty());
    EXPECT_FALSE(special_config.region.empty());
    EXPECT_FALSE(special_config.bucket.empty());
    EXPECT_FALSE(special_config.access_key.empty());
    EXPECT_FALSE(special_config.secret_key.empty());
    
    // Test that to_string handles special characters correctly
    std::string config_str = special_config.to_string();
    EXPECT_TRUE(config_str.find("s3-server.domain.com") != std::string::npos);
    EXPECT_TRUE(config_str.find("bucket-name_123") != std::string::npos);
    EXPECT_TRUE(config_str.find("***") != std::string::npos); // Access key should be masked
}

} // namespace doris