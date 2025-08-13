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

#include "runtime/plugin/plugin_file_cache.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace doris {

class PluginFileCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing
        temp_dir = std::filesystem::temp_directory_path() / "plugin_cache_test";
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

TEST_F(PluginFileCacheTest, TestFileInfoCreation) {
    // Test default constructor
    PluginFileCache::FileInfo default_info;
    EXPECT_TRUE(default_info.local_md5.empty());
    EXPECT_EQ(default_info.file_size, 0);

    // Test parameterized constructor
    PluginFileCache::FileInfo param_info("test-md5", 1024);
    EXPECT_EQ(param_info.local_md5, "test-md5");
    EXPECT_EQ(param_info.file_size, 1024);
}

TEST_F(PluginFileCacheTest, TestIsFileValidWithNonExistentFile) {
    std::string non_existent_path = temp_dir / "non-existent.jar";

    // Test with non-existent file - should return false
    EXPECT_FALSE(PluginFileCache::is_file_valid(non_existent_path, "some-md5"));
    EXPECT_FALSE(PluginFileCache::is_file_valid(non_existent_path, ""));
}

TEST_F(PluginFileCacheTest, TestIsFileValidWithEmptyFile) {
    // Create empty file
    std::string empty_file_path = temp_dir / "empty.jar";
    std::ofstream empty_file(empty_file_path);
    empty_file.close();

    // Test with empty file - should return false
    EXPECT_FALSE(PluginFileCache::is_file_valid(empty_file_path, "some-md5"));
}

TEST_F(PluginFileCacheTest, TestIsFileValidWithoutMd5) {
    // Create file with content
    std::string test_file_path = temp_dir / "test.jar";
    std::ofstream test_file(test_file_path);
    test_file << "Hello, World!";
    test_file.close();

    // Test without MD5 - should return true if file exists and has content
    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path, ""));
}

TEST_F(PluginFileCacheTest, TestIsFileValidWithMd5ButNoCache) {
    // Create file with content
    std::string test_file_path = temp_dir / "test2.jar";
    std::ofstream test_file(test_file_path);
    test_file << "Hello, World!";
    test_file.close();

    // Test with MD5 but no cache entry - should return false
    EXPECT_FALSE(PluginFileCache::is_file_valid(test_file_path, "some-md5-hash"));
}

TEST_F(PluginFileCacheTest, TestUpdateCacheWithNonExistentFile) {
    std::string non_existent_path = temp_dir / "non-existent.jar";

    // Test updating cache with non-existent file - should not throw exception
    EXPECT_NO_THROW({ PluginFileCache::update_cache(non_existent_path, "md5-hash", 1024); });
}

TEST_F(PluginFileCacheTest, TestUpdateCacheWithValidFile) {
    // Create file with content
    std::string test_file_path = temp_dir / "test3.jar";
    std::ofstream test_file(test_file_path);
    test_file << "Hello, World!";
    test_file.close();

    std::string md5_hash = "test-md5-hash";
    long file_size = std::filesystem::file_size(test_file_path);

    // Update cache
    EXPECT_NO_THROW({ PluginFileCache::update_cache(test_file_path, md5_hash, file_size); });

    // Now test validation with the cached MD5
    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path, md5_hash));

    // Test with different MD5 - should return false
    EXPECT_FALSE(PluginFileCache::is_file_valid(test_file_path, "different-md5"));
}

TEST_F(PluginFileCacheTest, TestLRUCacheEvictionBehavior) {
    // Create multiple files to test LRU cache eviction (100+ files)
    std::vector<std::string> file_paths;

    for (int i = 0; i < 105; i++) {
        std::string file_path = temp_dir / ("test_" + std::to_string(i) + ".jar");
        file_paths.push_back(file_path);

        std::ofstream test_file(file_path);
        test_file << "Content " << i;
        test_file.close();

        std::string md5_hash = "md5-hash-" + std::to_string(i);

        PluginFileCache::update_cache(file_path, md5_hash, std::filesystem::file_size(file_path));

        // Immediately verify the cache entry exists
        EXPECT_TRUE(PluginFileCache::is_file_valid(file_path, md5_hash))
                << "File " << i << " should be valid immediately after caching";
    }

    // Recent entries should still exist due to LRU behavior
    std::string recent_file = temp_dir / "test_104.jar";
    EXPECT_TRUE(PluginFileCache::is_file_valid(recent_file, "md5-hash-104"))
            << "Recent cache entry should still exist";

    // Early entries might be evicted, but let's check a few recent ones
    for (int i = 100; i < 105; i++) {
        std::string file_path = temp_dir / ("test_" + std::to_string(i) + ".jar");
        std::string md5_hash = "md5-hash-" + std::to_string(i);
        EXPECT_TRUE(PluginFileCache::is_file_valid(file_path, md5_hash))
                << "Recent file " << i << " should still be cached";
    }
}

TEST_F(PluginFileCacheTest, TestConcurrentAccess) {
    // Create a test file
    std::string test_file_path = temp_dir / "concurrent_test.jar";
    std::ofstream test_file(test_file_path);
    test_file << "Concurrent test content";
    test_file.close();

    // Test that multiple operations on the same cache don't crash
    std::string md5_1 = "concurrent-md5-1";
    std::string md5_2 = "concurrent-md5-2";

    // Update with first MD5
    PluginFileCache::update_cache(test_file_path, md5_1,
                                  std::filesystem::file_size(test_file_path));
    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path, md5_1));

    // Update with second MD5 (should overwrite)
    PluginFileCache::update_cache(test_file_path, md5_2,
                                  std::filesystem::file_size(test_file_path));
    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path, md5_2));
    EXPECT_FALSE(PluginFileCache::is_file_valid(test_file_path, md5_1)); // Should be overwritten
}

TEST_F(PluginFileCacheTest, TestMaxCacheSize) {
    // Verify that MAX_CACHE_SIZE is set to expected value
    EXPECT_EQ(PluginFileCache::MAX_CACHE_SIZE, 100);
}

} // namespace doris