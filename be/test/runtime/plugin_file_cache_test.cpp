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

#include "runtime/plugin_file_cache.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace doris {

class PluginFileCacheTest : public ::testing::Test {
public:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "plugin_cache_test";
        std::filesystem::create_directories(test_dir_);

        test_file_path_ = (test_dir_ / "test_plugin.jar").string();
        test_md5_ = "1e48521ebe1a2489118e4785461b17b8";
        test_content_ = "test content";

        std::filesystem::remove(test_file_path_);

        PluginFileCache::clear_all_cache();
    }

    void TearDown() override {
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }

        PluginFileCache::clear_all_cache();
    }

protected:
    std::filesystem::path test_dir_;
    std::string test_file_path_;
    std::string test_md5_;
    std::string test_content_;
};

TEST_F(PluginFileCacheTest, TestIsFileValidWithNonExistentFile) {
    EXPECT_FALSE(PluginFileCache::is_file_valid(test_file_path_, ""));
    EXPECT_FALSE(PluginFileCache::is_file_valid(test_file_path_, test_md5_));
}

TEST_F(PluginFileCacheTest, TestIsFileValidWithExistingFileNoMd5) {
    std::ofstream file(test_file_path_);
    file << test_content_;
    file.close();

    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path_, ""));
}

TEST_F(PluginFileCacheTest, TestUpdateCacheAndCheckFile) {
    std::ofstream file(test_file_path_);
    file << test_content_;
    file.close();

    PluginFileCache::update_cache(test_file_path_, "etag1", test_content_.length());

    EXPECT_TRUE(PluginFileCache::is_file_valid(test_file_path_, ""));
}

TEST_F(PluginFileCacheTest, TestNeedsRemoteCheckWithNonExistentFile) {
    EXPECT_TRUE(PluginFileCache::needs_remote_check(test_file_path_, ""));
    EXPECT_TRUE(PluginFileCache::needs_remote_check(test_file_path_, test_md5_));
}

TEST_F(PluginFileCacheTest, TestHasRemoteUpdateWithNonExistentFile) {
    EXPECT_TRUE(PluginFileCache::has_remote_update(test_file_path_, "etag1", 0));
}

TEST_F(PluginFileCacheTest, TestClearCache) {
    std::ofstream file(test_file_path_);
    file << test_content_;
    file.close();

    PluginFileCache::update_cache(test_file_path_, "etag1", test_content_.length());
    PluginFileCache::clear_all_cache();
    EXPECT_TRUE(PluginFileCache::needs_remote_check(test_file_path_, ""));
}

TEST_F(PluginFileCacheTest, TestMultipleFileCache) {
    std::string file1 = (test_dir_ / "file1.jar").string();
    std::string file2 = (test_dir_ / "file2.jar").string();
    std::string content1 = "content1";
    std::string content2 = "content2";

    std::ofstream f1(file1);
    f1 << content1;
    f1.close();

    std::ofstream f2(file2);
    f2 << content2;
    f2.close();

    PluginFileCache::update_cache(file1, "etag1", content1.length());
    PluginFileCache::update_cache(file2, "etag2", content2.length());

    EXPECT_TRUE(PluginFileCache::is_file_valid(file1, ""));
    EXPECT_TRUE(PluginFileCache::is_file_valid(file2, ""));
}

} // namespace doris