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

class S3PluginDownloaderTest : public testing::Test {
protected:
    void TearDown() override {
        std::vector<std::string> cleanup_files = {"/tmp/test.jar", "/tmp/existing_file.jar",
                                                  "/tmp/test_dir/nested/test.jar"};

        for (const auto& file : cleanup_files) {
            std::error_code ec;
            std::filesystem::remove(file, ec);
            std::filesystem::path parent = std::filesystem::path(file).parent_path();
            if (parent.string() != "/tmp") {
                std::filesystem::remove_all(parent, ec);
            }
        }
    }
};

// Test S3Config to_string method
TEST_F(S3PluginDownloaderTest, TestS3ConfigToString) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "access-key",
                                        "secret");
    std::string str = config.to_string();
    EXPECT_TRUE(str.find("***") != std::string::npos);
    EXPECT_TRUE(str.find("prefix='prefix'") != std::string::npos);

    S3PluginDownloader::S3Config empty_key("endpoint", "region", "bucket", "prefix", "", "secret");
    std::string empty_str = empty_key.to_string();
    EXPECT_TRUE(empty_str.find("null") != std::string::npos);
}

// Test constructor and destructor
TEST_F(S3PluginDownloaderTest, TestConstructorDestructor) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "key", "secret");
    EXPECT_NO_THROW({ S3PluginDownloader downloader(config); });
}

// Test uninitialized S3 filesystem
TEST_F(S3PluginDownloaderTest, TestS3FilesystemNotInitialized) {
    S3PluginDownloader::S3Config empty_config("", "", "", "", "", "");
    S3PluginDownloader downloader(empty_config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(status.to_string().find("S3 filesystem not initialized") != std::string::npos);
}

// Test download failure
TEST_F(S3PluginDownloaderTest, TestDownloadFailure) {
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "key", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("S3 filesystem not initialized"), 0);
}

// Test directory creation failure
TEST_F(S3PluginDownloaderTest, TestDirectoryCreationFailure) {
    std::string blocking_file = "/tmp/blocking_file";
    std::ofstream file(blocking_file);
    file.close();

    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "key", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", blocking_file + "/test.jar",
                                             &local_path);

    std::filesystem::remove(blocking_file);
    EXPECT_FALSE(status.ok());
}

// Test existing file handling
TEST_F(S3PluginDownloaderTest, TestExistingFileHandling) {
    std::string existing_file = "/tmp/existing_file.jar";
    std::ofstream file(existing_file);
    file << "existing content";
    file.close();

    EXPECT_TRUE(std::filesystem::exists(existing_file));

    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "region", "bucket", "prefix",
                                        "key", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", existing_file, &local_path);

    EXPECT_FALSE(status.ok());
}

// Test S3FileSystem creation exception handling
TEST_F(S3PluginDownloaderTest, TestS3FilesystemCreationException) {
    S3PluginDownloader::S3Config extreme_config(std::string(50000, 'x'), "region", "bucket",
                                                "prefix", "key", "secret");

    EXPECT_NO_THROW({
        S3PluginDownloader downloader(extreme_config);
        std::string local_path;
        Status status =
                downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);
        EXPECT_FALSE(status.ok());
    });
}

// Test S3FileSystem creation failure
TEST_F(S3PluginDownloaderTest, TestS3FilesystemCreationFailure) {
    S3PluginDownloader::S3Config invalid_config("", "", "", "", "", "");
    EXPECT_NO_THROW({ S3PluginDownloader downloader(invalid_config); });
}

// Test download path exercise
TEST_F(S3PluginDownloaderTest, TestDownloadPathExercise) {
    S3PluginDownloader::S3Config config("http://127.0.0.1:99999", "us-west-2", "test-bucket",
                                        "prefix/", "access", "secret");
    S3PluginDownloader downloader(config);

    std::string local_path;
    Status status = downloader.download_file("s3://test-bucket/file.jar",
                                             "/tmp/test_dir/nested/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
}

// Test config fields
TEST_F(S3PluginDownloaderTest, TestConfigFields) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "prefix", "access",
                                        "secret");

    EXPECT_EQ(config.endpoint, "endpoint");
    EXPECT_EQ(config.region, "region");
    EXPECT_EQ(config.bucket, "bucket");
    EXPECT_EQ(config.prefix, "prefix");
    EXPECT_EQ(config.access_key, "access");
    EXPECT_EQ(config.secret_key, "secret");
}

} // namespace doris