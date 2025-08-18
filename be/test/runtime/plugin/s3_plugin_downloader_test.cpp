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

TEST(S3PluginDownloaderTest, TestS3ConfigToString) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "access-key", "secret");
    std::string config_str = config.to_string();
    EXPECT_TRUE(config_str.find("***") != std::string::npos);

    S3PluginDownloader::S3Config empty_key_config("endpoint", "region", "bucket", "", "secret");
    std::string empty_str = empty_key_config.to_string();
    EXPECT_TRUE(empty_str.find("null") != std::string::npos);
}

TEST(S3PluginDownloaderTest, TestConstructorAndDestructor) {
    S3PluginDownloader::S3Config config("endpoint", "region", "bucket", "key", "secret");
    EXPECT_NO_THROW({ S3PluginDownloader downloader(config); });
}

TEST(S3PluginDownloaderTest, TestDownloadFileWithNullS3Filesystem) {
    S3PluginDownloader::S3Config empty_config("", "", "", "", "");
    S3PluginDownloader downloader(empty_config);

    std::string local_path;
    Status status = downloader.download_file("s3://bucket/file.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(status.to_string().find("S3 filesystem not initialized") != std::string::npos);
}

TEST(S3PluginDownloaderTest, TestS3FilesystemCreationException) {
    S3PluginDownloader::S3Config invalid_config(std::string(10000, 'x'), "region", "bucket", "key",
                                                "secret");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(invalid_config); });
}

TEST(S3PluginDownloaderTest, TestValidS3Config) {
    S3PluginDownloader::S3Config valid_config("http://localhost:9000", "us-west-2", "test-bucket",
                                              "access", "secret");

    EXPECT_NO_THROW({ S3PluginDownloader downloader(valid_config); });
}

} // namespace doris