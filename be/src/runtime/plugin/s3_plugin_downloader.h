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

#pragma once

#include <memory>
#include <string>

#include "common/status.h"

// Forward declarations
namespace doris::io {
class S3FileSystem;
}

namespace doris {

/**
 * S3PluginDownloader is an independent S3 downloader with MD5 verification and retry.
 */
class S3PluginDownloader {
public:
    // S3 configuration info
    struct S3Config {
        std::string endpoint;
        std::string region;
        std::string bucket;
        std::string access_key;
        std::string secret_key;

        S3Config(const std::string& endpoint, const std::string& region, const std::string& bucket,
                 const std::string& access_key, const std::string& secret_key)
                : endpoint(endpoint),
                  region(region),
                  bucket(bucket),
                  access_key(access_key),
                  secret_key(secret_key) {}

        std::string to_string() const;
    };

    explicit S3PluginDownloader(const S3Config& config);
    ~S3PluginDownloader();

    // Download single file with MD5 verification and retry
    Status download_file(const std::string& remote_s3_path, const std::string& local_target_path,
                         std::string* local_path, const std::string& expected_md5 = "");

private:
    static constexpr int MAX_RETRY_ATTEMPTS = 3;
    static constexpr int RETRY_DELAY_MS = 1000;

    S3Config config_;
    std::shared_ptr<io::S3FileSystem> s3_fs_;

    // Execute single file download with retry logic
    Status execute_download(const std::string& remote_s3_path, const std::string& local_path,
                            const std::string& expected_md5);

    // Sleep for retry with exponential backoff
    void sleep_for_retry(int attempt);

    // Update cache after download
    void update_cache_after_download(const std::string& local_path, const std::string& actual_md5);

    // Calculate file MD5
    std::string calculate_file_md5(const std::string& file_path);

    // Create S3 file system
    std::shared_ptr<io::S3FileSystem> create_s3_filesystem(const S3Config& config);

    // Create parent directory
    Status create_parent_directory(const std::string& file_path);
};

} // namespace doris