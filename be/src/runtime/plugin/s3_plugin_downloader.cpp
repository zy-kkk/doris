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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "runtime/plugin/plugin_file_cache.h"
#include "util/s3_util.h"

namespace doris {

std::string S3PluginDownloader::S3Config::to_string() const {
    return fmt::format("S3Config{{endpoint='{}', region='{}', bucket='{}', access_key='{}'}}",
                       endpoint, region, bucket, access_key.empty() ? "null" : "***");
}

S3PluginDownloader::S3PluginDownloader(const S3Config& config) : config_(config) {
    s3_fs_ = create_s3_filesystem(config_);
    // Note: We don't throw here. The caller should check via download_file return status
}

S3PluginDownloader::~S3PluginDownloader() = default;

Status S3PluginDownloader::download_file(const std::string& remote_s3_path,
                                         const std::string& local_target_path,
                                         std::string* local_path, const std::string& expected_md5) {
    // Check if S3 filesystem is initialized
    if (!s3_fs_) {
        return Status::InternalError("S3 filesystem not initialized");
    }

    // Check if download is needed first
    if (PluginFileCache::is_file_valid(local_target_path, expected_md5)) {
        *local_path = local_target_path;
        return Status::OK(); // Local file is valid, return directly
    }

    // Execute download retry logic
    Status last_status;
    for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; ++attempt) {
        last_status = execute_download(remote_s3_path, local_target_path, expected_md5);
        if (last_status.ok()) {
            *local_path = local_target_path;
            return Status::OK();
        }
        if (attempt < MAX_RETRY_ATTEMPTS) {
            sleep_for_retry(attempt);
        }
    }

    // All retries failed
    return last_status;
}

Status S3PluginDownloader::execute_download(const std::string& remote_s3_path,
                                            const std::string& local_path,
                                            const std::string& expected_md5) {
    // Create parent directory
    Status status = create_parent_directory(local_path);
    RETURN_IF_ERROR(status);

    // Use S3FileSystem's public download method
    status = s3_fs_->download(remote_s3_path, local_path);
    RETURN_IF_ERROR(status);

    // MD5 verification and cache update
    std::string actual_md5 = calculate_file_md5(local_path);

    // If user provided MD5, must verify consistency
    if (!expected_md5.empty()) {
        if (actual_md5.empty() || expected_md5 != actual_md5) {
            std::filesystem::remove(local_path); // Delete invalid file
            return Status::InvalidArgument("MD5 mismatch: expected={}, actual={}", expected_md5,
                                           actual_md5);
        }
    }

    // Update cache
    update_cache_after_download(local_path, actual_md5);

    LOG(INFO) << "Successfully downloaded " << remote_s3_path << " to " << local_path;
    return Status::OK();
}

void S3PluginDownloader::sleep_for_retry(int attempt) {
    try {
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS * attempt));
    } catch (const std::exception& e) {
        throw std::runtime_error("Download interrupted: " + std::string(e.what()));
    }
}

void S3PluginDownloader::update_cache_after_download(const std::string& local_path,
                                                     const std::string& actual_md5) {
    try {
        std::filesystem::path file_path(local_path);
        if (!std::filesystem::exists(file_path)) {
            return;
        }

        long file_size = std::filesystem::file_size(file_path);
        PluginFileCache::update_cache(local_path, actual_md5, file_size);
    } catch (const std::exception& e) {
        // Ignore cache update failures - not critical
        LOG(WARNING) << "Failed to update cache for " << local_path << ": " << e.what();
    }
}

Status S3PluginDownloader::create_parent_directory(const std::string& file_path) {
    try {
        std::filesystem::path path(file_path);
        std::filesystem::path parent_dir = path.parent_path();

        if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
            std::filesystem::create_directories(parent_dir);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to create parent directory: {}", e.what());
    }
}

std::string S3PluginDownloader::calculate_file_md5(const std::string& file_path) {
    std::string md5_result;
    Status status = io::global_local_filesystem()->md5sum(file_path, &md5_result);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to calculate MD5 for file " << file_path << ": "
                     << status.to_string();
        return "";
    }
    return md5_result;
}

std::shared_ptr<io::S3FileSystem> S3PluginDownloader::create_s3_filesystem(const S3Config& config) {
    try {
        // Create S3 configuration for S3FileSystem
        S3Conf s3_conf;
        s3_conf.client_conf.endpoint = config.endpoint;
        s3_conf.client_conf.region = config.region;
        s3_conf.client_conf.ak = config.access_key;
        s3_conf.client_conf.sk = config.secret_key;
        s3_conf.client_conf.provider = io::ObjStorageType::AWS; // Default to AWS compatible
        s3_conf.bucket = config.bucket;
        s3_conf.prefix = ""; // No prefix for direct S3 access

        // Create S3FileSystem using static factory method
        auto result = io::S3FileSystem::create(s3_conf, "s3_plugin_downloader");
        if (!result.has_value()) {
            LOG(WARNING) << "Failed to create S3FileSystem: " << result.error().to_string();
            return nullptr;
        }

        return result.value();
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception creating S3 filesystem: " << e.what();
        return nullptr;
    }
}

} // namespace doris