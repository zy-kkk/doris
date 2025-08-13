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

// Static member definitions
std::array<std::mutex, S3PluginDownloader::LOCK_SHARD_SIZE> S3PluginDownloader::_file_locks;

std::string S3PluginDownloader::S3Config::to_string() const {
    return fmt::format("S3Config{{endpoint='{}', region='{}', bucket='{}', access_key='{}'}}",
                       endpoint, region, bucket, access_key.empty() ? "null" : "***");
}

S3PluginDownloader::S3PluginDownloader(const S3Config& config) : _config(config) {
    _s3_fs = _create_s3_filesystem(_config);
}

S3PluginDownloader::~S3PluginDownloader() = default;

Status S3PluginDownloader::download_file(const std::string& remote_s3_path,
                                         const std::string& local_target_path,
                                         std::string* local_path, const std::string& expected_md5) {
    // Check if S3 filesystem is initialized
    if (!_s3_fs) {
        return Status::InternalError("S3 filesystem not initialized");
    }

    // Use file-level locking to prevent concurrent downloads of the same file
    std::lock_guard file_lock(_get_lock_for_path(local_target_path));

    // Check if download is needed (within lock to ensure consistency)
    if (PluginFileCache::is_file_valid(local_target_path, expected_md5)) {
        *local_path = local_target_path;
        return Status::OK(); // Local file is valid, return directly
    }

    // Execute download
    Status download_status = _execute_download(remote_s3_path, local_target_path, expected_md5);

    if (download_status.ok()) {
        *local_path = local_target_path;
        return Status::OK();
    }
    return download_status;
}

Status S3PluginDownloader::_execute_download(const std::string& remote_s3_path,
                                             const std::string& local_path,
                                             const std::string& expected_md5) {
    // Create parent directory using local filesystem
    std::filesystem::path file_path(local_path);
    std::filesystem::path parent_dir = file_path.parent_path();
    if (!parent_dir.empty()) {
        Status status = io::global_local_filesystem()->create_directory(parent_dir.string());
        // Ignore error if directory already exists
        if (!status.ok() && !status.is<ErrorCode::FILE_ALREADY_EXIST>()) {
            RETURN_IF_ERROR(status);
        }
    }

    // Delete existing file if present (to ensure clean download)
    if (std::filesystem::exists(local_path)) {
        std::error_code ec;
        if (!std::filesystem::remove(local_path, ec)) {
            return Status::InternalError("Failed to delete existing file: {} ({})", local_path,
                                         ec.message());
        }
    }

    // Use S3FileSystem's public download method
    Status download_status = _s3_fs->download(remote_s3_path, local_path);
    RETURN_IF_ERROR(download_status);

    // MD5 verification and cache update
    std::string actual_md5 = _calculate_file_md5(local_path);

    // If user provided MD5, must verify consistency
    if (!expected_md5.empty()) {
        if (actual_md5.empty() || expected_md5 != actual_md5) {
            std::filesystem::remove(local_path); // Delete invalid file
            return Status::InvalidArgument("MD5 mismatch: expected={}, actual={}", expected_md5,
                                           actual_md5);
        }
    }

    // Update cache
    _update_cache_after_download(local_path, actual_md5);

    LOG(INFO) << "Successfully downloaded " << remote_s3_path << " to " << local_path;
    return Status::OK();
}

void S3PluginDownloader::_update_cache_after_download(const std::string& local_path,
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

std::string S3PluginDownloader::_calculate_file_md5(const std::string& file_path) {
    std::string md5_result;
    Status status = io::global_local_filesystem()->md5sum(file_path, &md5_result);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to calculate MD5 for file " << file_path << ": "
                     << status.to_string();
        return "";
    }
    return md5_result;
}

std::shared_ptr<io::S3FileSystem> S3PluginDownloader::_create_s3_filesystem(
        const S3Config& config) {
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

std::mutex& S3PluginDownloader::_get_lock_for_path(const std::string& file_path) {
    // Use hash-based sharding to distribute locks (similar to TxnManager approach)
    size_t hash = std::hash<std::string> {}(file_path);
    return _file_locks[hash % LOCK_SHARD_SIZE];
}

} // namespace doris