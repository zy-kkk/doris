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

#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>

#include "common/logging.h"
#include "util/md5.h"

namespace doris {

// Static member definitions
std::unordered_map<std::string, PluginFileCache::FileInfo> PluginFileCache::_cache;
std::mutex PluginFileCache::_cache_mutex;

bool PluginFileCache::FileInfo::is_expired() const {
    auto now = std::chrono::system_clock::now();
    return now - cache_time > CACHE_EXPIRE_DURATION;
}

bool PluginFileCache::is_file_valid(const std::string& local_path, const std::string& user_md5) {
    try {
        // Check if file exists and is not empty
        if (!std::filesystem::exists(local_path) || std::filesystem::file_size(local_path) == 0) {
            return false;
        }

        std::lock_guard<std::mutex> lock(_cache_mutex);

        // Get or create file info
        auto& info = _cache[local_path];
        if (info.cache_time == std::chrono::system_clock::time_point {}) {
            info = FileInfo(); // Initialize if new entry
        }

        // If user provides MD5, always validate with MD5 (user MD5 is authoritative)
        if (!user_md5.empty()) {
            if (info.local_md5.empty()) {
                info.local_md5 = calculate_md5(local_path);
            }
            bool md5_match = (user_md5 == info.local_md5);
            VLOG_DEBUG << "User MD5 validation for " << local_path << ": expected=" << user_md5
                       << ", actual=" << info.local_md5 << ", match=" << md5_match;
            return md5_match;
        }

        // No user MD5, use cache expiry logic (optimization: don't check remote if cache valid)
        return !info.is_expired();

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to validate file " << local_path << ": " << e.what();
        return false;
    }
}

bool PluginFileCache::needs_remote_check(const std::string& local_path,
                                         const std::string& user_md5) {
    // If user provided MD5, always need remote validation
    if (!user_md5.empty()) {
        return true;
    }

    std::lock_guard<std::mutex> lock(_cache_mutex);

    // If no user MD5, only check remote when cache expired
    auto it = _cache.find(local_path);
    return it == _cache.end() || it->second.is_expired();
}

bool PluginFileCache::has_remote_update(const std::string& local_path,
                                        const std::string& remote_etag, long remote_size) {
    std::lock_guard<std::mutex> lock(_cache_mutex);

    auto it = _cache.find(local_path);
    if (it == _cache.end()) {
        return true; // No cache, assume updated
    }

    const auto& info = it->second;

    // Compare ETag if both exist
    if (!info.remote_etag.empty() && !remote_etag.empty()) {
        bool etag_changed = (info.remote_etag != remote_etag);
        if (etag_changed) {
            VLOG_DEBUG << "Remote ETag changed for " << local_path << ": " << info.remote_etag
                       << " -> " << remote_etag;
        }
        return etag_changed;
    }

    // Compare size if ETag not available
    bool size_changed = (info.remote_size > 0 && info.remote_size != remote_size);
    if (size_changed) {
        VLOG_DEBUG << "Remote size changed for " << local_path << ": " << info.remote_size << " -> "
                   << remote_size;
    }
    return size_changed;
}

void PluginFileCache::update_cache(const std::string& local_path, const std::string& remote_etag,
                                   long remote_size) {
    try {
        if (!std::filesystem::exists(local_path)) {
            return;
        }

        std::lock_guard<std::mutex> lock(_cache_mutex);

        FileInfo info;
        info.remote_etag = remote_etag;
        info.remote_size = remote_size;
        info.local_md5 = calculate_md5(local_path); // Pre-calculate MD5 for future use

        _cache[local_path] = info;
        VLOG_DEBUG << "Updated cache for: " << local_path;

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to update cache for " << local_path << ": " << e.what();
    }
}

void PluginFileCache::clear_cache(const std::string& local_path) {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    _cache.erase(local_path);
}

void PluginFileCache::clear_all_cache() {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    _cache.clear();
    LOG(INFO) << "Cleared all plugin file cache";
}

std::string PluginFileCache::calculate_md5(const std::string& file_path) {
    try {
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            LOG(WARNING) << "Cannot open file for MD5 calculation: " << file_path;
            return "";
        }

        Md5Digest digest;
        char buffer[8192];

        while (file.read(buffer, sizeof(buffer)) || file.gcount() > 0) {
            std::streamsize bytes_read = file.gcount();
            if (bytes_read > 0) {
                digest.update(buffer, bytes_read);
            }
            if (file.eof()) break;
        }

        // Must call digest() before hex() to finalize the computation
        digest.digest();
        return digest.hex();

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to calculate MD5 for file " << file_path << ": " << e.what();
        return "";
    }
}

} // namespace doris