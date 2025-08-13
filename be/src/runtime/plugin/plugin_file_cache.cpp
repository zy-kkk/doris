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

#include <filesystem>
#include <string>

#include "common/logging.h"

namespace doris {

// Static member definitions
LruCache<std::string, PluginFileCache::FileInfo> PluginFileCache::_cache {MAX_CACHE_SIZE};
std::mutex PluginFileCache::_cache_mutex;

bool PluginFileCache::is_file_valid(const std::string& local_path, const std::string& user_md5) {
    try {
        // Check if file exists and is not empty
        if (!std::filesystem::exists(local_path) || std::filesystem::file_size(local_path) == 0) {
            return false; // Case: No files -> download required
        }

        // Case: User provides MD5 -> must validate the MD5 of the local cache
        if (!user_md5.empty()) {
            std::lock_guard lock(_cache_mutex);
            FileInfo cached_info;
            if (_cache.get(local_path, &cached_info)) {
                // Found in cache, use cached MD5 for quick verification
                return user_md5 == cached_info.local_md5;
            }
            // Not in cache, need to download and verify
            return false;
        }

        // Case: User does not provide MD5 -> Use it directly if you have a file (most efficient)
        return true;

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to validate file " << local_path << ": " << e.what();
        return false;
    }
}

void PluginFileCache::update_cache(const std::string& local_path, const std::string& local_md5,
                                   long file_size) {
    try {
        if (!std::filesystem::exists(local_path)) {
            return;
        }
        std::lock_guard lock(_cache_mutex);
        _cache.put(local_path, FileInfo(local_md5, file_size));
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to update cache for " << local_path << ": " << e.what();
    }
}

} // namespace doris