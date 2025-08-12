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

#include <chrono>
#include <string>
#include <unordered_map>

#include "common/status.h"

namespace doris {

/**
 * PluginFileCache - Simple static memory cache for plugin file validation.
 */
class PluginFileCache {
public:
    // Simple file info for caching
    struct FileInfo {
        std::string local_md5; // MD5 hash of the file for quick validation
        long file_size = 0;    // File size

        FileInfo() = default;
    };

    // Check if local file is valid
    static bool is_file_valid(const std::string& local_path, const std::string& user_md5);

    // Update cache after successful download
    static void update_cache(const std::string& local_path, const std::string& local_md5,
                             long file_size);

    // Clear cache entry
    static void clear_cache(const std::string& local_path);

    // Clear all cache
    static void clear_all_cache();

private:
    // Static cache: local_path -> FileInfo
    static std::unordered_map<std::string, FileInfo> _cache;

    // Mutex for thread safety
    static std::mutex _cache_mutex;
};

} // namespace doris