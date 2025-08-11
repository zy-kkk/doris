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
 * 
 * Core logic (same as FE):
 * 1. If user provides MD5 -> use MD5 validation
 * 2. If no user MD5 -> use ETag comparison  
 * 3. Cache expires after 8 hours -> need remote check
 */
class PluginFileCache {
public:
    /**
     * Simple file info for caching
     */
    struct FileInfo {
        std::string local_md5;
        std::string remote_etag;
        long remote_size = 0;
        std::chrono::system_clock::time_point cache_time;

        FileInfo() : cache_time(std::chrono::system_clock::now()) {}

        bool is_expired() const;
    };

    /**
     * Check if local file is valid and up-to-date
     * 
     * @param local_path local file path
     * @param user_md5 user provided MD5, can be empty
     * @return true if file is valid and no download needed
     */
    static bool is_file_valid(const std::string& local_path, const std::string& user_md5);

    /**
     * Check if we need to get remote ETag (optimization for cache)
     * Only check remote when:
     * 1. User provided MD5 (always need to validate against remote)
     * 2. No user MD5 but cache expired
     * 
     * @param local_path local file path
     * @param user_md5 user provided MD5, can be empty
     * @return true if need to check remote ETag
     */
    static bool needs_remote_check(const std::string& local_path, const std::string& user_md5);

    /**
     * Check if remote file has updates compared to cached info
     */
    static bool has_remote_update(const std::string& local_path, const std::string& remote_etag,
                                  long remote_size);

    /**
     * Update cache after successful download
     */
    static void update_cache(const std::string& local_path, const std::string& remote_etag,
                             long remote_size);

    /**
     * Clear cache entry
     */
    static void clear_cache(const std::string& local_path);

    /**
     * Clear all cache
     */
    static void clear_all_cache();

    /**
     * Calculate file MD5 (public utility method)
     */
    static std::string calculate_md5(const std::string& file_path);

private:
    // Cache expires after 8 hours
    static constexpr std::chrono::hours CACHE_EXPIRE_DURATION {8};

    // Static cache: local_path -> FileInfo
    static std::unordered_map<std::string, FileInfo> _cache;

    // Mutex for thread safety
    static std::mutex _cache_mutex;
};

} // namespace doris