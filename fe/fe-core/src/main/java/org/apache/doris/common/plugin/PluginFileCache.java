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

package org.apache.doris.common.plugin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

/**
 * PluginFileCache - Simple static memory cache for plugin file validation.
 * <p>
 * Caching logic:
 * 1. Users do not provide MD5 -> Use it directly if there are files locally (most efficient)
 * 2. User provides MD5 -> Quick check using cached MD5 (avoid recalculation)
 */
public class PluginFileCache {
    private static final Logger LOG = LogManager.getLogger(PluginFileCache.class);

    // Caffeine LRU cache with max 100 entries
    private static final Cache<String, FileInfo> CACHE = Caffeine.newBuilder()
            .maximumSize(100)
            .build();

    /**
     * Simple file info for caching
     */
    static class FileInfo {
        String localMd5;        // The MD5 value of the file for quick verification
        long fileSize;          // File size

        FileInfo() {
        }
    }

    /**
     * Check if local file is valid based on user requirements
     *
     * @param localPath local file path
     * @param userMd5 user provided MD5, can be null
     * @return true if file is valid and no download needed
     */
    public static boolean isFileValid(String localPath, String userMd5) {
        try {
            File file = new File(localPath);
            if (!file.exists() || file.length() == 0) {
                return false; // Case: No files -> download required
            }
            // Case: The user provides MD5 -> must validate the MD5 of the local cache
            if (!Strings.isNullOrEmpty(userMd5)) {
                FileInfo info = CACHE.getIfPresent(localPath);
                if (info == null || Strings.isNullOrEmpty(info.localMd5)) {
                    // There is no MD5 information in the cache, so you need to download and verify it again
                    return false;
                }
                // Use cached MD5 for quick verification
                return userMd5.equalsIgnoreCase(info.localMd5);
            }
            // Case: Users do not provide MD5 -> Use it directly if you have a file
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to validate file {}: {}", localPath, e.getMessage());
            return false;
        }
    }

    /**
     * Update cache after successful download
     *
     * @param localPath local file path
     * @param localMd5 MD5 hash of the local file
     * @param fileSize size of the file in bytes
     */
    public static void updateCache(String localPath, String localMd5, long fileSize) {
        try {
            File file = new File(localPath);
            if (!file.exists()) {
                return;
            }
            FileInfo info = new FileInfo();
            info.localMd5 = localMd5;
            info.fileSize = fileSize;
            CACHE.put(localPath, info);
        } catch (Exception e) {
            LOG.warn("Failed to update cache for {}: {}", localPath, e.getMessage());
        }
    }
}
