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

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PluginFileCache - Simple static memory cache for plugin file validation.
 * <p>
 * Core logic:
 * 1. If user provides MD5 -> use MD5 validation
 * 2. If no user MD5 -> use ETag comparison
 * 3. Cache expires after 8 hours -> need remote check
 */
public class PluginFileCache {
    private static final Logger LOG = LogManager.getLogger(PluginFileCache.class);

    // Cache: localPath -> FileInfo
    private static final ConcurrentHashMap<String, FileInfo> CACHE = new ConcurrentHashMap<>();

    // Cache expires after 8 hours
    private static final long CACHE_EXPIRE_MS = 8 * 60 * 60 * 1000L;

    /**
     * Simple file info for caching
     */
    static class FileInfo {
        String localMd5;
        String remoteEtag;
        long remoteSize;
        long cacheTime;

        FileInfo() {
            this.cacheTime = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - cacheTime > CACHE_EXPIRE_MS;
        }
    }

    /**
     * Check if local file is valid and up-to-date
     *
     * @param localPath local file path
     * @param userMd5 user provided MD5, can be null
     * @return true if file is valid and no download needed
     */
    public static boolean isFileValid(String localPath, String userMd5) {
        try {
            File file = new File(localPath);
            if (!file.exists() || file.length() == 0) {
                return false;
            }

            FileInfo info = CACHE.get(localPath);
            if (info == null) {
                info = new FileInfo();
                CACHE.put(localPath, info);
            }

            // If user provides MD5, always validate with MD5 (user MD5 is authoritative)
            if (!Strings.isNullOrEmpty(userMd5)) {
                if (Strings.isNullOrEmpty(info.localMd5)) {
                    info.localMd5 = calculateMd5(file);
                }
                boolean md5Match = userMd5.equalsIgnoreCase(info.localMd5);
                LOG.debug("User MD5 validation for {}: expected={}, actual={}, match={}",
                        localPath, userMd5, info.localMd5, md5Match);
                return md5Match;
            }

            // No user MD5, use cache expiry logic (optimization: don't check remote if cache valid)
            return !info.isExpired();

        } catch (Exception e) {
            LOG.warn("Failed to validate file {}: {}", localPath, e.getMessage());
            return false;
        }
    }

    /**
     * Check if we need to get remote ETag (optimization for cache)
     * Only check remote when:
     * 1. User provided MD5 (always need to validate against remote)
     * 2. No user MD5 but cache expired
     *
     * @param localPath local file path
     * @param userMd5 user provided MD5, can be null
     * @return true if need to check remote ETag
     */
    public static boolean needsRemoteCheck(String localPath, String userMd5) {
        // If user provided MD5, always need remote validation
        if (!Strings.isNullOrEmpty(userMd5)) {
            return true;
        }

        // If no user MD5, only check remote when cache expired
        FileInfo info = CACHE.get(localPath);
        return info == null || info.isExpired();
    }

    /**
     * Check if remote file has updates compared to cached info
     */
    public static boolean hasRemoteUpdate(String localPath, String remoteEtag, long remoteSize) {
        FileInfo info = CACHE.get(localPath);
        if (info == null) {
            return true; // No cache, assume updated
        }

        // Compare ETag if both exist
        if (!Strings.isNullOrEmpty(info.remoteEtag) && !Strings.isNullOrEmpty(remoteEtag)) {
            return !info.remoteEtag.equals(remoteEtag);
        }

        // Compare size if ETag not available
        return info.remoteSize > 0 && info.remoteSize != remoteSize;
    }

    /**
     * Update cache after successful download
     */
    public static void updateCache(String localPath, String remoteEtag, long remoteSize) {
        try {
            File file = new File(localPath);
            if (!file.exists()) {
                return;
            }

            FileInfo info = new FileInfo();
            info.remoteEtag = remoteEtag;
            info.remoteSize = remoteSize;
            info.localMd5 = calculateMd5(file); // Pre-calculate MD5 for future use

            CACHE.put(localPath, info);
            LOG.debug("Updated cache for: {}", localPath);

        } catch (Exception e) {
            LOG.warn("Failed to update cache for {}: {}", localPath, e.getMessage());
        }
    }

    /**
     * Clear cache entry
     */
    public static void clearCache(String localPath) {
        CACHE.remove(localPath);
    }

    /**
     * Clear all cache
     */
    public static void clearAllCache() {
        CACHE.clear();
    }

    /**
     * Calculate file MD5
     */
    private static String calculateMd5(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = fis.read(buffer)) != -1) {
                md5.update(buffer, 0, bytesRead);
            }

            byte[] hashBytes = md5.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (Exception e) {
            LOG.warn("Failed to calculate MD5 for {}: {}", file.getAbsolutePath(), e.getMessage());
            return "";
        }
    }
}
