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

import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.fs.obj.S3ObjStorage;

import com.google.common.base.Strings;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * S3PluginDownloader is an independent, generic S3 downloader.
 * <p>
 * Design principles:
 * 1. Single responsibility: Only downloads files from S3, no business logic
 * 2. Complete decoupling: No dependency on cloud mode or Doris-specific configuration
 * 3. Reusable: Supports both cloud mode auto-configuration and manual S3 parameters
 * 4. Features: Single file download, batch directory download, MD5 verification, retry mechanism
 */
public class S3PluginDownloader implements AutoCloseable {

    private final S3ObjStorage s3Storage;

    /**
     * S3 configuration info (completely independent, no dependency on any Doris internal types)
     */
    public static class S3Config {
        public final String endpoint;
        public final String region;
        public final String bucket;
        public final String accessKey;
        public final String secretKey;

        public S3Config(String endpoint, String region, String bucket,
                String accessKey, String secretKey) {
            this.endpoint = endpoint;
            this.region = region;
            this.bucket = bucket;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @Override
        public String toString() {
            return String.format("S3Config{endpoint='%s', region='%s', bucket='%s', accessKey='%s'}",
                    endpoint, region, bucket, accessKey != null ? "***" : "null");
        }
    }

    /**
     * Constructor - pass in S3 configuration
     */
    public S3PluginDownloader(S3Config config) {
        this.s3Storage = createS3Storage(config);
    }

    // ======================== Core Download Methods ========================

    /**
     * Download single file (supports MD5 verification and retry)
     *
     * @param remoteS3Path complete S3 path like "s3://bucket/path/to/file.jar"
     * @param localPath local target file path
     * @param expectedMd5 optional MD5 verification value, null to skip verification
     * @return returns local file path on success
     * @throws RuntimeException if download fails after all retries
     */
    public String downloadFile(String remoteS3Path, String localPath, String expectedMd5) {
        // Synchronize on file path to prevent concurrent downloads of the same file
        synchronized (localPath.intern()) {
            // Check if download is needed (within lock to ensure consistency)
            if (PluginFileCache.isFileValid(localPath, expectedMd5)) {
                return localPath; // The local file is valid and returns directly
            }
            // Execute the download
            try {
                return executeDownload(remoteS3Path, localPath, expectedMd5);
            } catch (Exception e) {
                throw new RuntimeException("Download failed: " + e.getMessage());
            }
        }
    }

    private String executeDownload(String remoteS3Path, String localPath, String expectedMd5) throws Exception {
        // Create a parent directory
        Path parentDir = Paths.get(localPath).getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // Delete existing file if present (to ensure clean download)
        File localFile = new File(localPath);
        if (localFile.exists()) {
            if (!localFile.delete()) {
                throw new RuntimeException("Failed to delete existing file: " + localPath);
            }
        }

        // Perform the download
        Status status = s3Storage.getObject(remoteS3Path, localFile);
        if (status != Status.OK) {
            throw new RuntimeException("Download failed: " + status.getErrMsg());
        }

        // MD5 checksum cache update
        String actualMd5 = calculateFileMD5(localFile);

        // If the user provides MD5, the consistency must be verified
        if (!Strings.isNullOrEmpty(expectedMd5)) {
            if (!expectedMd5.equalsIgnoreCase(actualMd5)) {
                localFile.delete(); // Delete invalid files
                throw new RuntimeException(String.format(
                        "MD5 mismatch: expected=%s, actual=%s", expectedMd5, actualMd5));
            }
        }
        // Update cache
        updateCacheAfterDownload(localPath, actualMd5);

        return localPath;
    }

    /**
     * Update cache after download - save MD5 and file size
     *
     * @param localPath local file path
     * @param actualMd5 calculated MD5 hash of the downloaded file
     */
    private void updateCacheAfterDownload(String localPath, String actualMd5) {
        try {
            File localFile = new File(localPath);
            long fileSize = localFile.exists() ? localFile.length() : 0;
            PluginFileCache.updateCache(localPath, actualMd5, fileSize);
        } catch (Exception e) {
            // Ignore cache update failures - not critical
        }
    }

    private String calculateFileMD5(File file) {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead;
            do {
                bytesRead = inputStream.read(buf);
                if (bytesRead < 0) {
                    break;
                }
                digest.update(buf, 0, bytesRead);
            } while (true);

            return Hex.encodeHexString(digest.digest());
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate MD5 for file " + file.getAbsolutePath(), e);
        }
    }

    private S3ObjStorage createS3Storage(S3Config config) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", config.endpoint);
        properties.put("s3.region", config.region);
        properties.put("s3.access_key", config.accessKey);
        properties.put("s3.secret_key", config.secretKey);

        S3Properties s3Properties = S3Properties.of(properties);
        return new S3ObjStorage(s3Properties);
    }

    @Override
    public void close() throws Exception {
        if (s3Storage != null) {
            s3Storage.close();
        }
    }
}
