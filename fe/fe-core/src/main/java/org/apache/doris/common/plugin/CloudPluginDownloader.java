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
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.fs.obj.S3ObjStorage;
import org.apache.doris.fs.remote.RemoteFile;

import com.google.common.base.Strings;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

/**
 * CloudPluginDownloader provides unified cloud plugin download functionality for Doris cloud mode.
 * <p>
 * Features:
 * - Downloads JDBC drivers, Java UDF, Trino connectors, and Hadoop configuration files
 * - MD5-based incremental download with caching
 * - Supports batch download and extraction for connectors
 * - Compatible with S3-compatible storage (S3, OSS, COS, OBS)
 */
public class CloudPluginDownloader implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(CloudPluginDownloader.class);

    // MD5 cache for performance optimization
    private static final Map<String, MD5CacheEntry> md5Cache = new ConcurrentHashMap<>();

    private final Cloud.ObjectStoreInfoPB objStoreInfo;
    private final S3ObjStorage s3Storage;

    /**
     * Plugin types supported by the downloader
     */
    public enum PluginType {
        JDBC_DRIVERS("jdbc_drivers"),
        JAVA_UDF("java_udf"),
        CONNECTORS("connectors"),
        HADOOP_CONF("hadoop_conf");

        private final String directoryName;

        PluginType(String directoryName) {
            this.directoryName = directoryName;
        }

        public String getDirectoryName() {
            return directoryName;
        }
    }

    public CloudPluginDownloader() throws Exception {
        this.objStoreInfo = getDefaultStorageVaultInfo();
        if (objStoreInfo == null) {
            throw new Exception("Cannot get default storage vault info for cloud plugin downloader");
        }
        this.s3Storage = createS3StorageFromObjInfo(objStoreInfo);
    }

    /**
     * Main entry point for downloading cloud plugins
     *
     * @param pluginType the type of plugin to download
     * @param pluginName the name of the resource (can be null for CONNECTORS batch download)
     * @param localTargetPath the local target path where the plugin should be saved
     * @return the local path if download succeeds, empty string if fails or not needed
     */
    public static String downloadPluginIfNeeded(PluginType pluginType, String pluginName, String localTargetPath) {
        if (!Config.isCloudMode()) {
            return "";
        }

        try (CloudPluginDownloader downloader = new CloudPluginDownloader()) {
            if (pluginType == PluginType.CONNECTORS) {
                return downloader.downloadConnectorsDirectory(localTargetPath);
            } else {
                return downloader.downloadSinglePlugin(pluginType, pluginName, localTargetPath);
            }
        } catch (Exception e) {
            LOG.warn("Failed to download plugin from cloud: {}", e.getMessage());
            return "";
        }
    }

    // ======================== Core Download Methods ========================

    /**
     * Download a single plugin file with MD5 verification
     */
    private String downloadSinglePlugin(PluginType pluginType, String pluginName, String localTargetPath) {
        if (Strings.isNullOrEmpty(pluginName)) {
            LOG.warn("Plugin name is required for single file download");
            return "";
        }

        try {
            String cloudPath = buildCloudPath(pluginType, pluginName);
            String bucket = objStoreInfo.getBucket();
            String fullS3Path = "s3://" + bucket + "/" + cloudPath;

            // Check if download is needed (MD5 comparison)
            if (!needsUpdate(fullS3Path, localTargetPath)) {
                LOG.info("Local plugin {} is up to date, skipping download", localTargetPath);
                return localTargetPath;
            }

            // Create parent directory
            createParentDirectory(localTargetPath);

            // Download file
            File localFile = new File(localTargetPath);
            Status status = s3Storage.getObject(fullS3Path, localFile);

            if (status == Status.OK) {
                LOG.info("Successfully downloaded plugin from {} to {}", fullS3Path, localTargetPath);
                return localTargetPath;
            } else {
                LOG.warn("Failed to download plugin from {}: {}", fullS3Path, status.getErrMsg());
                return "";
            }

        } catch (Exception e) {
            LOG.warn("Failed to download single plugin {}: {}", pluginName, e.getMessage());
            return "";
        }
    }

    /**
     * Download all connector files and extract them to plugins/connectors/ directory
     */
    private String downloadConnectorsDirectory(String localTargetDir) {
        try {
            String cloudPath = buildCloudPath(PluginType.CONNECTORS, null);
            String bucket = objStoreInfo.getBucket();
            String fullS3Path = "s3://" + bucket + "/" + cloudPath + "/";

            // Create local directory
            createParentDirectory(localTargetDir);

            // List remote files
            List<RemoteFile> remoteFiles = new ArrayList<>();
            Status listStatus = s3Storage.listFiles(fullS3Path, false, remoteFiles);

            if (listStatus != Status.OK) {
                LOG.warn("Failed to list connector files from {}: {}", fullS3Path, listStatus.getErrMsg());
                return "";
            }

            boolean hasDownload = false;
            for (RemoteFile remoteFile : remoteFiles) {
                if (remoteFile.isFile() && remoteFile.getName().endsWith(".tar.gz")) {
                    String fileName = Paths.get(remoteFile.getName()).getFileName().toString();
                    String localFilePath = Paths.get(localTargetDir, fileName).toString();

                    // Build complete S3 path for the file
                    String fullRemoteFilePath = "s3://" + bucket + "/" + cloudPath + "/" + fileName;

                    // Check if download is needed
                    if (!needsUpdate(fullRemoteFilePath, localFilePath)) {
                        LOG.info("Connector {} is up to date, skipping", fileName);
                        continue;
                    }

                    // Create parent directory for the file
                    createParentDirectory(localFilePath);

                    // Download file
                    File localFile = new File(localFilePath);
                    LOG.info("Downloading connector from {} to {}", fullRemoteFilePath, localFilePath);

                    // Check if we have write permission
                    File parentDir = localFile.getParentFile();
                    if (!parentDir.canWrite()) {
                        LOG.warn("No write permission for directory: {}", parentDir.getAbsolutePath());
                    }

                    Status status = s3Storage.getObject(fullRemoteFilePath, localFile);

                    if (status == Status.OK) {
                        // Verify the downloaded file
                        if (!localFile.exists() || localFile.length() == 0) {
                            LOG.warn("Downloaded file is empty or missing: {}", localFilePath);
                            localFile.delete();
                            continue;
                        }

                        LOG.info("Downloaded connector: {} to {} (size: {} bytes)",
                                fileName, localFilePath, localFile.length());

                        // Extract tar.gz directly to the target directory (already should be plugins/connectors/)
                        String extractDir = localTargetDir;
                        if (extractTarGz(localFilePath, extractDir)) {
                            // Delete the tar.gz file after successful extraction
                            localFile.delete();
                            LOG.info("Extracted and cleaned up connector: {}", fileName);
                            hasDownload = true;
                        } else {
                            LOG.warn("Failed to extract connector: {}", fileName);
                            // Keep the tar.gz file for debugging
                        }
                    } else {
                        LOG.warn("Failed to download connector {}: {}", fileName, status.getErrMsg());
                        // Clean up any partially downloaded file
                        if (localFile.exists()) {
                            localFile.delete();
                        }
                    }
                }
            }

            return hasDownload ? localTargetDir : "";

        } catch (Exception e) {
            LOG.warn("Failed to download connectors directory: {}", e.getMessage());
            return "";
        }
    }

    // ======================== Helper Methods ========================

    /**
     * Extract tar.gz file to target directory
     */
    private boolean extractTarGz(String tarGzFilePath, String targetDir) {
        try {
            createParentDirectory(targetDir + "/dummy");

            try (FileInputStream fis = new FileInputStream(tarGzFilePath);
                    GZIPInputStream gis = new GZIPInputStream(fis);
                    TarArchiveInputStream tis = new TarArchiveInputStream(gis)) {

                TarArchiveEntry entry;
                while ((entry = tis.getNextTarEntry()) != null) {
                    if (!tis.canReadEntryData(entry)) {
                        continue;
                    }

                    File destFile = new File(targetDir, entry.getName());
                    if (entry.isDirectory()) {
                        destFile.mkdirs();
                    } else {
                        destFile.getParentFile().mkdirs();
                        try (FileOutputStream fos = new FileOutputStream(destFile)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = tis.read(buffer)) != -1) {
                                fos.write(buffer, 0, bytesRead);
                            }
                        }
                    }
                }
            }
            LOG.info("Successfully extracted {} to {}", tarGzFilePath, targetDir);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to extract tar.gz file {}: {}", tarGzFilePath, e.getMessage());
            return false;
        }
    }

    /**
     * Get default storage vault information for plugin storage
     */
    private static Cloud.ObjectStoreInfoPB getDefaultStorageVaultInfo() {
        try {
            // Get default storage vault info from StorageVaultMgr
            Pair<String, String> defaultVault = Env.getCurrentEnv().getStorageVaultMgr().getDefaultStorageVault();
            if (defaultVault == null) {
                LOG.warn("No default storage vault configured for plugin downloader");
                return null;
            }

            String vaultName = defaultVault.first;
            LOG.info("Using default storage vault for plugin download: {}", vaultName);

            // Use the dedicated RPC to get storage vault object store information
            Cloud.GetObjStoreInfoRequest request = Cloud.GetObjStoreInfoRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            try {
                Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance().getObjStoreInfo(request);

                if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                    // Find the default storage vault in the response
                    for (Cloud.StorageVaultPB vault : response.getStorageVaultList()) {
                        if (vaultName.equals(vault.getName())) {
                            if (vault.hasObjInfo()) {
                                LOG.info("Found storage vault {} with object store info", vaultName);
                                return vault.getObjInfo();
                            } else {
                                LOG.warn("Storage vault {} does not have object store info", vaultName);
                                return null;
                            }
                        }
                    }

                    // If not found by name, try to use the first available vault
                    if (!response.getStorageVaultList().isEmpty()) {
                        Cloud.StorageVaultPB firstVault = response.getStorageVaultList().get(0);
                        if (firstVault.hasObjInfo()) {
                            LOG.info("Using first available storage vault: {}", firstVault.getName());
                            return firstVault.getObjInfo();
                        }
                    }

                    LOG.warn("No suitable storage vault found for plugin download");
                    return null;
                } else {
                    LOG.warn("Failed to get storage vault info: {}", response.getStatus().getMsg());
                    return null;
                }
            } catch (Exception e) {
                LOG.warn("Failed to call getObjStoreInfo RPC: {}", e.getMessage());
                return null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get default storage vault info: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Build cloud storage path for plugin
     */
    private String buildCloudPath(PluginType pluginType, String pluginName) {
        String instanceId = ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId();
        if (Strings.isNullOrEmpty(instanceId)) {
            instanceId = String.valueOf(Config.cluster_id);
        }

        String path = instanceId + "/plugins/" + pluginType.getDirectoryName();
        if (!Strings.isNullOrEmpty(pluginName)) {
            path += "/" + pluginName;
        }
        return path;
    }

    /**
     * Create S3ObjStorage instance from ObjectStoreInfoPB configuration
     */
    private static S3ObjStorage createS3StorageFromObjInfo(Cloud.ObjectStoreInfoPB objInfo) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", objInfo.getEndpoint());
        properties.put("s3.region", objInfo.getRegion());
        properties.put("s3.access_key", objInfo.getAk());
        properties.put("s3.secret_key", objInfo.getSk());

        S3Properties s3Properties = S3Properties.of(properties);
        return new S3ObjStorage(s3Properties);
    }

    /**
     * Check if file needs to be updated based on MD5 comparison
     */
    private boolean needsUpdate(String remotePath, String localPath) {
        try {
            File localFile = new File(localPath);
            if (!localFile.exists()) {
                return true;
            }

            // Get remote ETag
            String remoteETag = s3Storage.getObjectETag(remotePath);
            if (Strings.isNullOrEmpty(remoteETag)) {
                LOG.warn("Cannot get remote file ETag for {}, will download", remotePath);
                return true;
            }

            // Calculate local MD5
            String localMD5 = calculateFileMD5(localFile);
            if (Strings.isNullOrEmpty(localMD5)) {
                return true;
            }

            // Compare MD5 and ETag
            String cleanETag = remoteETag.replaceAll("\"", "");
            boolean needUpdate = !localMD5.equalsIgnoreCase(cleanETag);

            if (needUpdate) {
                LOG.info("File {} needs update: local MD5={}, remote ETag={}",
                        localPath, localMD5, cleanETag);
            } else {
                LOG.debug("File {} is up to date: MD5={}", localPath, localMD5);
            }

            return needUpdate;

        } catch (Exception e) {
            LOG.warn("Error checking if update needed for {}: {}", localPath, e.getMessage());
            return true;
        }
    }

    /**
     * Create parent directory if not exists
     */
    private void createParentDirectory(String filePath) throws Exception {
        Path localFilePath = Paths.get(filePath);
        Path parentDir = localFilePath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }
    }

    // ======================== MD5 Cache Methods ========================

    /**
     * Calculate file MD5 with caching for performance
     */
    private static String calculateFileMD5(File file) {
        if (!file.exists() || !file.isFile()) {
            return null;
        }

        String absolutePath = file.getAbsolutePath();

        // Check cache first
        MD5CacheEntry cachedEntry = md5Cache.get(absolutePath);
        if (cachedEntry != null && cachedEntry.isValid(file)) {
            LOG.debug("Using cached MD5 for file {}: {}", absolutePath, cachedEntry.getMd5());
            return cachedEntry.getMd5();
        }

        // Cache miss, compute MD5
        long startTime = System.currentTimeMillis();
        String md5 = computeFileMD5(file);
        long duration = System.currentTimeMillis() - startTime;

        if (md5 != null) {
            // Update cache
            MD5CacheEntry newEntry = new MD5CacheEntry(md5, file.lastModified(), file.length());
            md5Cache.put(absolutePath, newEntry);

            LOG.info("Calculated MD5 for file {} ({}KB) in {}ms: {}",
                    absolutePath, file.length() / 1024, duration, md5);
        }

        return md5;
    }

    /**
     * Compute file MD5 hash
     */
    private static String computeFileMD5(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = fis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }

            byte[] hashBytes = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (Exception e) {
            LOG.warn("Failed to compute MD5 for file {}: {}", file.getAbsolutePath(), e.getMessage());
            return null;
        }
    }


    // ======================== Internal Classes ========================

    /**
     * MD5 cache entry with file metadata
     */
    private static class MD5CacheEntry {
        private final String md5;
        private final long lastModified;
        private final long fileSize;

        public MD5CacheEntry(String md5, long lastModified, long fileSize) {
            this.md5 = md5;
            this.lastModified = lastModified;
            this.fileSize = fileSize;
        }

        public String getMd5() {
            return md5;
        }

        public boolean isValid(File file) {
            return file.exists()
                    && file.lastModified() == lastModified
                    && file.length() == fileSize;
        }
    }

    @Override
    public void close() throws Exception {
        if (s3Storage != null) {
            s3Storage.close();
        }
    }
}
