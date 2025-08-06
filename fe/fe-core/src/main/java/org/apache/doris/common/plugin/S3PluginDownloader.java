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
import java.util.zip.GZIPInputStream;

/**
 * S3PluginDownloader 是一个独立的 S3 下载器。
 * 职责单一：只负责从 S3 下载文件，不包含任何云配置逻辑。
 */
public class S3PluginDownloader implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(S3PluginDownloader.class);

    private final S3ObjStorage s3Storage;
    private final String bucket;

    /**
     * 构造函数：使用 CloudS3ConfigProvider.S3Config
     */
    public S3PluginDownloader(CloudS3ConfigProvider.S3Config s3Config) throws Exception {
        this.bucket = s3Config.bucket;
        this.s3Storage = createS3Storage(s3Config);
    }

    /**
     * 下载单个文件
     *
     * @param remoteKey S3 对象键（不含 bucket）
     * @param localPath 本地文件路径
     * @param expectedMd5 可选的 MD5 验证
     * @return 成功返回本地路径，失败返回空字符串
     */
    public String downloadFile(String remoteKey, String localPath, String expectedMd5) {
        try {
            String fullS3Path = "s3://" + bucket + "/" + remoteKey;

            // 检查是否需要下载
            if (!needsUpdate(fullS3Path, localPath)) {
                LOG.info("Local file {} is up to date", localPath);
                return localPath;
            }

            // 创建父目录
            createParentDirectory(localPath);

            // 执行下载
            File localFile = new File(localPath);
            Status status = s3Storage.getObject(fullS3Path, localFile);

            if (status != Status.OK) {
                LOG.warn("Failed to download {}: {}", fullS3Path, status.getErrMsg());
                return "";
            }

            // 验证 MD5
            if (!Strings.isNullOrEmpty(expectedMd5)) {
                String actualMd5 = calculateFileMD5(localFile);
                if (!expectedMd5.equalsIgnoreCase(actualMd5)) {
                    LOG.warn("MD5 mismatch for {}: expected={}, actual={}", localPath, expectedMd5, actualMd5);
                    localFile.delete();
                    return "";
                }
            }

            LOG.info("Successfully downloaded {} to {}", fullS3Path, localPath);
            return localPath;

        } catch (Exception e) {
            LOG.warn("Failed to download {}: {}", remoteKey, e.getMessage());
            return "";
        }
    }

    /**
     * 批量下载目录下的所有 .tar.gz 文件并解压
     */
    public String downloadAndExtractTarGzFiles(String remotePrefix, String localTargetDir) {
        try {
            String fullS3Path = "s3://" + bucket + "/" + remotePrefix + "/";

            // 创建本地目录
            createParentDirectory(localTargetDir + "/dummy");

            // 列出远程文件
            List<RemoteFile> remoteFiles = new ArrayList<>();
            Status listStatus = s3Storage.listFiles(fullS3Path, false, remoteFiles);

            if (listStatus != Status.OK) {
                LOG.warn("Failed to list files from {}: {}", fullS3Path, listStatus.getErrMsg());
                return "";
            }

            boolean hasDownload = false;
            for (RemoteFile remoteFile : remoteFiles) {
                if (remoteFile.isFile() && remoteFile.getName().endsWith(".tar.gz")) {
                    String fileName = Paths.get(remoteFile.getName()).getFileName().toString();
                    String localFilePath = Paths.get(localTargetDir, fileName).toString();

                    String result = downloadFile(remotePrefix + "/" + fileName, localFilePath, null);
                    if (!result.isEmpty()) {
                        // 解压并删除 tar.gz
                        if (extractTarGz(localFilePath, localTargetDir)) {
                            new File(localFilePath).delete();
                            hasDownload = true;
                        }
                    }
                }
            }

            return hasDownload ? localTargetDir : "";

        } catch (Exception e) {
            LOG.warn("Failed to download tar.gz files from {}: {}", remotePrefix, e.getMessage());
            return "";
        }
    }

    // ======================== Helper Methods ========================

    private S3ObjStorage createS3Storage(CloudS3ConfigProvider.S3Config config) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", config.endpoint);
        properties.put("s3.region", config.region);
        properties.put("s3.access_key", config.accessKey);
        properties.put("s3.secret_key", config.secretKey);

        S3Properties s3Properties = S3Properties.of(properties);
        return new S3ObjStorage(s3Properties);
    }

    private boolean needsUpdate(String remotePath, String localPath) {
        try {
            File localFile = new File(localPath);
            if (!localFile.exists()) {
                return true;
            }

            // 获取远程 ETag
            String remoteETag = s3Storage.getObjectETag(remotePath);
            if (Strings.isNullOrEmpty(remoteETag)) {
                LOG.warn("Cannot get remote ETag for {}, will download", remotePath);
                return true;
            }

            // 计算本地 MD5
            String localMD5 = calculateFileMD5(localFile);
            if (Strings.isNullOrEmpty(localMD5)) {
                return true;
            }

            // 比较 MD5 和 ETag
            String cleanETag = remoteETag.replaceAll("\"", "");
            return !localMD5.equalsIgnoreCase(cleanETag);

        } catch (Exception e) {
            LOG.warn("Error checking update for {}: {}", localPath, e.getMessage());
            return true;
        }
    }

    private void createParentDirectory(String filePath) throws Exception {
        Path localFilePath = Paths.get(filePath);
        Path parentDir = localFilePath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }
    }

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
            LOG.warn("Failed to extract {}: {}", tarGzFilePath, e.getMessage());
            return false;
        }
    }

    private static String calculateFileMD5(File file) {
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
            LOG.warn("Failed to compute MD5 for {}: {}", file.getAbsolutePath(), e.getMessage());
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (s3Storage != null) {
            s3Storage.close();
        }
    }
}
