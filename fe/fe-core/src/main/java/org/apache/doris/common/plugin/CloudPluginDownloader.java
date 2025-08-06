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

import org.apache.doris.common.Config;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudPluginDownloader 是云模式下插件下载的简洁统一入口。
 * <p>
 * 架构设计：
 * 1. CloudS3ConfigProvider 获取云模式下的 S3 认证信息和基础路径
 * 2. S3PluginDownloader 执行实际的下载操作
 * 3. CloudPluginDownloader 组合以上两者，提供简洁的 API
 * <p>
 * 支持的插件类型：JDBC 驱动、Java UDF、Connectors、Hadoop 配置
 */
public class CloudPluginDownloader {
    private static final Logger LOG = LogManager.getLogger(CloudPluginDownloader.class);

    /**
     * 插件类型枚举
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

    /**
     * 下载插件（主入口方法，向后兼容）
     *
     * @param pluginType 插件类型
     * @param pluginName 插件名称（CONNECTORS 批量下载时可为 null）
     * @param localTargetPath 本地目标路径
     * @return 成功返回本地路径，失败返回空字符串
     */
    public static String downloadPluginIfNeeded(PluginType pluginType, String pluginName, String localTargetPath) {
        if (!Config.isCloudMode()) {
            return "";
        }

        try {
            // 1. 获取云 S3 配置
            CloudS3ConfigProvider.S3Config s3Config = CloudS3ConfigProvider.getCloudS3Config();
            if (s3Config == null) {
                LOG.warn("Cannot get cloud S3 configuration");
                return "";
            }

            // 2. 使用下载器下载
            try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
                if (pluginType == PluginType.CONNECTORS && Strings.isNullOrEmpty(pluginName)) {
                    // 批量下载 connectors
                    String remotePath = buildRemotePath(s3Config.basePath, pluginType, null);
                    return downloader.downloadAndExtractTarGzFiles(remotePath, localTargetPath);
                } else {
                    // 单文件下载
                    String remotePath = buildRemotePath(s3Config.basePath, pluginType, pluginName);
                    return downloader.downloadFile(remotePath, localTargetPath, null);
                }
            }

        } catch (Exception e) {
            LOG.warn("Failed to download plugin {} from cloud: {}", pluginName, e.getMessage());
            return "";
        }
    }

    // ======================== 便利方法 ========================

    /**
     * 下载 JDBC 驱动
     */
    public static String downloadJdbcDriver(String driverName, String localPath) {
        return downloadPluginIfNeeded(PluginType.JDBC_DRIVERS, driverName, localPath);
    }

    /**
     * 下载 Java UDF
     */
    public static String downloadJavaUdf(String jarName, String localPath) {
        return downloadPluginIfNeeded(PluginType.JAVA_UDF, jarName, localPath);
    }

    /**
     * 批量下载并解压所有 Connectors
     */
    public static String downloadAllConnectors(String localDir) {
        return downloadPluginIfNeeded(PluginType.CONNECTORS, null, localDir);
    }

    /**
     * 下载 Hadoop 配置
     */
    public static String downloadHadoopConfig(String configName, String localPath) {
        return downloadPluginIfNeeded(PluginType.HADOOP_CONF, configName, localPath);
    }

    // ======================== 非云模式支持（预留接口）========================

    /**
     * 手动配置下载（预留接口，暂不实现具体逻辑）
     * 用于非云模式下，用户自己提供 S3 认证信息进行下载
     *
     * @param endpoint S3 端点
     * @param region 区域
     * @param bucket 桶名
     * @param accessKey 访问密钥
     * @param secretKey 秘密密钥
     * @param remoteKey 远程文件键
     * @param localPath 本地路径
     * @param expectedMd5 可选的 MD5 验证
     * @return 成功返回本地路径，失败返回空字符串
     */
    public static String downloadWithManualConfig(String endpoint, String region, String bucket,
            String accessKey, String secretKey,
            String remoteKey, String localPath,
            String expectedMd5) {
        // 预留接口：用户可以手动提供 S3 配置进行下载
        // 当前版本暂未实现，返回空字符串
        LOG.info("Manual config download is not implemented yet");
        return "";
    }

    // ======================== Helper Methods ========================

    /**
     * 构建远程路径：basePath/plugins/pluginType/pluginName
     */
    private static String buildRemotePath(String basePath, PluginType pluginType, String pluginName) {
        String path = basePath + "/plugins/" + pluginType.getDirectoryName();
        if (!Strings.isNullOrEmpty(pluginName)) {
            path += "/" + pluginName;
        }
        return path;
    }

    /**
     * 检查是否支持云插件下载
     */
    public static boolean isCloudPluginSupported() {
        return Config.isCloudMode() && CloudS3ConfigProvider.getCloudS3Config() != null;
    }
}
