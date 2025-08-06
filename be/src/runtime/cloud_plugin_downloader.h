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

#include <gen_cpp/cloud.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris::io {
class S3ObjStorageClient;
}

namespace doris {

/**
 * CloudPluginDownloader provides unified cloud plugin download functionality for Doris BE cloud mode.
 * 
 * This class is designed to be consistent with the FE implementation and provides:
 * - Downloads JDBC drivers, Java UDF, Trino connectors, and Hadoop configuration files
 * - Simple ETag-based update checking
 * - Compatible with S3-compatible storage (S3, OSS, COS, OBS)
 * - Automatic retry mechanism with graceful fallback
 */
class CloudPluginDownloader {
public:
    /**
     * Plugin types supported by the downloader (consistent with FE)
     */
    enum class PluginType {
        JDBC_DRIVERS, // JDBC driver jar files
        JAVA_UDF,     // Java UDF jar files
        CONNECTORS,   // Trino connector tar.gz packages
        HADOOP_CONF   // Hadoop configuration files
    };

    /**
     * Main entry point for downloading cloud plugins
     * 
     * @param plugin_type the type of plugin to download
     * @param plugin_name the name of the resource (file name, can be empty for CONNECTORS batch download)
     * @param local_target_path the local target path where the plugin should be saved
     * @return the local path if download succeeds, empty string if fails or not needed
     */
    static std::string download_plugin_if_needed(PluginType plugin_type,
                                                 const std::string& plugin_name,
                                                 const std::string& local_target_path);

private:
    static constexpr int MAX_RETRY_ATTEMPTS = 3;
    static constexpr int RETRY_DELAY_MS = 1000;

    /**
     * Download with retry mechanism
     */
    static std::string download_with_retry(PluginType plugin_type, const std::string& plugin_name,
                                           const std::string& local_target_path);

    /**
     * Core download logic for single plugin
     */
    static std::string do_download_single(PluginType plugin_type, const std::string& plugin_name,
                                          const std::string& local_target_path);

    /**
     * Download and extract all connectors (batch download for .tar.gz files)
     */
    static std::string do_download_connectors(const std::string& local_target_dir);

    /**
     * Get cloud storage configuration from CloudMetaMgr
     */
    static Status get_obj_store_info(cloud::ObjectStoreInfoPB* obj_info);

    /**
     * Build cloud storage path for plugin (consistent with FE logic)
     * Format: instanceId/plugins/pluginType/pluginName
     */
    static std::string build_cloud_path(PluginType plugin_type, const std::string& plugin_name);

    /**
     * Convert PluginType enum to directory name string
     */
    static std::string plugin_type_to_string(PluginType plugin_type);

    /**
     * Check if local file is up to date by comparing with remote
     * Uses ETag comparison for better accuracy
     */
    static bool is_local_file_up_to_date(const std::string& remote_path,
                                         const std::string& local_path,
                                         const cloud::ObjectStoreInfoPB& obj_info);

    /**
     * Get ETag of remote object
     */
    static std::string get_remote_etag(const cloud::ObjectStoreInfoPB& obj_info,
                                       const std::string& remote_path);

    /**
     * Extract tar.gz file to target directory
     */
    static Status extract_tar_gz(const std::string& tar_gz_path, const std::string& target_dir);

    /**
     * List remote files in directory (for connector batch download)
     */
    static Status list_remote_files(const cloud::ObjectStoreInfoPB& obj_info,
                                    const std::string& remote_prefix,
                                    std::vector<io::FileInfo>* files);

    /**
     * Create parent directory if not exists
     */
    static Status create_parent_directory(const std::string& file_path);

    /**
     * Execute actual download using S3ObjStorageClient
     */
    static Status execute_download(const cloud::ObjectStoreInfoPB& obj_info,
                                   const std::string& cloud_path, const std::string& local_path);

    /**
     * Create S3ObjStorageClient from cloud configuration
     */
    static std::shared_ptr<io::S3ObjStorageClient> create_s3_client(
            const cloud::ObjectStoreInfoPB& obj_info);
};

} // namespace doris