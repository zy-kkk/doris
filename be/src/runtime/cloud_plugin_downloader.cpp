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

#include "runtime/cloud_plugin_downloader.h"

#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <unistd.h>
#include <zlib.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_obj_storage_client.h"
#include "io/hdfs_util.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

std::string CloudPluginDownloader::download_plugin_if_needed(PluginType plugin_type,
                                                             const std::string& plugin_name,
                                                             const std::string& local_target_path) {
    if (!config::is_cloud_mode()) {
        return "";
    }

    try {
        return download_with_retry(plugin_type, plugin_name, local_target_path);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to download plugin " << plugin_type_to_string(plugin_type) << "/"
                     << plugin_name << " from cloud: " << e.what();
        return "";
    }
}

std::string CloudPluginDownloader::download_with_retry(PluginType plugin_type,
                                                       const std::string& plugin_name,
                                                       const std::string& local_target_path) {
    for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; ++attempt) {
        try {
            if (plugin_type == PluginType::CONNECTORS) {
                return do_download_connectors(local_target_path);
            } else {
                return do_download_single(plugin_type, plugin_name, local_target_path);
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Download attempt " << attempt << "/" << MAX_RETRY_ATTEMPTS
                         << " failed for " << plugin_type_to_string(plugin_type) << "/"
                         << plugin_name << ": " << e.what();

            if (attempt == MAX_RETRY_ATTEMPTS) {
                throw;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS * attempt));
        }
    }
    return "";
}

std::string CloudPluginDownloader::do_download_single(PluginType plugin_type,
                                                      const std::string& plugin_name,
                                                      const std::string& local_target_path) {
    if (plugin_name.empty()) {
        throw std::invalid_argument("Plugin name cannot be empty for single file download");
    }

    // Get cloud storage configuration
    cloud::ObjectStoreInfoPB obj_info;
    Status status = get_obj_store_info(&obj_info);
    if (!status.ok()) {
        throw std::runtime_error("Cannot get object store info: " + status.to_string());
    }

    // Build cloud path
    std::string cloud_path = build_cloud_path(plugin_type, plugin_name);

    // Check if local file is up to date
    if (is_local_file_up_to_date(cloud_path, local_target_path, obj_info)) {
        LOG(INFO) << "Local plugin " << local_target_path << " is up to date, skipping download";
        return local_target_path;
    }

    // Create parent directory
    status = create_parent_directory(local_target_path);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create parent directory: " + status.to_string());
    }

    // Execute download
    status = execute_download(obj_info, cloud_path, local_target_path);
    if (!status.ok()) {
        throw std::runtime_error("Download failed: " + status.to_string());
    }

    LOG(INFO) << "Successfully downloaded plugin from cloud to " << local_target_path;
    return local_target_path;
}

std::string CloudPluginDownloader::do_download_connectors(const std::string& local_target_dir) {
    // Get cloud storage configuration
    cloud::ObjectStoreInfoPB obj_info;
    Status status = get_obj_store_info(&obj_info);
    if (!status.ok()) {
        throw std::runtime_error("Cannot get object store info: " + status.to_string());
    }

    // Build cloud path for connectors directory
    std::string cloud_prefix = build_cloud_path(PluginType::CONNECTORS, "");

    // Create parent directory
    status = create_parent_directory(local_target_dir + "/dummy");
    if (!status.ok()) {
        throw std::runtime_error("Failed to create parent directory: " + status.to_string());
    }

    // List remote .tar.gz files
    std::vector<io::FileInfo> remote_files;
    status = list_remote_files(obj_info, cloud_prefix, &remote_files);
    if (!status.ok()) {
        throw std::runtime_error("Failed to list remote connector files: " + status.to_string());
    }

    bool has_download = false;
    for (const auto& file_info : remote_files) {
        if (file_info.is_file && file_info.file_name.ends_with(".tar.gz")) {
            std::filesystem::path file_path(file_info.file_name);
            std::string file_name = file_path.filename().string();
            std::string local_file_path = std::filesystem::path(local_target_dir) / file_name;

            // Check if download is needed
            if (is_local_file_up_to_date(file_info.file_name, local_file_path, obj_info)) {
                LOG(INFO) << "Connector " << file_name << " is up to date, skipping";
                continue;
            }

            // Download file
            status = execute_download(obj_info, file_info.file_name, local_file_path);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to download connector " << file_name << ": "
                             << status.to_string();
                continue;
            }

            LOG(INFO) << "Downloaded connector: " << file_name << " to " << local_file_path;

            // Extract tar.gz to plugins/connectors/ directory
            std::string extract_dir =
                    std::filesystem::path(local_target_dir) / "plugins" / "connectors";
            status = extract_tar_gz(local_file_path, extract_dir);
            if (status.ok()) {
                // Delete the tar.gz file after successful extraction
                std::filesystem::remove(local_file_path);
                LOG(INFO) << "Extracted and cleaned up connector: " << file_name;
                has_download = true;
            } else {
                LOG(WARNING) << "Failed to extract connector " << file_name << ": "
                             << status.to_string();
            }
        }
    }

    return has_download ? local_target_dir : "";
}

Status CloudPluginDownloader::get_obj_store_info(cloud::ObjectStoreInfoPB* obj_info) {
    // Get CloudStorageEngine and CloudMetaMgr
    BaseStorageEngine& base_engine = ExecEnv::GetInstance()->storage_engine();
    CloudStorageEngine* cloud_engine = dynamic_cast<CloudStorageEngine*>(&base_engine);
    if (!cloud_engine) {
        return Status::NotFound("CloudStorageEngine not found, not in cloud mode");
    }

    cloud::CloudMetaMgr& meta_mgr = cloud_engine->meta_mgr();

    // Get storage vault info
    cloud::StorageVaultInfos vault_infos;
    bool is_vault_mode = false;
    RETURN_IF_ERROR(meta_mgr.get_storage_vault_info(&vault_infos, &is_vault_mode));

    if (vault_infos.empty()) {
        return Status::NotFound("No storage vault info available");
    }

    // Extract S3 configuration from the first vault
    const auto& [vault_name, vault_conf, path_format] = vault_infos[0];

    if (const S3Conf* s3_conf = std::get_if<S3Conf>(&vault_conf)) {
        // Convert S3Conf to ObjectStoreInfoPB
        obj_info->set_bucket(s3_conf->bucket);
        obj_info->set_ak(s3_conf->client_conf.ak);
        obj_info->set_sk(s3_conf->client_conf.sk);
        // Note: token field not available in ObjectStoreInfoPB
        obj_info->set_endpoint(s3_conf->client_conf.endpoint);
        obj_info->set_region(s3_conf->client_conf.region);

        // Map provider type
        switch (s3_conf->client_conf.provider) {
        case io::ObjStorageType::AWS:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_S3);
            break;
        case io::ObjStorageType::OSS:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_OSS);
            break;
        case io::ObjStorageType::COS:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_COS);
            break;
        case io::ObjStorageType::OBS:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_OBS);
            break;
        case io::ObjStorageType::AZURE:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_AZURE);
            break;
        default:
            obj_info->set_provider(cloud::ObjectStoreInfoPB_Provider_S3);
            break;
        }

        return Status::OK();
    }

    return Status::NotSupported("Only S3-compatible storage is supported for plugin download");
}

std::string CloudPluginDownloader::build_cloud_path(PluginType plugin_type,
                                                    const std::string& plugin_name) {
    // Get instance ID (consistent with FE logic)
    // In FE: instanceId = Config.cluster_id == -1 ? getCloudInstanceId() : String.valueOf(Config.cluster_id)
    std::string instance_id;
    if (config::cluster_id == -1) {
        // Extract instance ID from cloud_unique_id format: "1:instanceId:randomString"
        std::string cloud_unique_id = config::cloud_unique_id;
        if (cloud_unique_id.empty()) {
            LOG(WARNING) << "cloud_unique_id is empty, using default instance id";
            instance_id = "default";
        } else {
            // Parse cloud_unique_id format: "1:instanceId:randomString"
            std::vector<std::string> parts;
            size_t start = 0;
            size_t end = cloud_unique_id.find(':');

            while (end != std::string::npos) {
                parts.push_back(cloud_unique_id.substr(start, end - start));
                start = end + 1;
                end = cloud_unique_id.find(':', start);
            }
            parts.push_back(cloud_unique_id.substr(start)); // Add last part

            if (parts.size() >= 2) {
                instance_id = parts[1]; // Use the middle part as instance ID
                LOG(INFO) << "Parsed instance_id from cloud_unique_id: " << instance_id;
            } else {
                // Fallback: use the entire cloud_unique_id if parsing fails
                instance_id = cloud_unique_id;
                LOG(WARNING)
                        << "Failed to parse cloud_unique_id, using entire value as instance_id: "
                        << instance_id;
            }
        }
    } else {
        // Use configured cluster_id directly
        instance_id = std::to_string(config::cluster_id);
        LOG(INFO) << "Using configured cluster_id as instance_id: " << instance_id;
    }

    // Build path: instanceId/plugins/pluginType[/pluginName] (consistent with FE logic)
    std::string path =
            fmt::format("{}/plugins/{}", instance_id, plugin_type_to_string(plugin_type));
    if (!plugin_name.empty()) {
        path += "/" + plugin_name;
    }
    return path;
}

std::string CloudPluginDownloader::plugin_type_to_string(PluginType plugin_type) {
    switch (plugin_type) {
    case PluginType::JDBC_DRIVERS:
        return "jdbc_drivers";
    case PluginType::JAVA_UDF:
        return "java_udf";
    case PluginType::CONNECTORS:
        return "connectors";
    case PluginType::HADOOP_CONF:
        return "hadoop_conf";
    default:
        return "unknown";
    }
}

bool CloudPluginDownloader::is_local_file_up_to_date(const std::string& remote_path,
                                                     const std::string& local_path,
                                                     const cloud::ObjectStoreInfoPB& obj_info) {
    try {
        // Check if local file exists
        std::filesystem::path file_path(local_path);
        if (!std::filesystem::exists(file_path) || std::filesystem::file_size(file_path) == 0) {
            return false;
        }

        // 获取远程文件的ETag
        std::string remote_etag = get_remote_etag(obj_info, remote_path);

        LOG(INFO) << "Local file " << local_path << " exists with size "
                  << std::filesystem::file_size(file_path) << ", remote ETag: " << remote_etag;
        if (remote_etag.empty()) {
            // 无法获取远程ETag，使用文件大小和时间作为fallback
            LOG(WARNING) << "Cannot get remote ETag for " << remote_path
                         << ", using file age fallback";

            auto local_file_time = std::filesystem::last_write_time(file_path);
            auto now = std::filesystem::file_time_type::clock::now();
            auto file_age = std::chrono::duration_cast<std::chrono::hours>(now - local_file_time);

            if (file_age.count() > 1) {
                LOG(INFO) << "Local file " << local_path
                          << " is older than 1 hour, may need update";
                return false;
            }
            return true;
        }

        // 实现ETag比较逻辑 - 类似FE的实现
        // 这里需要实现本地ETag缓存，但为简化先使用扩展属性或文件名存储
        std::string etag_cache_file = local_path + ".etag";
        std::string cached_etag;

        try {
            if (std::filesystem::exists(etag_cache_file)) {
                std::ifstream etag_file(etag_cache_file);
                std::getline(etag_file, cached_etag);
                etag_file.close();
            }

            if (cached_etag == remote_etag) {
                LOG(INFO) << "Local file " << local_path
                          << " is up to date (ETag match: " << remote_etag << ")";
                return true;
            } else {
                LOG(INFO) << "Local file " << local_path << " needs update - ETag mismatch. "
                          << "Local: " << cached_etag << ", Remote: " << remote_etag;
                return false;
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Error reading ETag cache for " << local_path << ": " << e.what();
            return false;
        }

    } catch (const std::exception& e) {
        LOG(WARNING) << "Error checking if local file is up to date for " << local_path << ": "
                     << e.what();
        return false;
    }
}

std::string CloudPluginDownloader::get_remote_etag(const cloud::ObjectStoreInfoPB& obj_info,
                                                   const std::string& remote_path) {
    try {
        // Create S3 client
        auto s3_client = create_s3_client(obj_info);
        if (!s3_client) {
            return "";
        }

        // Prepare head request
        io::ObjectStoragePathOptions opts;
        opts.bucket = obj_info.bucket();
        opts.key = remote_path;

        // Get object metadata
        auto head_response = s3_client->head_object(opts);
        if (head_response.resp.status.code != 0) {
            LOG(WARNING) << "Failed to get head info for remote object: " << remote_path;
            return "";
        }

        // 从head_response中提取ETag
        if (head_response.etag.empty()) {
            LOG(WARNING) << "No ETag available for remote object: " << remote_path;
            return "";
        }

        // 清理ETag中的引号（如果存在）
        std::string etag = head_response.etag;
        if (!etag.empty() && etag.front() == '"' && etag.back() == '"') {
            etag = etag.substr(1, etag.length() - 2);
        }

        return etag;

    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception getting remote ETag for " << remote_path << ": " << e.what();
        return "";
    }
}

Status CloudPluginDownloader::create_parent_directory(const std::string& file_path) {
    try {
        std::filesystem::path path(file_path);
        std::filesystem::path parent_dir = path.parent_path();

        if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
            std::filesystem::create_directories(parent_dir);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to create parent directory: {}", e.what());
    }
}

Status CloudPluginDownloader::execute_download(const cloud::ObjectStoreInfoPB& obj_info,
                                               const std::string& cloud_path,
                                               const std::string& local_path) {
    // Create S3 client
    auto s3_client = create_s3_client(obj_info);
    if (!s3_client) {
        return Status::InternalError("Failed to create S3 client");
    }

    // Prepare download parameters
    io::ObjectStoragePathOptions opts;
    opts.bucket = obj_info.bucket();
    opts.key = cloud_path;

    // Read file size first to prepare buffer
    auto head_response = s3_client->head_object(opts);
    if (head_response.resp.status.code != 0) {
        return Status::NotFound("Remote file not found: {}", cloud_path);
    }

    size_t file_size = head_response.file_size;
    if (file_size == 0) {
        return Status::InternalError("Remote file is empty: {}", cloud_path);
    }

    // Create local file and download
    try {
        std::ofstream local_file(local_path, std::ios::binary);
        if (!local_file.is_open()) {
            return Status::IOError("Cannot create local file: {}", local_path);
        }

        // Download in chunks
        constexpr size_t CHUNK_SIZE = 8 * 1024 * 1024; // 8MB chunks
        std::vector<char> buffer(std::min(file_size, CHUNK_SIZE));

        size_t offset = 0;
        while (offset < file_size) {
            size_t bytes_to_read = std::min(CHUNK_SIZE, file_size - offset);
            size_t bytes_read = 0;

            auto response =
                    s3_client->get_object(opts, buffer.data(), offset, bytes_to_read, &bytes_read);

            if (response.status.code != 0) {
                local_file.close();
                std::filesystem::remove(local_path); // Clean up partial file
                return Status::IOError("Failed to download chunk at offset {}: {}", offset,
                                       response.status.msg);
            }

            local_file.write(buffer.data(), bytes_read);
            if (!local_file.good()) {
                local_file.close();
                std::filesystem::remove(local_path); // Clean up partial file
                return Status::IOError("Failed to write to local file: {}", local_path);
            }

            offset += bytes_read;
        }

        local_file.close();

        // Verify file size
        if (std::filesystem::file_size(local_path) != file_size) {
            std::filesystem::remove(local_path); // Clean up incomplete file
            return Status::IOError("Downloaded file size mismatch");
        }

        // Save ETag cache for future incremental download checks
        if (!head_response.etag.empty()) {
            try {
                std::string etag_cache_file = local_path + ".etag";
                std::ofstream etag_file(etag_cache_file);
                std::string clean_etag = head_response.etag;
                if (!clean_etag.empty() && clean_etag.front() == '"' && clean_etag.back() == '"') {
                    clean_etag = clean_etag.substr(1, clean_etag.length() - 2);
                }
                etag_file << clean_etag << std::endl;
                etag_file.close();
                LOG(INFO) << "Saved ETag cache for " << local_path << ": " << clean_etag;
            } catch (const std::exception& e) {
                LOG(WARNING) << "Failed to save ETag cache for " << local_path << ": " << e.what();
            }
        }

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Download exception: {}", e.what());
    }
}

std::shared_ptr<io::S3ObjStorageClient> CloudPluginDownloader::create_s3_client(
        const cloud::ObjectStoreInfoPB& obj_info) {
    try {
        // Create S3Conf from ObjectStoreInfoPB
        S3Conf s3_conf = S3Conf::get_s3_conf(obj_info);

        // Create S3 client using existing factory
        auto obj_storage_client = S3ClientFactory::instance().create(s3_conf.client_conf);
        if (!obj_storage_client) {
            LOG(WARNING) << "Failed to create S3 client";
            return nullptr;
        }

        // Cast to S3ObjStorageClient
        auto s3_client = std::dynamic_pointer_cast<io::S3ObjStorageClient>(obj_storage_client);
        if (!s3_client) {
            LOG(WARNING) << "Failed to cast to S3ObjStorageClient";
            return nullptr;
        }

        return s3_client;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception creating S3 client: " << e.what();
        return nullptr;
    }
}

Status CloudPluginDownloader::extract_tar_gz(const std::string& tar_gz_path,
                                             const std::string& target_dir) {
    try {
        // Create target directory
        std::filesystem::create_directories(target_dir);

        // Open the .tar.gz file
        std::ifstream file(tar_gz_path, std::ios::binary);
        if (!file.is_open()) {
            return Status::IOError("Cannot open tar.gz file: {}", tar_gz_path);
        }

        // Read the entire file into memory
        std::vector<char> compressed_data((std::istreambuf_iterator<char>(file)),
                                          std::istreambuf_iterator<char>());
        file.close();

        // Decompress using zlib (gzip format)
        z_stream zs = {};
        if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) {
            return Status::IOError("Failed to initialize zlib decompression");
        }

        // Set up input buffer
        zs.next_in = reinterpret_cast<Bytef*>(compressed_data.data());
        zs.avail_in = compressed_data.size();

        // Decompress in chunks
        std::vector<char> decompressed_data;
        constexpr size_t CHUNK_SIZE = 16384;
        char out_buffer[CHUNK_SIZE];

        int ret;
        do {
            zs.next_out = reinterpret_cast<Bytef*>(out_buffer);
            zs.avail_out = CHUNK_SIZE;

            ret = inflate(&zs, Z_NO_FLUSH);
            if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
                inflateEnd(&zs);
                return Status::IOError("zlib decompression error: {}", ret);
            }

            size_t bytes_written = CHUNK_SIZE - zs.avail_out;
            decompressed_data.insert(decompressed_data.end(), out_buffer,
                                     out_buffer + bytes_written);
        } while (ret != Z_STREAM_END);

        inflateEnd(&zs);

        // Simple tar parsing (basic implementation for standard tar format)
        const char* tar_data = decompressed_data.data();
        size_t offset = 0;
        size_t tar_size = decompressed_data.size();

        while (offset + 512 <= tar_size) {
            // Read tar header (512 bytes)
            const char* header = tar_data + offset;

            // Check if this is the end of the archive (empty header)
            if (header[0] == '\0') {
                break;
            }

            // Extract file name
            std::string file_name(header, std::find(header, header + 100, '\0'));
            if (file_name.empty()) {
                break;
            }

            // Extract file size (octal format at offset 124)
            std::string size_str(header + 124, 12);
            size_t file_size = 0;
            try {
                file_size = std::stoull(size_str, nullptr, 8);
            } catch (...) {
                // Skip invalid entries
                offset += 512;
                continue;
            }

            // Check file type (offset 156)
            char type_flag = header[156];

            offset += 512; // Move past header

            if (type_flag == '0' || type_flag == '\0') {
                // Regular file
                std::filesystem::path full_path = std::filesystem::path(target_dir) / file_name;
                std::filesystem::create_directories(full_path.parent_path());

                std::ofstream out_file(full_path, std::ios::binary);
                if (out_file.is_open() && offset + file_size <= tar_size) {
                    out_file.write(tar_data + offset, file_size);
                    out_file.close();
                }
            } else if (type_flag == '5') {
                // Directory
                std::filesystem::path dir_path = std::filesystem::path(target_dir) / file_name;
                std::filesystem::create_directories(dir_path);
            }

            // Move to next entry (files are padded to 512-byte boundaries)
            offset += (file_size + 511) & ~511;
        }

        LOG(INFO) << "Successfully extracted " << tar_gz_path << " to " << target_dir;
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError("Failed to extract tar.gz file {}: {}", tar_gz_path, e.what());
    }
}

Status CloudPluginDownloader::list_remote_files(const cloud::ObjectStoreInfoPB& obj_info,
                                                const std::string& remote_prefix,
                                                std::vector<io::FileInfo>* files) {
    // Create S3 client
    auto s3_client = create_s3_client(obj_info);
    if (!s3_client) {
        return Status::InternalError("Failed to create S3 client");
    }

    // Prepare list parameters
    io::ObjectStoragePathOptions opts;
    opts.bucket = obj_info.bucket();
    opts.prefix = remote_prefix;

    // List objects
    auto response = s3_client->list_objects(opts, files);
    if (response.status.code != 0) {
        return Status::IOError("Failed to list remote files: {}", response.status.msg);
    }

    return Status::OK();
}

} // namespace doris