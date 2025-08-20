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

#include "runtime/plugin/cloud_plugin_config_provider.h"

#include <fmt/format.h>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

Status CloudPluginConfigProvider::get_cloud_s3_config(
        std::unique_ptr<S3PluginDownloader::S3Config>* s3_config) {
    S3PluginDownloader::S3Config config("", "", "", "", "", "");
    Status status = _get_storage_vault_info(&config);
    RETURN_IF_ERROR(status);

    if (config.bucket.empty() || config.access_key.empty() || config.secret_key.empty()) {
        return Status::InvalidArgument(
                "Incomplete S3 configuration: bucket={}, access_key={}, secret_key={}",
                config.bucket, config.access_key.empty() ? "empty" : "***",
                config.secret_key.empty() ? "empty" : "***");
    }

    *s3_config = std::make_unique<S3PluginDownloader::S3Config>(
            config.endpoint, config.region, config.bucket, config.prefix, config.access_key,
            config.secret_key);
    return Status::OK();
}

Status CloudPluginConfigProvider::_get_storage_vault_info(S3PluginDownloader::S3Config* s3_config) {
    BaseStorageEngine& base_engine = ExecEnv::GetInstance()->storage_engine();
    CloudStorageEngine* cloud_engine = dynamic_cast<CloudStorageEngine*>(&base_engine);
    if (!cloud_engine) {
        return Status::NotFound("CloudStorageEngine not found, not in cloud mode");
    }

    cloud::CloudMetaMgr& meta_mgr = cloud_engine->meta_mgr();

    cloud::StorageVaultInfos vault_infos;
    bool enable_storage_vault = false;
    RETURN_IF_ERROR(meta_mgr.get_storage_vault_info(&vault_infos, &enable_storage_vault));

    if (vault_infos.empty()) {
        return Status::NotFound("No storage vault info available");
    }

    // Find first S3-compatible storage vault for plugin download
    for (const auto& [vault_id, vault_conf, path_format] : vault_infos) {
        if (const S3Conf* s3_conf = std::get_if<S3Conf>(&vault_conf)) {
            s3_config->endpoint = s3_conf->client_conf.endpoint;
            s3_config->region = s3_conf->client_conf.region;
            s3_config->bucket = s3_conf->bucket;
            s3_config->prefix = s3_conf->prefix;
            s3_config->access_key = s3_conf->client_conf.ak;
            s3_config->secret_key = s3_conf->client_conf.sk;

            LOG(INFO) << "Using storage vault for plugin download: " << vault_id
                      << ", prefix: " << s3_conf->prefix;
            return Status::OK();
        }
    }
    return Status::NotSupported("No S3-compatible storage found for plugin download");
}

} // namespace doris