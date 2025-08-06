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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudS3ConfigProvider 专门负责从云模式环境中获取 S3 认证信息和插件基础路径。
 * 职责单一：仅处理云模式配置获取，不包含下载逻辑。
 */
public class CloudS3ConfigProvider {
    private static final Logger LOG = LogManager.getLogger(CloudS3ConfigProvider.class);

    /**
     * S3 配置信息
     */
    public static class S3Config {
        public final String endpoint;
        public final String region;
        public final String bucket;
        public final String accessKey;
        public final String secretKey;
        public final String basePath;  // 插件基础路径，如 "948698861"

        public S3Config(String endpoint, String region, String bucket,
                String accessKey, String secretKey, String basePath) {
            this.endpoint = endpoint;
            this.region = region;
            this.bucket = bucket;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.basePath = basePath;
        }
    }

    /**
     * 获取云模式下的 S3 配置信息
     *
     * @return S3Config 如果成功，null 如果失败
     */
    public static S3Config getCloudS3Config() {
        if (!Config.isCloudMode()) {
            return null;
        }

        try {
            // 1. 获取默认存储库信息
            Pair<String, String> defaultVault = Env.getCurrentEnv().getStorageVaultMgr().getDefaultStorageVault();
            if (defaultVault == null) {
                LOG.warn("No default storage vault configured");
                return null;
            }

            // 2. 通过 RPC 获取存储库的对象存储信息
            Cloud.ObjectStoreInfoPB objStoreInfo = getStorageVaultObjectStoreInfo(defaultVault.first);
            if (objStoreInfo == null) {
                return null;
            }

            // 3. 获取实例 ID 作为基础路径
            String instanceId = getInstanceId();

            return new S3Config(
                    objStoreInfo.getEndpoint(),
                    objStoreInfo.getRegion(),
                    objStoreInfo.getBucket(),
                    objStoreInfo.getAk(),
                    objStoreInfo.getSk(),
                    instanceId
            );

        } catch (Exception e) {
            LOG.warn("Failed to get cloud S3 config: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 从 MetaService 获取存储库的对象存储信息
     */
    private static Cloud.ObjectStoreInfoPB getStorageVaultObjectStoreInfo(String vaultName) {
        try {
            Cloud.GetObjStoreInfoRequest request = Cloud.GetObjStoreInfoRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance().getObjStoreInfo(request);

            if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                // 查找指定的存储库
                for (Cloud.StorageVaultPB vault : response.getStorageVaultList()) {
                    if (vaultName.equals(vault.getName()) && vault.hasObjInfo()) {
                        LOG.info("Found storage vault {} with object store info", vaultName);
                        return vault.getObjInfo();
                    }
                }

                // 如果未找到，使用第一个可用的存储库
                if (!response.getStorageVaultList().isEmpty()) {
                    Cloud.StorageVaultPB firstVault = response.getStorageVaultList().get(0);
                    if (firstVault.hasObjInfo()) {
                        LOG.info("Using first available storage vault: {}", firstVault.getName());
                        return firstVault.getObjInfo();
                    }
                }
            }

            LOG.warn("No suitable storage vault found");
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to call getObjStoreInfo RPC: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 获取实例 ID（与 FE 逻辑保持一致）
     */
    private static String getInstanceId() {
        try {
            String instanceId = ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId();
            if (!Strings.isNullOrEmpty(instanceId)) {
                return instanceId;
            }
        } catch (Exception e) {
            LOG.debug("Failed to get instance ID from CloudEnv: {}", e.getMessage());
        }

        // 回退到 cluster_id
        if (Config.cluster_id != -1) {
            return String.valueOf(Config.cluster_id);
        }

        return "default";
    }
}
