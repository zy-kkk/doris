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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Strings;

/**
 * CloudPluginConfigProvider retrieves S3 authentication info and plugin base paths
 * from Doris cloud mode environment.
 * <p>
 * Responsibilities:
 * - Configuration retrieval only, no download logic
 * - Converts complex cloud mode configuration to simple S3 config objects
 * - Provides unified configuration interface, hiding implementation details
 * <p>
 * Uses StorageVaultMgr to get default storage configuration, more stable and reliable.
 */
public class CloudPluginConfigProvider {

    /**
     * Get S3 configuration from cloud mode
     *
     * @return S3 config object
     * @throws RuntimeException if configuration retrieval fails
     */
    public static S3PluginDownloader.S3Config getCloudS3Config() {
        Cloud.ObjectStoreInfoPB objInfo = getStorageVaultInfo();

        if (Strings.isNullOrEmpty(objInfo.getBucket())
                || Strings.isNullOrEmpty(objInfo.getAk())
                || Strings.isNullOrEmpty(objInfo.getSk())) {
            throw new RuntimeException("Incomplete S3 configuration: bucket=" + objInfo.getBucket()
                    + ", ak=" + (Strings.isNullOrEmpty(objInfo.getAk()) ? "empty" : "***")
                    + ", sk=" + (Strings.isNullOrEmpty(objInfo.getSk()) ? "empty" : "***"));
        }

        return new S3PluginDownloader.S3Config(
                objInfo.getEndpoint(),
                objInfo.getRegion(),
                objInfo.getBucket(),
                objInfo.getPrefix(),
                objInfo.getAk(),
                objInfo.getSk()
        );
    }

    // ======================== Private Helper Methods ========================

    /**
     * Get storage configuration info for both SaaS and Vault modes
     * Uses the existing MetaServiceProxy.getObjStoreInfo() method directly
     */
    private static Cloud.ObjectStoreInfoPB getStorageVaultInfo() {
        try {
            Cloud.GetObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());
            for (Cloud.StorageVaultPB vault : response.getStorageVaultList()) {
                if (vault.hasObjInfo()) {
                    return vault.getObjInfo();
                }
            }
            throw new RuntimeException("Failed to get obj store info");
        } catch (RpcException e) {
            throw new RuntimeException("Failed to get obj store info: " + e.getMessage());
        }
    }
}
