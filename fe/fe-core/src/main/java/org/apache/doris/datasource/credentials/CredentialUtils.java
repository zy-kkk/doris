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

package org.apache.doris.datasource.credentials;

import org.apache.doris.datasource.property.storage.StorageProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Credential operations
 */
public class CredentialUtils {

    /**
     * Extract backend properties from StorageProperties map
     * Reference: CatalogProperty.getBackendStorageProperties()
     *
     * @param storagePropertiesMap Map of storage properties
     * @return Backend properties with null values filtered out
     */
    public static Map<String, String> getBackendPropertiesFromStorageMap(
            Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        Map<String, String> result = new HashMap<>();
        for (StorageProperties sp : storagePropertiesMap.values()) {
            Map<String, String> backendProps = sp.getBackendConfigProperties();
            // the backend property's value can not be null, because it will be serialized to thrift,
            // which does not support null value.
            backendProps.entrySet().stream().filter(e -> e.getValue() != null)
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        }
        return result;
    }
}
