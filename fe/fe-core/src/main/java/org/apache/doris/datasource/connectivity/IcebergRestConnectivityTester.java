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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.IcebergRestProperties;

import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Map;

/**
 * Connectivity tester for Iceberg REST catalog.
 * Tests connectivity by calling GET /v1/config endpoint.
 */
public class IcebergRestConnectivityTester implements MetaConnectivityTester {
    private final IcebergRestProperties properties;

    public IcebergRestConnectivityTester(IcebergRestProperties properties) {
        this.properties = properties;
    }

    @Override
    public void testConnection() throws Exception {
        Map<String, String> restProps = properties.getIcebergRestCatalogProperties();

        // Create a temporary catalog and test initialization
        // The initialize() method will call fetchConfig internally, which tests connectivity
        try (RESTSessionCatalog catalog = new RESTSessionCatalog()) {
            catalog.initialize("connectivity-test", restProps);
        }
    }
}
