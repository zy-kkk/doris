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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectorProperty;

import org.apache.iceberg.CatalogProperties;

import java.util.Map;

public class IcebergRestProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"iceberg.rest.uri"},
            description = "The uri of the iceberg rest catalog service.")
    private String icebergRestUri = "";

    @ConnectorProperty(names = {"iceberg.rest.security.type"},
            required = false,
            supported = false,
            description = "The security type of the iceberg rest catalog service.")
    private String icebergRestSecurityType = "none";

    @ConnectorProperty(names = {"iceberg.rest.prefix"},
            required = false,
            supported = false,
            description = "The prefix of the iceberg rest catalog service.")
    private String icebergRestPrefix = "";

    @ConnectorProperty(names = {"iceberg.rest.warehouse"},
            required = false,
            supported = false,
            description = "The warehouse of the iceberg rest catalog service.")
    private String icebergRestWarehouse = "";

    @ConnectorProperty(names = {"iceberg.rest.session"},
            required = false,
            supported = false,
            description = "The session name for the iceberg rest catalog service.")
    private String icebergRestSession = "";

    @ConnectorProperty(names = {"iceberg.rest.session-timeout"},
            required = false,
            supported = false,
            description = "The session timeout in milliseconds for the iceberg rest catalog service.")
    private String icebergRestSessionTimeout = "";

    @ConnectorProperty(names = {"iceberg.rest.vended-credentials-enabled"},
            required = false,
            supported = false,
            description = "Whether to enable vended credentials for the iceberg rest catalog service.")
    private String icebergRestVendedCredentialsEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.nested-namespace-enabled"},
            required = false,
            supported = false,
            description = "Whether to enable nested namespace for the iceberg rest catalog service.")
    private String icebergRestNestedNamespaceEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching"},
            required = false,
            supported = false,
            description = "Whether to enable case insensitive name matching for the iceberg rest catalog service.")
    private String icebergRestCaseInsensitiveNameMatching = "false";

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching.cache-ttl"},
            required = false,
            supported = false,
            description = "The cache TTL in milliseconds for case insensitive name matching.")
    private String icebergRestCaseInsensitiveNameMatchingCacheTtl = "";

    // OAuth2 properties
    private IcebergRestOAuth2Properties oauth2Properties;

    public IcebergRestProperties(Map<String, String> origProps) {
        super(Type.ICEBERG_REST, origProps);
        this.oauth2Properties = new IcebergRestOAuth2Properties(origProps);
    }

    @Override
    protected void checkRequiredProperties() {
        // Validate OAuth2 configuration if security is set to oauth2
        if ("oauth2".equalsIgnoreCase(icebergRestSecurityType)) {
            oauth2Properties.validate();
        }
    }

    public Map<String, String> toIcebergRestCatalogProperties() {
        Map<String, String> catalogProps = getMatchedProperties();

        // Set catalog type
        catalogProps.put("type", "rest");
        addPropertyIfSet(catalogProps, CatalogProperties.URI, icebergRestUri);
        addPropertyIfSet(catalogProps, CatalogProperties.WAREHOUSE_LOCATION, icebergRestWarehouse);
        addPropertyIfSet(catalogProps, "prefix", icebergRestPrefix);
        addPropertyIfSet(catalogProps, "session", icebergRestSession);
        addPropertyIfSet(catalogProps, CatalogProperties.AUTH_SESSION_TIMEOUT_MS, icebergRestSessionTimeout);
        addPropertyIfSet(catalogProps, "header.X-Iceberg-Access-Delegation", icebergRestVendedCredentialsEnabled);
        addPropertyIfSet(catalogProps, "nested-namespace-enabled", icebergRestNestedNamespaceEnabled);
        addPropertyIfSet(catalogProps, "case-insensitive-name-matching", icebergRestCaseInsensitiveNameMatching);
        addPropertyIfSet(catalogProps, "case-insensitive-name-matching.cache-ttl", icebergRestCaseInsensitiveNameMatchingCacheTtl);

        // Add OAuth2 properties if security is oauth2
        if ("oauth2".equalsIgnoreCase(icebergRestSecurityType)) {
            oauth2Properties.toIcebergRestCatalogProperties(catalogProps);
        }

        return catalogProps;
    }

    private void addPropertyIfSet(Map<String, String> catalogProps, String key, String value) {
        if (value != null && !value.isEmpty()) {
            catalogProps.put(key, value);
        }
    }

    public IcebergRestOAuth2Properties getOauth2Properties() {
        return oauth2Properties;
    }
}
