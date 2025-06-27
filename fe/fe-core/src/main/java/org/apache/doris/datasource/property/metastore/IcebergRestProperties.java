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

import java.util.HashMap;
import java.util.Map;

public class IcebergRestProperties extends MetastoreProperties {

    // ==================== 基础连接属性 ====================
    @ConnectorProperty(names = {"iceberg.rest.uri"},
            description = "The URI of the Iceberg REST catalog service.")
    private String icebergRestUri = "";

    @ConnectorProperty(names = {"iceberg.rest.prefix"},
            required = false,
            supported = false,
            description = "The prefix for the resource path to use with the REST catalog server.")
    private String icebergRestPrefix = "";

    @ConnectorProperty(names = {"iceberg.rest.warehouse"},
            required = false,
            supported = false,
            description = "The warehouse location/identifier to use with the REST catalog server.")
    private String icebergRestWarehouse = "";

    // ==================== 安全认证属性 ====================
    @ConnectorProperty(names = {"iceberg.rest.security"},
            required = false,
            supported = false,
            description = "The type of security to use (NONE, OAUTH2, SIGV4); default is NONE.")
    private String icebergRestSecurityType = "NONE";

    // OAuth2 相关属性
    @ConnectorProperty(names = {"iceberg.rest.oauth2.token"},
            required = false,
            description = "The bearer token for OAuth2 authentication.")
    private String oauth2Token = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.credential"},
            required = false,
            description = "The credential to exchange for a token in OAuth2 client credentials flow.")
    private String oauth2Credential = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.scope"},
            required = false,
            description = "The scope for OAuth2 authentication.")
    private String oauth2Scope = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.server-uri"},
            required = false,
            description = "The endpoint to retrieve access token from OAuth2 server.")
    private String oauth2ServerUri = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token-refresh-enabled"},
            required = false,
            description = "Controls whether a token should be refreshed.")
    private boolean oauth2TokenRefreshEnabled = true;

    // SigV4 相关属性
    @ConnectorProperty(names = {"iceberg.rest.sigv4-enabled"},
            required = false,
            description = "Enable AWS Signature version 4 (SigV4).")
    private boolean sigV4Enabled = false;

    @ConnectorProperty(names = {"iceberg.rest.signing-name"},
            required = false,
            description = "AWS SigV4 signing service name.")
    private String signingName = "execute-api";

    @ConnectorProperty(names = {"iceberg.rest.aws.region"},
            required = false,
            description = "AWS region for SigV4 signing.")
    private String awsRegion = "";

    @ConnectorProperty(names = {"iceberg.rest.aws.access-key"},
            required = false,
            description = "AWS access key for SigV4 signing.")
    private String awsAccessKey = "";

    @ConnectorProperty(names = {"iceberg.rest.aws.secret-key"},
            required = false,
            description = "AWS secret key for SigV4 signing.")
    private String awsSecretKey = "";

    // ==================== 会话管理属性 ====================
    @ConnectorProperty(names = {"iceberg.rest.session"},
            required = false,
            description = "Session information included when communicating with the REST catalog (NONE, USER).")
    private String icebergRestSession = "NONE";

    @ConnectorProperty(names = {"iceberg.rest.session-timeout"},
            required = false,
            description = "Duration to keep authentication session in cache (in seconds).")
    private int icebergRestSessionTimeout = 3600;

    // ==================== 功能特性属性 ====================
    @ConnectorProperty(names = {"iceberg.rest.vended-credentials-enabled"},
            required = false,
            description = "Use credentials provided by the REST backend for file system access.")
    private boolean vendedCredentialsEnabled = false;

    @ConnectorProperty(names = {"iceberg.rest.view-endpoints-enabled"},
            required = false,
            description = "Enable view endpoints support.")
    private boolean viewEndpointsEnabled = true;

    @ConnectorProperty(names = {"iceberg.rest.nested-namespace-enabled"},
            required = false,
            description = "Support querying objects under nested namespace.")
    private boolean nestedNamespaceEnabled = false;

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching"},
            required = false,
            description = "Match namespace, table, and view names case insensitively.")
    private boolean caseInsensitiveNameMatching = false;

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching.cache-ttl"},
            required = false,
            description = "Duration for which case-insensitive names are cached (in seconds).")
    private int caseInsensitiveNameMatchingCacheTtl = 60;

    // ==================== 高级配置属性 ====================
    @ConnectorProperty(names = {"iceberg.rest.http.connect-timeout"},
            required = false,
            description = "HTTP connection timeout in milliseconds.")
    private int httpConnectTimeout = 30000;

    @ConnectorProperty(names = {"iceberg.rest.http.read-timeout"},
            required = false,
            description = "HTTP read timeout in milliseconds.")
    private int httpReadTimeout = 60000;

    @ConnectorProperty(names = {"iceberg.rest.http.max-retries"},
            required = false,
            description = "Maximum number of HTTP retries.")
    private int httpMaxRetries = 3;

    @ConnectorProperty(names = {"iceberg.rest.http.user-agent"},
            required = false,
            description = "User agent string for HTTP requests.")
    private String httpUserAgent = "Doris-Iceberg-REST-Client";


    public IcebergRestProperties(Map<String, String> origProps) {
        super(Type.ICEBERG_REST, origProps);
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        
        // 检查必需的 URI
        if (icebergRestUri == null || icebergRestUri.trim().isEmpty()) {
            throw new IllegalArgumentException("Iceberg REST URI is required.");
        }

        // 检查 OAuth2 相关配置的完整性
        if ("OAUTH2".equalsIgnoreCase(icebergRestSecurityType)) {
            if ((oauth2Token == null || oauth2Token.trim().isEmpty())
                && (oauth2ServerUri == null || oauth2ServerUri.trim().isEmpty()
                    || oauth2Credential == null || oauth2Credential.trim().isEmpty())) {
                throw new IllegalArgumentException(
                    "OAuth2 authentication requires either oauth2.token or oauth2.server-uri with oauth2.credential");
            }
        }

        // 检查 SigV4 相关配置的完整性
        if (sigV4Enabled && "SIGV4".equalsIgnoreCase(icebergRestSecurityType)) {
            if ((awsAccessKey == null || awsAccessKey.trim().isEmpty())
                || (awsSecretKey == null || awsSecretKey.trim().isEmpty())
                || (awsRegion == null || awsRegion.trim().isEmpty())) {
                throw new IllegalArgumentException(
                    "SigV4 authentication requires aws.access-key, aws.secret-key, and aws.region");
            }
        }
    }

    public void toIcebergRestCatalogProperties(Map<String, String> catalogProps) {
        // 基础配置
        catalogProps.put("type", "rest");
        catalogProps.put("uri", icebergRestUri);

        // 可选的基础属性
        if (icebergRestPrefix != null && !icebergRestPrefix.trim().isEmpty()) {
            catalogProps.put("prefix", icebergRestPrefix);
        }
        if (icebergRestWarehouse != null && !icebergRestWarehouse.trim().isEmpty()) {
            catalogProps.put("warehouse", icebergRestWarehouse);
        }

        // 安全配置
        if (icebergRestSecurityType != null && !icebergRestSecurityType.trim().isEmpty()) {
            catalogProps.put("rest-catalog-config.security", icebergRestSecurityType);
        }

        // OAuth2 配置
        if ("OAUTH2".equalsIgnoreCase(icebergRestSecurityType)) {
            if (oauth2Token != null && !oauth2Token.trim().isEmpty()) {
                catalogProps.put("token", oauth2Token);
            }
            if (oauth2Credential != null && !oauth2Credential.trim().isEmpty()) {
                catalogProps.put("credential", oauth2Credential);
            }
            if (oauth2Scope != null && !oauth2Scope.trim().isEmpty()) {
                catalogProps.put("scope", oauth2Scope);
            }
            if (oauth2ServerUri != null && !oauth2ServerUri.trim().isEmpty()) {
                catalogProps.put("oauth2-server-uri", oauth2ServerUri);
            }
            catalogProps.put("oauth2.token-refresh-enabled", String.valueOf(oauth2TokenRefreshEnabled));
        }

        // SigV4 配置
        if (sigV4Enabled) {
            catalogProps.put("rest.sigv4-enabled", String.valueOf(sigV4Enabled));
            if (signingName != null && !signingName.trim().isEmpty()) {
                catalogProps.put("rest.signing-name", signingName);
            }
            if (awsRegion != null && !awsRegion.trim().isEmpty()) {
                catalogProps.put("rest.signing-region", awsRegion);
            }
        }

        // 会话管理配置
        if (icebergRestSession != null && !icebergRestSession.trim().isEmpty()) {
            catalogProps.put("rest-catalog-config.session-type", icebergRestSession);
        }
        catalogProps.put("rest-catalog-config.session-timeout-ms", String.valueOf(icebergRestSessionTimeout * 1000));

        // 功能特性配置
        catalogProps.put("rest-catalog-config.vended-credentials-enabled", String.valueOf(vendedCredentialsEnabled));
        catalogProps.put("rest-catalog-config.view-endpoints-enabled", String.valueOf(viewEndpointsEnabled));
        catalogProps.put("rest-catalog-config.nested-namespace-enabled", String.valueOf(nestedNamespaceEnabled));
        catalogProps.put("rest-catalog-config.case-insensitive-name-matching", String.valueOf(caseInsensitiveNameMatching));
        catalogProps.put("rest-catalog-config.case-insensitive-name-matching.cache-ttl",
                        String.valueOf(caseInsensitiveNameMatchingCacheTtl * 1000));

        // HTTP 配置
        catalogProps.put("rest-catalog-config.http.connect-timeout-ms", String.valueOf(httpConnectTimeout));
        catalogProps.put("rest-catalog-config.http.read-timeout-ms", String.valueOf(httpReadTimeout));
        catalogProps.put("rest-catalog-config.http.max-retries", String.valueOf(httpMaxRetries));
        if (httpUserAgent != null && !httpUserAgent.trim().isEmpty()) {
            catalogProps.put("rest-catalog-config.http.user-agent", httpUserAgent);
        }
    }

    /**
     * 获取 REST catalog 的基础配置属性，用于后端配置
     */
    public Map<String, String> getRestCatalogBackendProperties() {
        Map<String, String> backendProps = new HashMap<>();
        
        // 基础连接属性
        backendProps.put("iceberg.rest.uri", icebergRestUri);
        if (icebergRestPrefix != null && !icebergRestPrefix.trim().isEmpty()) {
            backendProps.put("iceberg.rest.prefix", icebergRestPrefix);
        }
        if (icebergRestWarehouse != null && !icebergRestWarehouse.trim().isEmpty()) {
            backendProps.put("iceberg.rest.warehouse", icebergRestWarehouse);
        }

        // 安全认证属性
        backendProps.put("iceberg.rest.security", icebergRestSecurityType);

        // OAuth2 属性
        if ("OAUTH2".equalsIgnoreCase(icebergRestSecurityType)) {
            if (oauth2Token != null && !oauth2Token.trim().isEmpty()) {
                backendProps.put("iceberg.rest.oauth2.token", oauth2Token);
            }
            if (oauth2Credential != null && !oauth2Credential.trim().isEmpty()) {
                backendProps.put("iceberg.rest.oauth2.credential", oauth2Credential);
            }
            if (oauth2Scope != null && !oauth2Scope.trim().isEmpty()) {
                backendProps.put("iceberg.rest.oauth2.scope", oauth2Scope);
            }
            if (oauth2ServerUri != null && !oauth2ServerUri.trim().isEmpty()) {
                backendProps.put("iceberg.rest.oauth2.server-uri", oauth2ServerUri);
            }
            backendProps.put("iceberg.rest.oauth2.token-refresh-enabled", String.valueOf(oauth2TokenRefreshEnabled));
        }

        // SigV4 属性
        if (sigV4Enabled) {
            backendProps.put("iceberg.rest.sigv4-enabled", String.valueOf(sigV4Enabled));
            if (signingName != null && !signingName.trim().isEmpty()) {
                backendProps.put("iceberg.rest.signing-name", signingName);
            }
            if (awsRegion != null && !awsRegion.trim().isEmpty()) {
                backendProps.put("iceberg.rest.aws.region", awsRegion);
            }
            if (awsAccessKey != null && !awsAccessKey.trim().isEmpty()) {
                backendProps.put("iceberg.rest.aws.access-key", awsAccessKey);
            }
            if (awsSecretKey != null && !awsSecretKey.trim().isEmpty()) {
                backendProps.put("iceberg.rest.aws.secret-key", awsSecretKey);
            }
        }

        // 会话管理属性
        backendProps.put("iceberg.rest.session", icebergRestSession);
        backendProps.put("iceberg.rest.session-timeout", String.valueOf(icebergRestSessionTimeout));

        // 功能特性属性
        backendProps.put("iceberg.rest.vended-credentials-enabled", String.valueOf(vendedCredentialsEnabled));
        backendProps.put("iceberg.rest.view-endpoints-enabled", String.valueOf(viewEndpointsEnabled));
        backendProps.put("iceberg.rest.nested-namespace-enabled", String.valueOf(nestedNamespaceEnabled));
        backendProps.put("iceberg.rest.case-insensitive-name-matching", String.valueOf(caseInsensitiveNameMatching));
        backendProps.put("iceberg.rest.case-insensitive-name-matching.cache-ttl", String.valueOf(caseInsensitiveNameMatchingCacheTtl));

        // HTTP 配置属性
        backendProps.put("iceberg.rest.http.connect-timeout", String.valueOf(httpConnectTimeout));
        backendProps.put("iceberg.rest.http.read-timeout", String.valueOf(httpReadTimeout));
        backendProps.put("iceberg.rest.http.max-retries", String.valueOf(httpMaxRetries));
        if (httpUserAgent != null && !httpUserAgent.trim().isEmpty()) {
            backendProps.put("iceberg.rest.http.user-agent", httpUserAgent);
        }
        
        return backendProps;
    }
}
