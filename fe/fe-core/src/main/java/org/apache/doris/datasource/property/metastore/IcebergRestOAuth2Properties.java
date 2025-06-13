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

import org.apache.doris.datasource.property.ConnectionProperties;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import com.google.common.base.Strings;

import java.util.Map;

public class IcebergRestOAuth2Properties extends ConnectionProperties {

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token"},
            required = false,
            supported = false,
            description = "The OAuth2 bearer token for authentication.")
    private String oauth2Token = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.credential"},
            required = false,
            supported = false,
            description = "The OAuth2 client credentials in format 'client_id:client_secret'.")
    private String oauth2Credential = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.scope"},
            required = false,
            supported = false,
            description = "The OAuth2 scope for the token request.")
    private String oauth2Scope = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.server-uri"},
            required = false,
            supported = false,
            description = "The OAuth2 server URI for token requests.")
    private String oauth2ServerUri = "";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token-refresh-enabled"},
            required = false,
            supported = false,
            description = "Whether to enable automatic token refresh.")
    private String oauth2TokenRefreshEnabled = "true";

    public IcebergRestOAuth2Properties(Map<String, String> origProps) {
        super(origProps);
        initNormalizeAndCheckProps();
    }

    public void validate() {
        boolean hasCredential = !Strings.isNullOrEmpty(oauth2Credential);
        boolean hasToken = !Strings.isNullOrEmpty(oauth2Token);

        if (!hasCredential && !hasToken) {
            throw new IllegalArgumentException(
                    "OAuth2 authentication requires either 'iceberg.rest.oauth2.credential' or 'iceberg.rest.oauth2.token'");
        }

        if (hasCredential && hasToken) {
            throw new IllegalArgumentException(
                    "Cannot set both 'iceberg.rest.oauth2.credential' and 'iceberg.rest.oauth2.token'. Please use only one.");
        }

        if (hasCredential && Strings.isNullOrEmpty(oauth2ServerUri)) {
            throw new IllegalArgumentException(
                    "OAuth2 credential flow requires 'iceberg.rest.oauth2.server-uri' to be set");
        }

        if (hasCredential && !oauth2Credential.contains(":")) {
            throw new IllegalArgumentException(
                    "OAuth2 credential must be in format 'client_id:client_secret'");
        }
    }

    public void toIcebergRestCatalogProperties(Map<String, String> catalogProps) {
        if (!Strings.isNullOrEmpty(oauth2Token)) {
            catalogProps.put(OAuth2Properties.TOKEN, oauth2Token);
        }

        if (!Strings.isNullOrEmpty(oauth2Credential)) {
            catalogProps.put(OAuth2Properties.CREDENTIAL, oauth2Credential);
        }

        if (!Strings.isNullOrEmpty(oauth2Scope)) {
            catalogProps.put(OAuth2Properties.SCOPE, oauth2Scope);
        }

        if (!Strings.isNullOrEmpty(oauth2ServerUri)) {
            catalogProps.put(OAuth2Properties.TOKEN, oauth2ServerUri);
        }

        if (!Strings.isNullOrEmpty(oauth2TokenRefreshEnabled)) {
            catalogProps.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, oauth2TokenRefreshEnabled);
        }
    }
} 