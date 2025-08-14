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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloudPluginDownloaderTest {

    @Test
    public void testUnsupportedPluginTypes() {
        // Test CONNECTORS (unsupported)
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, "connector.jar", "/tmp/connector.jar");
        });
        Assertions.assertTrue(exception1.getMessage().contains("Unsupported plugin type"));

        // Test HADOOP_CONF (unsupported)
        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.HADOOP_CONF, "core-site.xml", "/tmp/conf.xml");
        });
        Assertions.assertTrue(exception2.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testPluginNameValidation() {
        // Test empty string
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "", "/tmp/test.jar");
        });
        Assertions.assertTrue(exception1.getMessage().contains("pluginName cannot be null or empty"));

        // Test null
        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, null, "/tmp/test.jar");
        });
        Assertions.assertTrue(exception2.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testGetDirectoryName() {
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());
    }

    @Test
    public void testSupportedPluginTypeDownloadFailure() {
        // Test that supported types fail at config retrieval in non-cloud environment
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JDBC_DRIVERS, "driver.jar", "/tmp/driver.jar");
        });

        // Should fail at config retrieval, not plugin type validation
        String message = exception.getMessage();
        boolean isConfigError = message.contains("storage vault")
                || message.contains("Failed to get")
                || message.contains("configuration")
                || message.contains("Failed to download plugin from cloud");

        Assertions.assertTrue(isConfigError, "Expected config-related error, got: " + message);
    }
}
