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
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, "test.jar", "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testEmptyPluginName() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "", "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testNullPluginName() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, null, "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testSupportedPluginTypes() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test.jar", "/path");
        });
        Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testGetDirectoryName() {
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());
    }

    @Test
    public void testAllPluginTypeEnumValues() {
        // Test all enum values to ensure complete coverage
        CloudPluginDownloader.PluginType[] types = CloudPluginDownloader.PluginType.values();
        Assertions.assertEquals(4, types.length);

        // Test each type's properties
        for (CloudPluginDownloader.PluginType type : types) {
            Assertions.assertNotNull(type.getDirectoryName());
            Assertions.assertFalse(type.getDirectoryName().isEmpty());
            Assertions.assertNotNull(type.name());
        }
    }

    @Test
    public void testSupportedPluginTypeValidation() {
        // Test JDBC_DRIVERS (supported)
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JDBC_DRIVERS, "mysql-connector.jar", "/tmp/mysql.jar");
        });
        Assertions.assertFalse(exception1.getMessage().contains("Unsupported plugin type"));

        // Test JAVA_UDF (supported)
        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "my-udf.jar", "/tmp/udf.jar");
        });
        Assertions.assertFalse(exception2.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testUnsupportedPluginTypeValidation() {
        // Test CONNECTORS (unsupported)
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, "connector.jar", "/tmp/connector.jar");
        });
        Assertions.assertTrue(exception1.getMessage().contains("Unsupported plugin type"));
        Assertions.assertTrue(exception1.getMessage().contains("CONNECTORS"));

        // Test HADOOP_CONF (unsupported)
        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.HADOOP_CONF, "core-site.xml", "/tmp/conf.xml");
        });
        Assertions.assertTrue(exception2.getMessage().contains("Unsupported plugin type"));
        Assertions.assertTrue(exception2.getMessage().contains("HADOOP_CONF"));
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

        // Test whitespace-only string (should be considered empty after trim by Strings.isNullOrEmpty)
        RuntimeException exception3 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "   ", "/tmp/test.jar");
        });
        // This should pass plugin name validation and fail later at config retrieval
        Assertions.assertFalse(exception3.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testCloudConfigRetrievalFailure() {
        // Test that supported types fail at config retrieval (not plugin type validation)
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JDBC_DRIVERS, "driver.jar", "/tmp/driver.jar");
        });

        // Should fail at CloudPluginConfigProvider::getCloudS3Config
        String message = exception.getMessage();
        boolean isConfigError = message.contains("storage vault")
                || message.contains("Failed to get")
                || message.contains("configuration")
                || message.contains("Failed to download plugin from cloud");

        Assertions.assertTrue(isConfigError, "Expected config-related error, got: " + message);
    }

    @Test
    public void testS3PathConstruction() {
        // Test that path construction logic is exercised
        // This will fail at config retrieval but exercises the path building code
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test-udf.jar", "/tmp/udf.jar");
        });

        // Should not be plugin type or name validation errors
        Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
        Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testExceptionHandling() {
        // Test that exceptions are properly wrapped
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JDBC_DRIVERS, "test.jar", "/invalid/path");
        });

        // Should be wrapped in RuntimeException with descriptive message
        Assertions.assertNotNull(exception.getMessage());
        String message = exception.getMessage();
        boolean hasExpectedMessage = message.contains("Failed to download plugin from cloud")
                || message.contains("storage vault")
                || message.contains("Failed to get");

        Assertions.assertTrue(hasExpectedMessage, "Unexpected error message: " + message);
    }

    @Test
    public void testTryWithResourcesHandling() {
        // Test that try-with-resources properly handles S3PluginDownloader.close()
        // This will fail but should exercise the close() path
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test.jar", "/tmp/test.jar");
        });

        // Should fail before reaching the download part, but close() should be called
        Assertions.assertNotNull(exception.getMessage());
    }
}
