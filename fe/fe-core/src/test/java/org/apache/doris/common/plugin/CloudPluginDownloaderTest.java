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

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class CloudPluginDownloaderTest {

    @Test
    public void testPluginTypeDirectoryNames() {
        // Test PluginType directory name mapping - covers getDirectoryName() method
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());
    }

    @Test
    public void testPluginTypeEnumValues() {
        // Test PluginType enum values exist and are distinct
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.JAVA_UDF);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.JDBC_DRIVERS);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.CONNECTORS);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.HADOOP_CONF);

        // Test enum name method
        Assertions.assertEquals("JAVA_UDF", CloudPluginDownloader.PluginType.JAVA_UDF.name());
        Assertions.assertEquals("JDBC_DRIVERS", CloudPluginDownloader.PluginType.JDBC_DRIVERS.name());
        Assertions.assertEquals("CONNECTORS", CloudPluginDownloader.PluginType.CONNECTORS.name());
        Assertions.assertEquals("HADOOP_CONF", CloudPluginDownloader.PluginType.HADOOP_CONF.name());

        // Test enum distinctness
        Assertions.assertNotEquals(CloudPluginDownloader.PluginType.JAVA_UDF,
                CloudPluginDownloader.PluginType.JDBC_DRIVERS);
        Assertions.assertNotEquals(CloudPluginDownloader.PluginType.CONNECTORS,
                CloudPluginDownloader.PluginType.HADOOP_CONF);
    }

    @Test
    public void testPluginTypeConstructorAndPrivateField() {
        // Test that directoryName is properly set via constructor
        CloudPluginDownloader.PluginType jdbcType = CloudPluginDownloader.PluginType.JDBC_DRIVERS;
        CloudPluginDownloader.PluginType udfType = CloudPluginDownloader.PluginType.JAVA_UDF;

        // Verify directory names are correctly mapped
        Assertions.assertFalse(Strings.isNullOrEmpty(jdbcType.getDirectoryName()));
        Assertions.assertFalse(Strings.isNullOrEmpty(udfType.getDirectoryName()));
        Assertions.assertNotEquals(jdbcType.getDirectoryName(), udfType.getDirectoryName());
    }

    @Test
    public void testDownloadFromCloudSupportedPluginTypes() {
        // Test supported plugin types (should fail due to no cloud env, but pass validation)
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    "test.jar", "/local/path");
        });
        // Should NOT fail due to unsupported type, but due to cloud config issues
        Assertions.assertFalse(exception1.getMessage().contains("Unsupported plugin type"));

        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JDBC_DRIVERS,
                    "mysql.jar", "/local/path");
        });
        // Should NOT fail due to unsupported type, but due to cloud config issues
        Assertions.assertFalse(exception2.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testDownloadFromCloudUnsupportedPluginTypes() {
        // Test unsupported plugin types - covers lines 68-70
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS,
                    "test.jar", "/local/path");
        });
        Assertions.assertTrue(exception1.getMessage().contains("Unsupported plugin type"));
        Assertions.assertTrue(exception1.getMessage().contains("CONNECTORS"));

        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.HADOOP_CONF,
                    "config.xml", "/local/path");
        });
        Assertions.assertTrue(exception2.getMessage().contains("Unsupported plugin type"));
        Assertions.assertTrue(exception2.getMessage().contains("HADOOP_CONF"));
    }

    @Test
    public void testDownloadFromCloudParameterValidation() {
        // Test with null plugin name - covers lines 72-74
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    null, "/local/path");
        });
        Assertions.assertTrue(exception1.getMessage().contains("pluginName cannot be null or empty"));

        // Test with empty plugin name - covers lines 72-74
        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    "", "/local/path");
        });
        Assertions.assertTrue(exception2.getMessage().contains("pluginName cannot be null or empty"));

        // Test with whitespace-only plugin name (should pass validation but fail at cloud config)
        RuntimeException exception3 = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    "   ", "/local/path");
        });
        Assertions.assertFalse(exception3.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testDownloadFromCloudParameterValidationOrder() {
        // Test that plugin type validation happens before plugin name validation
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, // Unsupported type
                    "", // Empty name
                    "/local/path");
        });

        // Should fail on plugin type first, not on empty name
        Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
        Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testDownloadFromCloudAllPluginTypesCoverage() {
        // Test all plugin types to ensure complete enum coverage
        CloudPluginDownloader.PluginType[] allTypes = CloudPluginDownloader.PluginType.values();
        Assertions.assertEquals(4, allTypes.length); // Ensure we test all types

        for (CloudPluginDownloader.PluginType type : allTypes) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(type, "test.file", "/local/path");
            });

            // Check expected behavior based on support status
            if (type == CloudPluginDownloader.PluginType.JAVA_UDF
                    || type == CloudPluginDownloader.PluginType.JDBC_DRIVERS) {
                // Supported types should NOT fail due to type validation
                Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
            } else {
                // Unsupported types should fail due to type validation
                Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
            }
        }
    }

    @Test
    public void testDownloadFromCloudWithValidParameters() {
        // Test that valid parameters pass validation and reach cloud config retrieval
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    "valid-plugin-name.jar",
                    "/valid/local/path");
        });

        // Should fail at cloud config retrieval, not parameter validation
        Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
        Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
        // Should contain cloud-related error
        Assertions.assertTrue(exception.getMessage().contains("storage vault")
                || exception.getMessage().contains("Failed")
                || exception.getMessage().contains("CloudEnv"));
    }

    @Test
    public void testMethodSignatureAndReturnType() {
        // Test method signature through reflection
        try {
            Method method = CloudPluginDownloader.class.getMethod("downloadFromCloud",
                    CloudPluginDownloader.PluginType.class, String.class, String.class);

            Assertions.assertEquals(String.class, method.getReturnType());
            Assertions.assertTrue(Modifier.isStatic(method.getModifiers()));
            Assertions.assertTrue(Modifier.isPublic(method.getModifiers()));

            Class<?>[] paramTypes = method.getParameterTypes();
            Assertions.assertEquals(3, paramTypes.length);
            Assertions.assertEquals(CloudPluginDownloader.PluginType.class, paramTypes[0]);
            Assertions.assertEquals(String.class, paramTypes[1]);
            Assertions.assertEquals(String.class, paramTypes[2]);

        } catch (NoSuchMethodException e) {
            Assertions.fail("downloadFromCloud method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testPluginTypeEnumReflection() {
        // Test PluginType enum through reflection
        Assertions.assertTrue(CloudPluginDownloader.PluginType.class.isEnum());

        // Test values() method exists
        CloudPluginDownloader.PluginType[] values = CloudPluginDownloader.PluginType.values();
        Assertions.assertEquals(4, values.length);

        // Test valueOf() method
        Assertions.assertEquals(CloudPluginDownloader.PluginType.JAVA_UDF,
                CloudPluginDownloader.PluginType.valueOf("JAVA_UDF"));
        Assertions.assertEquals(CloudPluginDownloader.PluginType.JDBC_DRIVERS,
                CloudPluginDownloader.PluginType.valueOf("JDBC_DRIVERS"));
    }

    @Test
    public void testErrorMessageFormats() {
        // Test that error messages are properly formatted and informative
        try {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS,
                    "test.jar", "/path");
            Assertions.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            String message = e.getMessage();
            Assertions.assertNotNull(message);
            Assertions.assertFalse(message.trim().isEmpty());
            Assertions.assertTrue(message.contains("Unsupported plugin type"));
            Assertions.assertTrue(message.contains("CONNECTORS"));
        }

        try {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    null, "/path");
            Assertions.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            String message = e.getMessage();
            Assertions.assertNotNull(message);
            Assertions.assertFalse(message.trim().isEmpty());
            Assertions.assertTrue(message.contains("pluginName cannot be null or empty"));
        }
    }

    @Test
    public void testExceptionHandling() {
        // Test that method handles errors gracefully and throws correct exception types
        try {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS,
                    "test.jar", "/path");
            Assertions.fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // Expected - should be RuntimeException, not other types
            Assertions.assertTrue(e instanceof RuntimeException);
        } catch (Exception e) {
            Assertions.fail("Should only throw RuntimeException, not: " + e.getClass().getSimpleName());
        }
    }

    @Test
    public void testGuavaStringsIntegration() {
        // Test that Strings.isNullOrEmpty is properly used in implementation
        Assertions.assertNotNull(Strings.class);

        // Test the behavior matches what we expect from the implementation
        Assertions.assertTrue(Strings.isNullOrEmpty(null));
        Assertions.assertTrue(Strings.isNullOrEmpty(""));
        Assertions.assertFalse(Strings.isNullOrEmpty("   ")); // Whitespace is not considered empty by Guava
        Assertions.assertFalse(Strings.isNullOrEmpty("test"));
    }

    @Test
    public void testConcurrentAccess() {
        // Test basic thread safety - static method should be stateless
        final int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        final boolean[] results = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    CloudPluginDownloader.downloadFromCloud(
                            CloudPluginDownloader.PluginType.CONNECTORS,
                            "test.jar", "/path");
                    results[index] = false; // Should have thrown exception
                } catch (RuntimeException e) {
                    results[index] = e.getMessage().contains("Unsupported plugin type");
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Assertions.fail("Thread interrupted: " + e.getMessage());
            }
        }

        // All threads should have gotten the expected error consistently
        for (int i = 0; i < threadCount; i++) {
            Assertions.assertTrue(results[i], "Thread " + i + " should have gotten correct exception");
        }
    }

    @Test
    public void testPluginNameEdgeCases() {
        // Test invalid plugin names (null, empty) - Strings.isNullOrEmpty() only checks these
        String[] invalidNames = {"", null};

        for (String invalidName : invalidNames) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF,
                        invalidName, "/path");
            });
            Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
        }

        // Test whitespace names (should pass validation since Strings.isNullOrEmpty() doesn't check whitespace)
        String[] whitespaceNames = {"   ", "\t", "\n"};
        for (String whitespaceName : whitespaceNames) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF,
                        whitespaceName, "/path");
            });
            Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
        }

        // Test valid plugin names (should pass validation, fail at cloud config)
        String[] validNames = {"test.jar", "plugin-name.jar", "plugin_name_123.jar", "a"};

        for (String validName : validNames) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF,
                        validName, "/path");
            });
            // Should NOT fail due to name validation
            Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
        }
    }

    @Test
    public void testPluginTypeDirectoryMapping() {
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());

        for (CloudPluginDownloader.PluginType type : CloudPluginDownloader.PluginType.values()) {
            Assertions.assertNotNull(type.getDirectoryName());
            Assertions.assertFalse(type.getDirectoryName().isEmpty());
        }
    }

    @Test
    public void testSupportedPluginTypesOnly() {
        CloudPluginDownloader.PluginType[] supportedTypes = {
                CloudPluginDownloader.PluginType.JAVA_UDF,
                CloudPluginDownloader.PluginType.JDBC_DRIVERS
        };

        for (CloudPluginDownloader.PluginType type : supportedTypes) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(type, "test.jar", "/path");
            });
            Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
        }

        CloudPluginDownloader.PluginType[] unsupportedTypes = {
                CloudPluginDownloader.PluginType.CONNECTORS,
                CloudPluginDownloader.PluginType.HADOOP_CONF
        };

        for (CloudPluginDownloader.PluginType type : unsupportedTypes) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(type, "test.jar", "/path");
            });
            Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
        }
    }

    @Test
    public void testPluginNameValidationWithGuava() {
        String[] validNames = {"test.jar", "plugin-name.jar", "plugin_name_123.jar", "a", "plugin.war", "driver.zip"};

        for (String validName : validNames) {
            Assertions.assertFalse(Strings.isNullOrEmpty(validName));

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF, validName, "/path");
            });
            Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
        }

        // Test invalid names (only null and empty are checked by Strings.isNullOrEmpty())
        String[] invalidNames = {null, ""};

        for (String invalidName : invalidNames) {
            Assertions.assertTrue(Strings.isNullOrEmpty(invalidName));

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF, invalidName, "/path");
            });
            Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
        }

        // Test whitespace names (should pass validation)
        String[] whitespaceNames = {"   ", "\t", "\n"};
        for (String whitespaceName : whitespaceNames) {
            Assertions.assertFalse(
                    Strings.isNullOrEmpty(whitespaceName)); // This is correct - whitespace is not null or empty

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(
                        CloudPluginDownloader.PluginType.JAVA_UDF, whitespaceName, "/path");
            });
            Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
        }
    }

    @Test
    public void testCloudConfigProviderIntegration() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test.jar", "/path");
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to download plugin from cloud")
                || exception.getMessage().contains("storage vault")
                || exception.getMessage().contains("CloudEnv"));
    }

    @Test
    public void testParameterValidationPriority() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, null, "/path");
        });

        Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
        Assertions.assertFalse(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testClassAndMethodAvailability() {
        Assertions.assertNotNull(CloudPluginDownloader.class);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.class);

        try {
            Method downloadMethod = CloudPluginDownloader.class.getMethod("downloadFromCloud",
                    CloudPluginDownloader.PluginType.class, String.class, String.class);
            Assertions.assertNotNull(downloadMethod);
            Assertions.assertTrue(Modifier.isStatic(downloadMethod.getModifiers()));
            Assertions.assertTrue(Modifier.isPublic(downloadMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("downloadFromCloud method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testExceptionWrapping() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test.jar", "/path");
        });

        Assertions.assertNotNull(exception.getMessage());
        Assertions.assertTrue(exception instanceof RuntimeException);
        Assertions.assertFalse(exception.getMessage().trim().isEmpty());
    }

    @Test
    public void testAllEnumValuesHandled() {
        CloudPluginDownloader.PluginType[] allTypes = CloudPluginDownloader.PluginType.values();
        Assertions.assertEquals(4, allTypes.length);

        int supportedCount = 0;
        int unsupportedCount = 0;

        for (CloudPluginDownloader.PluginType type : allTypes) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                CloudPluginDownloader.downloadFromCloud(type, "test.jar", "/path");
            });

            if (exception.getMessage().contains("Unsupported plugin type")) {
                unsupportedCount++;
            } else {
                supportedCount++;
            }
        }

        Assertions.assertEquals(2, supportedCount);
        Assertions.assertEquals(2, unsupportedCount);
    }

    @Test
    public void testStaticMethodBehavior() {
        try {
            Method downloadMethod = CloudPluginDownloader.class.getMethod("downloadFromCloud",
                    CloudPluginDownloader.PluginType.class, String.class, String.class);

            Assertions.assertTrue(Modifier.isStatic(downloadMethod.getModifiers()));
            Assertions.assertFalse(Modifier.isFinal(downloadMethod.getModifiers()));
            Assertions.assertTrue(Modifier.isPublic(downloadMethod.getModifiers()));

        } catch (NoSuchMethodException e) {
            Assertions.fail("Method should exist: " + e.getMessage());
        }
    }
}
