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
import org.apache.doris.common.Config;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class CloudPluginConfigProviderTest {

    @Test
    public void testGetCloudS3ConfigWithoutCloudEnvironment() {
        // Test getting S3 config without actual cloud environment
        // This should throw RuntimeException since we're not in cloud mode
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);
        Assertions.assertNotNull(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("No default storage vault")
                || exception.getMessage().contains("Failed to get storage vault"));
    }

    @Test
    public void testGetCloudInstanceIdWithoutCloudEnvironment() {
        // Test getting cloud instance ID without actual cloud environment
        // This should throw RuntimeException since we're not in cloud mode
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudInstanceId);
        Assertions.assertNotNull(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("CloudEnv instance ID is null or empty"));
    }

    @Test
    public void testGetCloudInstanceIdWithMockedCloudEnv(@Mocked CloudEnv mockCloudEnv) {
        // Mock CloudEnv with valid instance ID
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public String getCloudInstanceId() {
                return "test-instance-123";
            }
        };

        String instanceId = CloudPluginConfigProvider.getCloudInstanceId();
        Assertions.assertEquals("test-instance-123", instanceId);
    }

    @Test
    public void testGetCloudInstanceIdWithEmptyInstanceId(@Mocked CloudEnv mockCloudEnv) {
        // Mock CloudEnv with empty instance ID
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public String getCloudInstanceId() {
                return "";  // Empty instance ID
            }
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudInstanceId);
        Assertions.assertTrue(exception.getMessage().contains("CloudEnv instance ID is null or empty"));
    }

    @Test
    public void testGetCloudInstanceIdWithNullInstanceId(@Mocked CloudEnv mockCloudEnv) {
        // Mock CloudEnv with null instance ID
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public String getCloudInstanceId() {
                return null;  // Null instance ID
            }
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudInstanceId);
        Assertions.assertTrue(exception.getMessage().contains("CloudEnv instance ID is null or empty"));
    }

    @Test
    public void testGetCloudInstanceIdWithNonCloudEnv(@Mocked Env mockEnv) {
        // Mock non-CloudEnv environment
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;  // Regular Env, not CloudEnv
            }
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudInstanceId);
        Assertions.assertTrue(exception.getMessage().contains("CloudEnv instance ID is null or empty"));
    }

    @Test
    public void testCloudPluginConfigProviderClassExists() {
        // Basic test that the class exists and methods are accessible
        Assertions.assertNotNull(CloudPluginConfigProvider.class);

        // Verify method existence by reflection
        try {
            CloudPluginConfigProvider.class.getMethod("getCloudS3Config");
            CloudPluginConfigProvider.class.getMethod("getCloudInstanceId");
        } catch (NoSuchMethodException e) {
            Assertions.fail("Required methods should exist: " + e.getMessage());
        }
    }

    @Test
    public void testMethodSignaturesAndReturnTypes() {
        // Test method signatures through reflection
        try {
            Method getS3ConfigMethod = CloudPluginConfigProvider.class.getMethod("getCloudS3Config");
            Assertions.assertEquals(S3PluginDownloader.S3Config.class, getS3ConfigMethod.getReturnType());
            Assertions.assertTrue(Modifier.isStatic(getS3ConfigMethod.getModifiers()));
            Assertions.assertTrue(Modifier.isPublic(getS3ConfigMethod.getModifiers()));

            Method getInstanceIdMethod = CloudPluginConfigProvider.class.getMethod("getCloudInstanceId");
            Assertions.assertEquals(String.class, getInstanceIdMethod.getReturnType());
            Assertions.assertTrue(Modifier.isStatic(getInstanceIdMethod.getModifiers()));
            Assertions.assertTrue(Modifier.isPublic(getInstanceIdMethod.getModifiers()));

        } catch (NoSuchMethodException e) {
            Assertions.fail("Method signature verification failed: " + e.getMessage());
        }
    }

    @Test
    public void testErrorMessageFormats() {
        // Test that error messages are properly formatted and informative
        try {
            CloudPluginConfigProvider.getCloudS3Config();
            Assertions.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            String message = e.getMessage();
            Assertions.assertNotNull(message);
            Assertions.assertFalse(message.trim().isEmpty());
            // Error message should contain relevant keywords
            Assertions.assertTrue(message.contains("storage vault")
                    || message.contains("Failed")
                    || message.contains("configuration"));
        }

        try {
            CloudPluginConfigProvider.getCloudInstanceId();
            Assertions.fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            String message = e.getMessage();
            Assertions.assertNotNull(message);
            Assertions.assertFalse(message.trim().isEmpty());
            // Error message should contain relevant keywords
            Assertions.assertTrue(message.contains("CloudEnv")
                    || message.contains("instance ID")
                    || message.contains("null or empty"));
        }
    }

    @Test
    public void testConfigClassIntegration() {
        // Test that Config class is accessible (used in the implementation)
        Assertions.assertNotNull(Config.class);

        // Verify cloud_unique_id field exists (used in implementation)
        try {
            Field field = Config.class.getField("cloud_unique_id");
            Assertions.assertEquals(String.class, field.getType());
        } catch (NoSuchFieldException e) {
            Assertions.fail("Config.cloud_unique_id field should exist: " + e.getMessage());
        }
    }

    @Test
    public void testExceptionHandling() {
        // Test that methods handle errors gracefully and don't throw unexpected exceptions
        try {
            CloudPluginConfigProvider.getCloudS3Config();
            Assertions.fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // Expected - should be RuntimeException, not other types
            Assertions.assertTrue(e instanceof RuntimeException);
        } catch (Exception e) {
            Assertions.fail("Should only throw RuntimeException, not: " + e.getClass().getSimpleName());
        }

        try {
            CloudPluginConfigProvider.getCloudInstanceId();
            Assertions.fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // Expected - should be RuntimeException, not other types
            Assertions.assertTrue(e instanceof RuntimeException);
        } catch (Exception e) {
            Assertions.fail("Should only throw RuntimeException, not: " + e.getClass().getSimpleName());
        }
    }

    @Test
    public void testConcurrentAccess() {
        // Test basic thread safety - methods should be stateless
        final int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        final boolean[] results = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    CloudPluginConfigProvider.getCloudInstanceId();
                    results[index] = false; // Should have thrown exception
                } catch (RuntimeException e) {
                    results[index] = true; // Expected exception
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

        // All threads should have gotten RuntimeException consistently
        for (int i = 0; i < threadCount; i++) {
            Assertions.assertTrue(results[i], "Thread " + i + " should have gotten RuntimeException");
        }
    }

    @Test
    public void testS3ConfigTypeAvailability() {
        Assertions.assertNotNull(S3PluginDownloader.S3Config.class);

        try {
            S3PluginDownloader.S3Config.class.getConstructor(String.class, String.class, String.class, String.class,
                    String.class);
        } catch (NoSuchMethodException e) {
            Assertions.fail("S3Config constructor should exist: " + e.getMessage());
        }
    }

    @Test
    public void testEnvIntegration() {
        Assertions.assertNotNull(Env.class);
        Assertions.assertNotNull(CloudEnv.class);

        try {
            Method getCurrentEnv = Env.class.getMethod("getCurrentEnv");
            Assertions.assertEquals(Env.class, getCurrentEnv.getReturnType());
            Assertions.assertTrue(Modifier.isStatic(getCurrentEnv.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("Env.getCurrentEnv method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testCloudEnvInstanceIdMethod() {
        try {
            Method getCloudInstanceId = CloudEnv.class.getMethod("getCloudInstanceId");
            Assertions.assertEquals(String.class, getCloudInstanceId.getReturnType());
            Assertions.assertTrue(Modifier.isPublic(getCloudInstanceId.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("CloudEnv.getCloudInstanceId method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testGetDefaultStorageVaultIntegration(@Mocked CloudEnv mockCloudEnv) {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        Assertions.assertNotNull(exception.getMessage());
        // The specific error depends on the mock setup, just verify it's a meaningful error
        Assertions.assertFalse(exception.getMessage().trim().isEmpty());
    }

    @Test
    public void testStringValidationWithInstanceId(@Mocked CloudEnv mockCloudEnv) {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public String getCloudInstanceId() {
                return "   "; // Whitespace string - should NOT trigger null/empty validation
            }
        };

        // This should NOT throw exception because "   " is not null or empty according to Strings.isNullOrEmpty()
        String instanceId = CloudPluginConfigProvider.getCloudInstanceId();
        Assertions.assertEquals("   ", instanceId);
    }

    @Test
    public void testClassLoadingAndAvailability() {
        try {
            Class.forName("org.apache.doris.common.plugin.CloudPluginConfigProvider");
            Class.forName("org.apache.doris.common.plugin.S3PluginDownloader");
            Class.forName("org.apache.doris.catalog.Env");
            Class.forName("org.apache.doris.cloud.catalog.CloudEnv");
        } catch (ClassNotFoundException e) {
            Assertions.fail("Required classes should be available: " + e.getMessage());
        }
    }

    @Test
    public void testRuntimeExceptionTypes() {
        try {
            CloudPluginConfigProvider.getCloudS3Config();
            Assertions.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assertions.assertEquals(RuntimeException.class, e.getClass());
        }

        try {
            CloudPluginConfigProvider.getCloudInstanceId();
            Assertions.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assertions.assertEquals(RuntimeException.class, e.getClass());
        }
    }
}
