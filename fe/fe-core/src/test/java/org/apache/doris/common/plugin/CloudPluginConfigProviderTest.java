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

public class CloudPluginConfigProviderTest {

    @Test
    public void testGetCloudS3ConfigWithoutCloudEnvironment() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);
        Assertions.assertNotNull(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("No default storage vault")
                || exception.getMessage().contains("Failed to get storage vault")
                || exception.getMessage().contains("Incomplete S3 configuration"));
    }

    @Test
    public void testGetCloudS3ConfigErrorMessages() {
        // Test different error scenarios that can occur
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        String message = exception.getMessage();
        Assertions.assertNotNull(message);

        // Should contain one of the expected error patterns
        boolean hasExpectedError = message.contains("No default storage vault")
                || message.contains("Failed to get storage vault")
                || message.contains("No suitable storage vault")
                || message.contains("Incomplete S3 configuration")
                || message.contains("does not have object store info");

        Assertions.assertTrue(hasExpectedError, "Unexpected error message: " + message);
    }

    @Test
    public void testGetDefaultStorageVaultInfoErrorPaths() {
        // Test various error conditions in getDefaultStorageVaultInfo
        // All should throw RuntimeException since we're not in cloud mode

        for (int i = 0; i < 3; i++) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    CloudPluginConfigProvider::getCloudS3Config);
            Assertions.assertNotNull(exception.getMessage());
        }
    }

    @Test
    public void testS3ConfigValidation() {
        // This test exercises the S3 config validation logic
        // Since we can't create valid cloud environment in unit test,
        // we expect validation errors

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        String message = exception.getMessage();
        // Could be storage vault error or config validation error
        Assertions.assertTrue(
                message.contains("storage vault")
                        || message.contains("Incomplete S3 configuration")
                        || message.contains("bucket")
                        || message.contains("ak")
                        || message.contains("sk")
        );
    }

    @Test
    public void testErrorHandlingConsistency() {
        // Test that error handling is consistent across multiple calls
        RuntimeException exception1 = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        RuntimeException exception2 = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        // Both should fail with similar error patterns
        Assertions.assertNotNull(exception1.getMessage());
        Assertions.assertNotNull(exception2.getMessage());

        // Error messages should be consistent (both should be about storage vault or config)
        boolean bothAboutVault = exception1.getMessage().contains("vault")
                && exception2.getMessage().contains("vault");
        boolean bothAboutConfig = exception1.getMessage().contains("configuration")
                && exception2.getMessage().contains("configuration");

        // At least one pattern should be consistent
        Assertions.assertTrue(bothAboutVault || bothAboutConfig
                || exception1.getClass().equals(exception2.getClass()));
    }

    @Test
    public void testMultipleErrorScenarios() {
        // Test that all potential error paths throw RuntimeException
        String[] expectedErrorKeywords = {
                "storage vault", "configuration", "bucket", "ak", "sk", "endpoint", "Failed", "No default"
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudS3Config);

        String message = exception.getMessage().toLowerCase();
        boolean foundExpectedKeyword = false;

        for (String keyword : expectedErrorKeywords) {
            if (message.contains(keyword.toLowerCase())) {
                foundExpectedKeyword = true;
                break;
            }
        }

        Assertions.assertTrue(foundExpectedKeyword,
                "Error message should contain expected keywords: " + message);
    }
}
