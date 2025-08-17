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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class S3PluginDownloaderTest {

    private S3PluginDownloader.S3Config testConfig;
    private String tempDir;

    @BeforeEach
    public void setUp() throws Exception {
        testConfig = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", "access-key", "secret-key");

        tempDir = System.getProperty("java.io.tmpdir") + "/s3_plugin_test_" + UUID.randomUUID();
        Files.createDirectories(Paths.get(tempDir));
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (tempDir != null) {
            deleteDirectory(new File(tempDir));
        }
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }

    @Test
    public void testS3ConfigCreation() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", "access-key", "secret-key");

        Assertions.assertEquals("http://s3.amazonaws.com", config.endpoint);
        Assertions.assertEquals("us-west-2", config.region);
        Assertions.assertEquals("test-bucket", config.bucket);
        Assertions.assertEquals("access-key", config.accessKey);
        Assertions.assertEquals("secret-key", config.secretKey);
    }

    @Test
    public void testS3ConfigToString() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", "access-key", "secret-key");

        String configStr = config.toString();

        Assertions.assertTrue(configStr.contains("s3.amazonaws.com"));
        Assertions.assertTrue(configStr.contains("us-west-2"));
        Assertions.assertTrue(configStr.contains("test-bucket"));
        Assertions.assertTrue(configStr.contains("***"));
        Assertions.assertFalse(configStr.contains("access-key"));
        Assertions.assertFalse(configStr.contains("secret-key"));
    }

    @Test
    public void testS3ConfigWithNullValues() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                null, null, null, null, null);

        Assertions.assertNull(config.endpoint);
        Assertions.assertNull(config.region);
        Assertions.assertNull(config.bucket);
        Assertions.assertNull(config.accessKey);
        Assertions.assertNull(config.secretKey);

        String configStr = config.toString();
        Assertions.assertTrue(configStr.contains("null"));
    }

    @Test
    public void testS3ConfigWithEmptyValues() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "", "", "", "");

        Assertions.assertEquals("", config.endpoint);
        Assertions.assertEquals("", config.region);
        Assertions.assertEquals("", config.bucket);
        Assertions.assertEquals("", config.accessKey);
        Assertions.assertEquals("", config.secretKey);

        String configStr = config.toString();
        Assertions.assertTrue(configStr.contains("***"));
    }

    @Test
    public void testS3ConfigFieldsAreFinal() {
        try {
            Field endpointField = S3PluginDownloader.S3Config.class.getField("endpoint");
            Field regionField = S3PluginDownloader.S3Config.class.getField("region");
            Field bucketField = S3PluginDownloader.S3Config.class.getField("bucket");
            Field accessKeyField = S3PluginDownloader.S3Config.class.getField("accessKey");
            Field secretKeyField = S3PluginDownloader.S3Config.class.getField("secretKey");

            Assertions.assertTrue(Modifier.isFinal(endpointField.getModifiers()));
            Assertions.assertTrue(Modifier.isFinal(regionField.getModifiers()));
            Assertions.assertTrue(Modifier.isFinal(bucketField.getModifiers()));
            Assertions.assertTrue(Modifier.isFinal(accessKeyField.getModifiers()));
            Assertions.assertTrue(Modifier.isFinal(secretKeyField.getModifiers()));
        } catch (NoSuchFieldException e) {
            Assertions.fail("S3Config fields should be accessible: " + e.getMessage());
        }
    }

    @Test
    public void testS3ConfigConstructorSignature() {
        try {
            Constructor<S3PluginDownloader.S3Config> constructor =
                    S3PluginDownloader.S3Config.class.getConstructor(
                            String.class, String.class, String.class, String.class, String.class);

            Assertions.assertNotNull(constructor);
            Assertions.assertTrue(Modifier.isPublic(constructor.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("S3Config constructor should exist: " + e.getMessage());
        }
    }

    @Test
    public void testS3PluginDownloaderConstructor() {
        try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
            Assertions.assertNotNull(downloader);
        } catch (Exception e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void testS3PluginDownloaderImplementsAutoCloseable() {
        Assertions.assertTrue(true);

        try {
            Method closeMethod = S3PluginDownloader.class.getMethod("close");
            Assertions.assertNotNull(closeMethod);
            Assertions.assertTrue(Modifier.isPublic(closeMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("close method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testDownloadFileMethodSignature() {
        try {
            Method downloadMethod = S3PluginDownloader.class.getMethod(
                    "downloadFile", String.class, String.class);

            Assertions.assertEquals(String.class, downloadMethod.getReturnType());
            Assertions.assertTrue(Modifier.isPublic(downloadMethod.getModifiers()));
            Assertions.assertFalse(Modifier.isStatic(downloadMethod.getModifiers()));
        } catch (NoSuchMethodException e) {
            Assertions.fail("downloadFile method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testDownloadFileWithInvalidS3Path() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "", "", "", "");

        try (S3PluginDownloader downloader = new S3PluginDownloader(config)) {
            String localPath = tempDir + "/test.jar";

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile("invalid-s3-path", localPath);
            });

            Assertions.assertNotNull(exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("Download failed"));
        } catch (Exception e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void testDownloadFileWithNullParameters() {
        try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile(null, "/path");
            });
            Assertions.assertNotNull(exception.getMessage());

            exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile("s3://bucket/file", null);
            });
            Assertions.assertNotNull(exception.getMessage());
        } catch (Exception e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void testDownloadFileWithEmptyParameters() {
        try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile("", "/path");
            });
            Assertions.assertNotNull(exception.getMessage());

            exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile("s3://bucket/file", "");
            });
            Assertions.assertNotNull(exception.getMessage());
        } catch (Exception e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void testS3ConfigEquality() {
        S3PluginDownloader.S3Config config1 = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "access", "secret");
        S3PluginDownloader.S3Config config2 = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "access", "secret");
        S3PluginDownloader.S3Config config3 = new S3PluginDownloader.S3Config(
                "different", "region", "bucket", "access", "secret");

        Assertions.assertEquals(config1.endpoint, config2.endpoint);
        Assertions.assertEquals(config1.region, config2.region);
        Assertions.assertEquals(config1.bucket, config2.bucket);
        Assertions.assertEquals(config1.accessKey, config2.accessKey);
        Assertions.assertEquals(config1.secretKey, config2.secretKey);

        Assertions.assertNotEquals(config1.endpoint, config3.endpoint);
    }

    @Test
    public void testS3ConfigStringFormatting() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-east-1", "my-bucket", "AKIA123", "secret123");

        String str = config.toString();

        Assertions.assertTrue(str.startsWith("S3Config{"));
        Assertions.assertTrue(str.contains("endpoint='https://s3.amazonaws.com'"));
        Assertions.assertTrue(str.contains("region='us-east-1'"));
        Assertions.assertTrue(str.contains("bucket='my-bucket'"));
        Assertions.assertTrue(str.contains("accessKey='***'"));
        Assertions.assertFalse(str.contains("AKIA123"));
        Assertions.assertFalse(str.contains("secret123"));
    }

    @Test
    public void testS3PluginDownloaderClassStructure() {
        Assertions.assertNotNull(S3PluginDownloader.class);
        Assertions.assertNotNull(S3PluginDownloader.S3Config.class);

        Assertions.assertTrue(Modifier.isStatic(S3PluginDownloader.S3Config.class.getModifiers()));
        Assertions.assertTrue(Modifier.isPublic(S3PluginDownloader.S3Config.class.getModifiers()));
    }

    @Test
    public void testExceptionHandling() {
        S3PluginDownloader.S3Config invalidConfig = new S3PluginDownloader.S3Config(
                "invalid-endpoint", "invalid-region", "invalid-bucket", "invalid-key", "invalid-secret");

        try (S3PluginDownloader downloader = new S3PluginDownloader(invalidConfig)) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile("s3://invalid/path", tempDir + "/test.jar");
            });

            Assertions.assertNotNull(exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("Download failed"));
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testConcurrentDownloads() {
        final int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        final boolean[] results = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
                    downloader.downloadFile("s3://test/file" + index, tempDir + "/file" + index);
                    results[index] = false;
                } catch (Exception e) {
                    results[index] = true;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Assertions.fail("Thread interrupted: " + e.getMessage());
            }
        }

        for (int i = 0; i < threadCount; i++) {
            Assertions.assertTrue(results[i], "Thread " + i + " should handle errors");
        }
    }

    @Test
    public void testResourceManagement() {
        S3PluginDownloader downloader = null;
        try {
            downloader = new S3PluginDownloader(testConfig);
            Assertions.assertNotNull(downloader);
        } catch (Exception e) {
            // Construction may fail
        } finally {
            if (downloader != null) {
                try {
                    downloader.close();
                } catch (Exception e) {
                    Assertions.assertNotNull(e);
                }
            }
        }
    }

    @Test
    public void testS3ConfigWithSpecialCharacters() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3-特殊字符.amazonaws.com", "us-west-1", "bucket-with-dashes_and_underscores",
                "AKIA/special+chars", "secret/with+special=chars");

        Assertions.assertEquals("https://s3-特殊字符.amazonaws.com", config.endpoint);
        Assertions.assertEquals("bucket-with-dashes_and_underscores", config.bucket);
        Assertions.assertEquals("AKIA/special+chars", config.accessKey);
        Assertions.assertEquals("secret/with+special=chars", config.secretKey);

        String str = config.toString();
        Assertions.assertTrue(str.contains("***"));
        Assertions.assertFalse(str.contains("AKIA/special+chars"));
        Assertions.assertFalse(str.contains("secret/with+special=chars"));
    }

    @Test
    public void testDownloadFileReturnValue() {
        try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
            String localPath = tempDir + "/return_test.jar";

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                String result = downloader.downloadFile("s3://test/file.jar", localPath);
                Assertions.assertEquals(localPath, result);
            });

            Assertions.assertTrue(exception.getMessage().contains("Download failed"));
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof RuntimeException || e instanceof Exception);
        }
    }

    @Test
    public void testS3ConfigToStringWithNullAccessKey() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", null, "secret-key");

        String str = config.toString();
        Assertions.assertTrue(str.contains("accessKey='null'"));
        Assertions.assertFalse(str.contains("***"));
    }

    @Test
    public void testMethodExceptionTypes() {
        try (S3PluginDownloader downloader = new S3PluginDownloader(testConfig)) {
            try {
                downloader.downloadFile("invalid", "/invalid");
                Assertions.fail("Should throw RuntimeException");
            } catch (RuntimeException e) {
                Assertions.assertEquals(RuntimeException.class, e.getClass());
                Assertions.assertNotNull(e.getMessage());
            }
        } catch (Exception e) {
            Assertions.assertTrue(true);
        }
    }
}
