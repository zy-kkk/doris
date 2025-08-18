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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

public class S3PluginDownloaderTest {

    @Test
    public void testS3ConfigToString() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "access-key", "secret");
        String configStr = config.toString();
        Assertions.assertTrue(configStr.contains("***"));
        Assertions.assertTrue(configStr.contains("prefix='prefix'"));

        S3PluginDownloader.S3Config emptyKeyConfig = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", null, "secret");
        String emptyStr = emptyKeyConfig.toString();
        Assertions.assertTrue(emptyStr.contains("null"));
    }

    @Test
    public void testS3ConfigConstructor() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "key", "secret");
        Assertions.assertEquals("https://s3.amazonaws.com", config.endpoint);
        Assertions.assertEquals("us-west-2", config.region);
        Assertions.assertEquals("bucket", config.bucket);
        Assertions.assertEquals("prefix", config.prefix);
        Assertions.assertEquals("key", config.accessKey);
        Assertions.assertEquals("secret", config.secretKey);
    }

    @Test
    public void testS3PluginDownloaderConstructor() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "key", "secret");
        Assertions.assertDoesNotThrow(() -> {
            new S3PluginDownloader(config);
        });
    }

    @Test
    public void testDownloadFileFailure() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "", "", "", "", "");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file.jar", "/tmp/test.jar");
        });
        Assertions.assertTrue(exception.getMessage().contains("Download failed"));
    }

    @Test
    public void testAutoCloseable() throws Exception {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "key", "secret");

        Assertions.assertDoesNotThrow(() -> {
            try (S3PluginDownloader downloader = new S3PluginDownloader(config)) {
                // Should not throw during close
            }
        });
    }

    @Test
    public void testS3ConfigPrefixInToString() {
        // Test empty prefix
        S3PluginDownloader.S3Config emptyPrefix = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "", "key", "secret");
        String emptyPrefixStr = emptyPrefix.toString();
        Assertions.assertTrue(emptyPrefixStr.contains("prefix=''"));

        // Test non-empty prefix
        S3PluginDownloader.S3Config withPrefix = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "test-prefix", "key", "secret");
        String withPrefixStr = withPrefix.toString();
        Assertions.assertTrue(withPrefixStr.contains("prefix='test-prefix'"));
    }

    @Test
    public void testDirectoryCreation(@TempDir Path tempDir) {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://invalid.endpoint.example.com", "region", "bucket", "prefix", "key", "secret");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        // Test directory creation by using nested path
        String nestedPath = tempDir.resolve("nested/dir/test.jar").toString();

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file.jar", nestedPath);
        });

        // Should fail at S3 download but directory creation should work
        Assertions.assertTrue(exception.getMessage().contains("Download failed"));

        // Verify directory was created
        Assertions.assertTrue(tempDir.resolve("nested/dir").toFile().exists());
    }

    @Test
    public void testExistingFileHandling(@TempDir Path tempDir) throws IOException {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://invalid.endpoint.example.com", "region", "bucket", "prefix", "key", "secret");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        // Create a test file
        File testFile = tempDir.resolve("existing-file.jar").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("test content");
        }

        Assertions.assertTrue(testFile.exists());

        // Attempt download - should delete existing file first
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file.jar", testFile.getAbsolutePath());
        });

        // Should fail at S3 download, but file deletion logic should be exercised
        Assertions.assertTrue(exception.getMessage().contains("Download failed"));
    }

    @Test
    public void testCreateS3StorageWithDifferentConfigs() {
        // Test various configurations to exercise createS3Storage method
        S3PluginDownloader.S3Config[] configs = {
                new S3PluginDownloader.S3Config("https://s3.amazonaws.com", "us-east-1", "bucket1", "prefix1", "key1",
                        "secret1"),
                new S3PluginDownloader.S3Config("https://s3.eu-west-1.amazonaws.com", "eu-west-1", "bucket2", "",
                        "key2", "secret2"),
                new S3PluginDownloader.S3Config("https://localhost:9000", "us-west-2", "bucket3", "prefix3", "key3",
                        "secret3")
        };

        for (S3PluginDownloader.S3Config config : configs) {
            Assertions.assertDoesNotThrow(() -> {
                new S3PluginDownloader(config);
            });
        }
    }

    @Test
    public void testExecuteDownloadSynchronization(@TempDir Path tempDir) {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://invalid.endpoint.example.com", "region", "bucket", "prefix", "key", "secret");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        String path1 = tempDir.resolve("file1.jar").toString();
        String path2 = tempDir.resolve("file2.jar").toString();

        // These will fail but exercise the synchronized executeDownload method
        Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file1.jar", path1);
        });

        Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file2.jar", path2);
        });
    }

    @Test
    public void testDownloadWithInvalidS3Path(@TempDir Path tempDir) {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "key", "secret");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        String localPath = tempDir.resolve("test.jar").toString();

        // Test with invalid S3 paths
        String[] invalidPaths = {
                "invalid-s3-path",
                "s3://",
                "s3://bucket/",
                "http://not-s3/file.jar"
        };

        for (String invalidPath : invalidPaths) {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                downloader.downloadFile(invalidPath, localPath);
            });
            Assertions.assertTrue(exception.getMessage().contains("Download failed"));
        }
    }

    @Test
    public void testCloseWithNullS3Storage() throws Exception {
        // This test exercises the close method's null check
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "https://s3.amazonaws.com", "us-west-2", "bucket", "prefix", "key", "secret");

        try (S3PluginDownloader downloader = new S3PluginDownloader(config)) {
            // close() will be called automatically
        }

        // Test should complete without exceptions
    }
}
