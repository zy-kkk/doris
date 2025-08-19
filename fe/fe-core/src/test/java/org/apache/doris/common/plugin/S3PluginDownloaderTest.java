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
                "", "us-west-2", "bucket", "prefix", "access-key", "secret");
        String configStr = config.toString();
        Assertions.assertTrue(configStr.contains("***"));
        Assertions.assertTrue(configStr.contains("prefix='prefix'"));

        S3PluginDownloader.S3Config emptyKeyConfig = new S3PluginDownloader.S3Config(
                "", "us-west-2", "bucket", "prefix", null, "secret");
        String emptyStr = emptyKeyConfig.toString();
        Assertions.assertTrue(emptyStr.contains("null"));
    }

    @Test
    public void testS3ConfigConstructor() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "us-west-2", "bucket", "prefix", "key", "secret");
        Assertions.assertEquals("", config.endpoint);
        Assertions.assertEquals("us-west-2", config.region);
        Assertions.assertEquals("bucket", config.bucket);
        Assertions.assertEquals("prefix", config.prefix);
        Assertions.assertEquals("key", config.accessKey);
        Assertions.assertEquals("secret", config.secretKey);

        // Test constructor doesn't throw
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
                "", "", "", "", "", ""); // Use empty config to avoid endpoint validation

        Assertions.assertDoesNotThrow(() -> {
            try (S3PluginDownloader downloader = new S3PluginDownloader(config)) {
                // Should not throw during close
            }
        });
    }


    @Test
    public void testDirectoryCreation(@TempDir Path tempDir) {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "", "", "", "", ""); // Use empty config to avoid endpoint validation
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
                "", "", "", "", "", ""); // Use empty config to avoid endpoint validation
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

}
