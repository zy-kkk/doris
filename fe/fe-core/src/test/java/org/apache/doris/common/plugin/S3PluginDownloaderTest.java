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

public class S3PluginDownloaderTest {

    @Test
    public void testS3ConfigToString() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "access-key", "secret");
        String configStr = config.toString();
        Assertions.assertTrue(configStr.contains("***"));

        S3PluginDownloader.S3Config emptyKeyConfig = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", null, "secret");
        String emptyStr = emptyKeyConfig.toString();
        Assertions.assertTrue(emptyStr.contains("null"));
    }

    @Test
    public void testS3ConfigConstructor() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "key", "secret");
        Assertions.assertEquals("endpoint", config.endpoint);
        Assertions.assertEquals("region", config.region);
        Assertions.assertEquals("bucket", config.bucket);
        Assertions.assertEquals("key", config.accessKey);
        Assertions.assertEquals("secret", config.secretKey);
    }

    @Test
    public void testS3PluginDownloaderConstructor() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "key", "secret");
        Assertions.assertDoesNotThrow(() -> {
            new S3PluginDownloader(config);
        });
    }

    @Test
    public void testDownloadFileFailure() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "", "", "", "", "");
        S3PluginDownloader downloader = new S3PluginDownloader(config);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            downloader.downloadFile("s3://bucket/file.jar", "/tmp/test.jar");
        });
        Assertions.assertTrue(exception.getMessage().contains("Download failed"));
    }

    @Test
    public void testAutoCloseable() throws Exception {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "endpoint", "region", "bucket", "key", "secret");

        Assertions.assertDoesNotThrow(() -> {
            try (S3PluginDownloader downloader = new S3PluginDownloader(config)) {
                // Should not throw during close
            }
        });
    }
}
