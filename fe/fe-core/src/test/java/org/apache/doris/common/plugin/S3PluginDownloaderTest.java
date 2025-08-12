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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class S3PluginDownloaderTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private S3PluginDownloader.S3Config s3Config;
    private String localPath;

    @Before
    public void setUp() throws IOException {
        s3Config = new S3PluginDownloader.S3Config(
                "s3.test-endpoint.com", "us-west-2", "test-bucket", "ak", "sk");
        File tempFile = tempFolder.newFile("test-plugin.jar");
        localPath = tempFile.getAbsolutePath();
        tempFile.delete();
    }

    @Test
    public void testS3ConfigCreation() {
        Assert.assertNotNull(s3Config);
        Assert.assertEquals("s3.test-endpoint.com", s3Config.endpoint);
        Assert.assertEquals("us-west-2", s3Config.region);
        Assert.assertEquals("test-bucket", s3Config.bucket);
        Assert.assertEquals("ak", s3Config.accessKey);
        Assert.assertEquals("sk", s3Config.secretKey);
    }

    @Test
    public void testDownloaderCreation() throws Exception {
        try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
            Assert.assertNotNull(downloader);
        }
    }

    @Test
    public void testDownloadFileWithNonexistentFile() throws Exception {
        String remotePath = "s3://test-bucket/nonexistent.jar";

        try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
            String resultPath = downloader.downloadFile(remotePath, localPath, null);

            Assert.assertTrue(resultPath.isEmpty());
        }
    }

    @Test
    public void testDownloadFileWithInvalidPath() throws Exception {
        String invalidPath = "/invalid/path/that/does/not/exist/file.jar";

        try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
            String resultPath = downloader.downloadFile("s3://test-bucket/test.jar", invalidPath, null);

            Assert.assertTrue(resultPath.isEmpty());
        }
    }
}
