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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PluginFileCacheTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String testFilePath;
    private String testMd5 = "1e48521ebe1a2489118e4785461b17b8"; // MD5 of "test content"
    private String testContent = "test content";

    @Before
    public void setUp() throws IOException {
        testFilePath = tempFolder.newFile("test_plugin.jar").getAbsolutePath();
        Files.deleteIfExists(Paths.get(testFilePath)); // Ensure file doesn't exist initially
    }

    @Test
    public void testIsFileValidWithNonExistentFile() {
        Assert.assertFalse(PluginFileCache.isFileValid(testFilePath, null));
        Assert.assertFalse(PluginFileCache.isFileValid(testFilePath, testMd5));
    }

    @Test
    public void testIsFileValidWithExistingFileNoMd5() throws IOException {
        Files.write(Paths.get(testFilePath), testContent.getBytes());
        Assert.assertTrue(PluginFileCache.isFileValid(testFilePath, null));
    }

    @Test
    public void testUpdateCacheAndCheckFile() throws IOException {
        Files.write(Paths.get(testFilePath), testContent.getBytes());

        PluginFileCache.updateCache(testFilePath, "etag1", testContent.length());

        Assert.assertTrue(PluginFileCache.isFileValid(testFilePath, null));
    }

    @Test
    public void testMultipleFileCache() throws IOException {
        String file1 = tempFolder.newFile("file1.jar").getAbsolutePath();
        String file2 = tempFolder.newFile("file2.jar").getAbsolutePath();
        String content1 = "content1";
        String content2 = "content2";

        Files.write(Paths.get(file1), content1.getBytes());
        Files.write(Paths.get(file2), content2.getBytes());

        PluginFileCache.updateCache(file1, "etag1", content1.length());
        PluginFileCache.updateCache(file2, "etag2", content2.length());

        Assert.assertTrue(PluginFileCache.isFileValid(file1, null));
        Assert.assertTrue(PluginFileCache.isFileValid(file2, null));
    }
}
