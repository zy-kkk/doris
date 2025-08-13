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

public class PluginFileCacheTest {

    @TempDir
    Path tempDir;

    @Test
    public void testFileInfoCreation() {
        PluginFileCache.FileInfo fileInfo = new PluginFileCache.FileInfo();

        // Test default values
        Assertions.assertNull(fileInfo.localMd5);
        Assertions.assertEquals(0, fileInfo.fileSize);
    }

    @Test
    public void testIsFileValidWithNonExistentFile() {
        String nonExistentPath = tempDir.resolve("non-existent.jar").toString();

        // Test with non-existent file - should return false
        boolean result1 = PluginFileCache.isFileValid(nonExistentPath, "some-md5");
        Assertions.assertFalse(result1);

        boolean result2 = PluginFileCache.isFileValid(nonExistentPath, null);
        Assertions.assertFalse(result2);

        boolean result3 = PluginFileCache.isFileValid(nonExistentPath, "");
        Assertions.assertFalse(result3);
    }

    @Test
    public void testIsFileValidWithEmptyFile() throws IOException {
        // Create empty file
        File emptyFile = tempDir.resolve("empty.jar").toFile();
        emptyFile.createNewFile();

        // Test with empty file - should return false
        boolean result = PluginFileCache.isFileValid(emptyFile.getAbsolutePath(), "some-md5");
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsFileValidWithoutMd5() throws IOException {
        // Create file with content
        File testFile = tempDir.resolve("test.jar").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("Hello, World!");
        }

        // Test without MD5 - should return true if file exists and has content
        boolean result1 = PluginFileCache.isFileValid(testFile.getAbsolutePath(), null);
        Assertions.assertTrue(result1);

        boolean result2 = PluginFileCache.isFileValid(testFile.getAbsolutePath(), "");
        Assertions.assertTrue(result2);
    }

    @Test
    public void testIsFileValidWithMd5ButNoCache() throws IOException {
        // Create file with content
        File testFile = tempDir.resolve("test2.jar").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("Hello, World!");
        }

        // Test with MD5 but no cache entry - should return false
        boolean result = PluginFileCache.isFileValid(testFile.getAbsolutePath(), "some-md5-hash");
        Assertions.assertFalse(result);
    }

    @Test
    public void testUpdateCacheWithNonExistentFile() {
        String nonExistentPath = tempDir.resolve("non-existent.jar").toString();

        // Test updating cache with non-existent file - should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            PluginFileCache.updateCache(nonExistentPath, "md5-hash", 1024);
        });
    }

    @Test
    public void testUpdateCacheWithValidFile() throws IOException {
        // Create file with content
        File testFile = tempDir.resolve("test3.jar").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("Hello, World!");
        }

        String filePath = testFile.getAbsolutePath();
        String md5Hash = "test-md5-hash";
        long fileSize = testFile.length();

        // Update cache
        Assertions.assertDoesNotThrow(() -> {
            PluginFileCache.updateCache(filePath, md5Hash, fileSize);
        });

        // Now test validation with the cached MD5
        boolean result = PluginFileCache.isFileValid(filePath, md5Hash);
        Assertions.assertTrue(result);

        // Test with different MD5 - should return false
        boolean result2 = PluginFileCache.isFileValid(filePath, "different-md5");
        Assertions.assertFalse(result2);
    }

    @Test
    public void testCacheEvictionBehavior() throws IOException {
        // Create multiple files to test cache eviction (100+ files)
        for (int i = 0; i < 105; i++) {
            File testFile = tempDir.resolve("test_" + i + ".jar").toFile();
            try (FileWriter writer = new FileWriter(testFile)) {
                writer.write("Content " + i);
            }

            String filePath = testFile.getAbsolutePath();
            String md5Hash = "md5-hash-" + i;

            PluginFileCache.updateCache(filePath, md5Hash, testFile.length());

            // Immediately verify the cache entry exists
            boolean result = PluginFileCache.isFileValid(filePath, md5Hash);
            Assertions.assertTrue(result, "File " + i + " should be valid immediately after caching");
        }

        // Early entries might be evicted due to LRU behavior, but recent ones should still exist
        File recentFile = tempDir.resolve("test_104.jar").toFile();
        boolean recentResult = PluginFileCache.isFileValid(recentFile.getAbsolutePath(), "md5-hash-104");
        Assertions.assertTrue(recentResult, "Recent cache entry should still exist");
    }
}
