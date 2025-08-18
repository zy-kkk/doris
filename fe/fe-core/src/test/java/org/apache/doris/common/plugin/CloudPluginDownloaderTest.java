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

public class CloudPluginDownloaderTest {

    @Test
    public void testUnsupportedPluginTypes() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS, "test.jar", "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testEmptyPluginName() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "", "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testNullPluginName() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, null, "/path");
        });
        Assertions.assertTrue(exception.getMessage().contains("pluginName cannot be null or empty"));
    }

    @Test
    public void testSupportedPluginTypes() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF, "test.jar", "/path");
        });
        Assertions.assertFalse(exception.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    public void testGetDirectoryName() {
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());
    }
}
