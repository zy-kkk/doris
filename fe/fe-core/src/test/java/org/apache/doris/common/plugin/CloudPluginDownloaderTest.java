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
import org.junit.Test;

public class CloudPluginDownloaderTest {

    @Test
    public void testDownloadFromCloudConfigFails() {
        String pluginName = "mysql-connector-j-8.2.0.jar";
        String localTargetPath = "/path/to/mysql-connector-j-8.2.0.jar";

        String resultPath = CloudPluginDownloader.downloadFromCloud(
                CloudPluginDownloader.PluginType.JDBC_DRIVERS, pluginName, localTargetPath, null);

        Assert.assertTrue(resultPath.isEmpty());
    }

    @Test
    public void testPluginTypeMapping() {
        Assert.assertNotNull(CloudPluginDownloader.PluginType.JDBC_DRIVERS);
        Assert.assertNotNull(CloudPluginDownloader.PluginType.JAVA_UDF);
        Assert.assertNotNull(CloudPluginDownloader.PluginType.CONNECTORS);
        Assert.assertNotNull(CloudPluginDownloader.PluginType.HADOOP_CONF);
    }
}
