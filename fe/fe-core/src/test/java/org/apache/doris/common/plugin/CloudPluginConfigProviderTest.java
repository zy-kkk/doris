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
                || exception.getMessage().contains("Failed to get storage vault"));
    }

    @Test
    public void testGetCloudInstanceIdWithoutCloudEnvironment() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                CloudPluginConfigProvider::getCloudInstanceId);
        Assertions.assertNotNull(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("CloudEnv instance ID"));
    }
}
