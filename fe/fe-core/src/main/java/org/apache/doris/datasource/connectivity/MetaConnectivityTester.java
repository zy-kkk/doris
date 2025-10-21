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

package org.apache.doris.datasource.connectivity;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

public interface MetaConnectivityTester {
    Pattern LOCATION_PATTERN = Pattern.compile("^(s3|s3a|hdfs)://.+");

    default void testConnection() throws Exception {
        // Default: test passes (no-op)
    }

    default String getTestLocation() {
        return null;
    }

    default String validateAndGetLocation(String location) {
        if (StringUtils.isNotBlank(location) && LOCATION_PATTERN.matcher(location).matches()) {
            return location;
        }
        return null;
    }

    /**
     * Returns the type of meta connectivity test for error messages.
     * Default implementation returns "Meta" for generic meta connectivity test.
     */
    default String getTestType() {
        return "Meta";
    }
}
