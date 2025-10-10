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

/**
 * Interface for testing connectivity to storage systems.
 * Implementations should test both FE-to-storage and BE-to-storage connectivity.
 */
public interface StorageConnectivityTester {
    /**
     * Test FE to storage connectivity.
     * This method should create a temporary client, perform a lightweight
     * connectivity test (e.g., headBucket for S3, exists("/") for HDFS),
     * and clean up resources.
     *
     * @throws Exception if FE connection test fails
     */
    void testFeConnection() throws Exception;

    /**
     * Test BE to storage connectivity via RPC.
     * This method should send a test request to the specified backend
     * with storage properties, and the BE will perform the actual connectivity test.
     *
     * @param backendId BE node ID to test
     * @throws Exception if BE connection test fails
     */
    void testBeConnection(long backendId) throws Exception;
}
