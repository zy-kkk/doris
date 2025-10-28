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

#include "io/fs/connectivity/storage_connectivity_tester.h"

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace doris::io {

class StorageConnectivityTesterTest : public testing::Test {};

TEST_F(StorageConnectivityTesterTest, test_unsupported_backend_types) {
    std::map<std::string, std::string> properties;

    // Test HDFS backend - should return OK (not implemented yet)
    auto status = StorageConnectivityTester::test(TStorageBackendType::HDFS, properties);
    EXPECT_TRUE(status.ok());

    // Test AZURE backend - should return OK (not implemented yet)
    status = StorageConnectivityTester::test(TStorageBackendType::AZURE, properties);
    EXPECT_TRUE(status.ok());

    // Test default case with unsupported type
    status = StorageConnectivityTester::test(TStorageBackendType::LOCAL, properties);
    EXPECT_TRUE(status.ok());
}

TEST_F(StorageConnectivityTesterTest, test_s3_backend_missing_location) {
    std::map<std::string, std::string> properties;
    // Empty properties - S3 test should fail because test_location is missing
    auto status = StorageConnectivityTester::test(TStorageBackendType::S3, properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(StorageConnectivityTesterTest, test_s3_backend_invalid_location) {
    std::map<std::string, std::string> properties;
    properties["test_location"] = "invalid-uri-format";

    auto status = StorageConnectivityTester::test(TStorageBackendType::S3, properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(StorageConnectivityTesterTest, test_s3_backend_empty_bucket) {
    std::map<std::string, std::string> properties;
    // S3 URI without bucket
    properties["test_location"] = "s3://";

    auto status = StorageConnectivityTester::test(TStorageBackendType::S3, properties);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("bucket") != std::string::npos);
}

} // namespace doris::io
