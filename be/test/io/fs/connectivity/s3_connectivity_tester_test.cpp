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

#include "io/fs/connectivity/s3_connectivity_tester.h"

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace doris::io {

class S3ConnectivityTesterTest : public testing::Test {};

TEST_F(S3ConnectivityTesterTest, test_missing_location) {
    std::map<std::string, std::string> properties;
    // Missing test_location property
    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(S3ConnectivityTesterTest, test_invalid_s3_uri_format) {
    std::map<std::string, std::string> properties;
    properties[S3ConnectivityTester::TEST_LOCATION] = "invalid-uri";

    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(S3ConnectivityTesterTest, test_empty_s3_uri) {
    std::map<std::string, std::string> properties;
    properties[S3ConnectivityTester::TEST_LOCATION] = "";

    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(S3ConnectivityTesterTest, test_s3_uri_without_bucket) {
    std::map<std::string, std::string> properties;
    properties[S3ConnectivityTester::TEST_LOCATION] = "s3://";

    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("bucket") != std::string::npos);
}

TEST_F(S3ConnectivityTesterTest, test_s3_uri_with_only_scheme) {
    std::map<std::string, std::string> properties;
    properties[S3ConnectivityTester::TEST_LOCATION] = "s3a://";

    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("bucket") != std::string::npos);
}

TEST_F(S3ConnectivityTesterTest, test_non_s3_uri_scheme) {
    std::map<std::string, std::string> properties;
    properties[S3ConnectivityTester::TEST_LOCATION] = "hdfs://bucket/path";

    auto status = S3ConnectivityTester::test(properties);
    EXPECT_FALSE(status.ok());
}

TEST_F(S3ConnectivityTesterTest, test_test_location_constant) {
    // Verify the constant value
    EXPECT_STREQ(S3ConnectivityTester::TEST_LOCATION, "test_location");
}

} // namespace doris::io
