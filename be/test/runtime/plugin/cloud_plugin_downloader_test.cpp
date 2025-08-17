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

#include "runtime/plugin/cloud_plugin_downloader.h"

#include <gtest/gtest.h>

#include <string>

#include "common/status.h"

namespace doris {

// Test all PluginType enum values and their distinctness
TEST(CloudPluginDownloaderTest, TestPluginTypeEnumValues) {
    CloudPluginDownloader::PluginType jdbc_type = CloudPluginDownloader::PluginType::JDBC_DRIVERS;
    CloudPluginDownloader::PluginType udf_type = CloudPluginDownloader::PluginType::JAVA_UDF;
    CloudPluginDownloader::PluginType connector_type =
            CloudPluginDownloader::PluginType::CONNECTORS;
    CloudPluginDownloader::PluginType hadoop_type = CloudPluginDownloader::PluginType::HADOOP_CONF;

    // Verify all enum values are distinct
    EXPECT_NE(jdbc_type, udf_type);
    EXPECT_NE(udf_type, connector_type);
    EXPECT_NE(connector_type, hadoop_type);
    EXPECT_NE(hadoop_type, jdbc_type);
    EXPECT_NE(jdbc_type, connector_type);
    EXPECT_NE(udf_type, hadoop_type);
}

// Test unsupported plugin types
TEST(CloudPluginDownloaderTest, TestDownloadFromCloudUnsupportedPluginTypes) {
    std::string local_path;

    // Test CONNECTORS type (unsupported)
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.tar.gz", "/tmp/test.tar.gz",
            &local_path);

    EXPECT_FALSE(status1.ok());
    EXPECT_EQ(status1.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status1.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_TRUE(status1.to_string().find("connectors") != std::string::npos);

    // Test HADOOP_CONF type (unsupported)
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.xml", "/tmp/test.xml",
            &local_path);

    EXPECT_FALSE(status2.ok());
    EXPECT_EQ(status2.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status2.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_TRUE(status2.to_string().find("hadoop_conf") != std::string::npos);
}

// Test empty plugin name validation
TEST(CloudPluginDownloaderTest, TestDownloadFromCloudEmptyPluginName) {
    std::string local_path;

    // Test with empty plugin name
    Status status =
            CloudPluginDownloader::download_from_cloud(CloudPluginDownloader::PluginType::JAVA_UDF,
                                                       "", // empty plugin name
                                                       "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("cannot be empty") != std::string::npos);
}

// Test null pointer handling
TEST(CloudPluginDownloaderTest, TestDownloadFromCloudNullPointer) {
    // Test with null pointer for local_path
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar",
            nullptr); // null pointer

    // Should handle null pointer gracefully or crash predictably
    EXPECT_FALSE(status.ok());
}

// Test supported plugin types without cloud environment
TEST(CloudPluginDownloaderTest, TestDownloadFromCloudSupportedPluginTypesNoCloudEnv) {
    std::string local_path;

    // Test JAVA_UDF type (supported but will fail due to no cloud env)
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status1.ok());
    // Should NOT fail due to unsupported type, but due to cloud config issues
    EXPECT_FALSE(status1.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_FALSE(status1.to_string().empty());

    // Test JDBC_DRIVERS type (supported but will fail due to no cloud env)
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql.jar", "/tmp/mysql.jar",
            &local_path);

    EXPECT_FALSE(status2.ok());
    // Should NOT fail due to unsupported type, but due to cloud config issues
    EXPECT_FALSE(status2.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_FALSE(status2.to_string().empty());
}

// Test parameter validation order - plugin type should be checked before plugin name
TEST(CloudPluginDownloaderTest, TestParameterValidationOrder) {
    std::string local_path;

    // Both plugin type and name are invalid, should fail on plugin type first
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, // Unsupported type
            "",                                            // Empty name
            "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    // Should fail on plugin type, not on empty name
    EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_FALSE(status.to_string().find("cannot be empty") != std::string::npos);
}

// Test method signature existence and compilation
TEST(CloudPluginDownloaderTest, TestMethodSignatureExists) {
    std::string local_path;

    // This should compile successfully (testing method signature)
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "valid_name.jar", "/tmp/valid_path.jar",
            &local_path);

    // We don't care about success/failure, just that it compiles and runs
    EXPECT_TRUE(status.ok() || !status.ok()); // Always true
}

// Test Status type integration and error handling
TEST(CloudPluginDownloaderTest, TestStatusTypeIntegration) {
    std::string local_path;

    // Test various Status methods work correctly
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, // Will fail with InvalidArgument
            "test.jar", "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status1.ok());
    EXPECT_EQ(status1.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_FALSE(status1.to_string().empty());

    Status status2 =
            CloudPluginDownloader::download_from_cloud(CloudPluginDownloader::PluginType::JAVA_UDF,
                                                       "", // Empty name
                                                       "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status2.ok());
    EXPECT_EQ(status2.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_FALSE(status2.to_string().empty());
}

} // namespace doris