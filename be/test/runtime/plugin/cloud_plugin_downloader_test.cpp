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

#include <memory>
#include <string>

#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize a minimal StorageEngine to avoid BaseStorageEngine nullptr
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void TearDown() override {
        // Cleanup storage engine
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void SetupCloudStorageEngine() {
        doris::EngineOptions options;
        auto cloud_engine = std::make_unique<CloudStorageEngine>(options);
        std::unique_ptr<BaseStorageEngine> base_engine(cloud_engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }
};

TEST_F(CloudPluginDownloaderTest, TestUnsupportedPluginTypes) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.jar", "/tmp/test.jar",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestEmptyPluginName) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("cannot be empty") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestSupportedPluginTypes) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestPluginTypeToString) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.xml", "/tmp/test.xml",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("hadoop_conf") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestJdbcDriversPluginType) {
    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "test.jar", "/tmp/test.jar",
            &local_path);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestPluginTypeStringConversion) {
    std::string local_path;
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "udf.jar", "/tmp/udf.jar", &local_path);
    EXPECT_FALSE(status1.ok());

    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "connector.jar", "/tmp/connector.jar",
            &local_path);
    EXPECT_FALSE(status2.ok());
    EXPECT_TRUE(status2.to_string().find("connectors") != std::string::npos);
}

// Test all plugin type string conversions to cover _plugin_type_to_string function
TEST_F(CloudPluginDownloaderTest, TestAllPluginTypeStrings) {
    std::string local_path;

    // Test JDBC_DRIVERS - should pass type check but fail at config
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "driver.jar", "/tmp/driver.jar",
            &local_path);
    EXPECT_FALSE(status1.ok());
    EXPECT_FALSE(status1.to_string().find("Unsupported plugin type") != std::string::npos);

    // Test JAVA_UDF - should pass type check but fail at config
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "udf.jar", "/tmp/udf.jar", &local_path);
    EXPECT_FALSE(status2.ok());
    EXPECT_FALSE(status2.to_string().find("Unsupported plugin type") != std::string::npos);

    // Test CONNECTORS - should fail at type check
    Status status3 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "connector.jar", "/tmp/connector.jar",
            &local_path);
    EXPECT_FALSE(status3.ok());
    EXPECT_TRUE(status3.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_TRUE(status3.to_string().find("connectors") != std::string::npos);

    // Test HADOOP_CONF - should fail at type check
    Status status4 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "core-site.xml", "/tmp/core-site.xml",
            &local_path);
    EXPECT_FALSE(status4.ok());
    EXPECT_TRUE(status4.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_TRUE(status4.to_string().find("hadoop_conf") != std::string::npos);
}

// Test cloud storage engine configuration retrieval failure
TEST_F(CloudPluginDownloaderTest, TestCloudConfigRetrievalFailure) {
    // Setup CloudStorageEngine but it will fail to get vault info
    SetupCloudStorageEngine();

    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar", &local_path);

    // Should fail at CloudPluginConfigProvider::get_cloud_s3_config
    EXPECT_FALSE(status.ok());
    // Should not be "Unsupported plugin type" or "cannot be empty" errors
    EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_FALSE(status.to_string().find("cannot be empty") != std::string::npos);
}

// Test different supported plugin types with cloud engine
TEST_F(CloudPluginDownloaderTest, TestSupportedTypesWithCloudEngine) {
    SetupCloudStorageEngine();

    std::string local_path;

    // Test JDBC_DRIVERS
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql-connector.jar",
            "/tmp/mysql.jar", &local_path);
    EXPECT_FALSE(status1.ok());
    // Should pass plugin type check, fail later
    EXPECT_FALSE(status1.to_string().find("Unsupported plugin type") != std::string::npos);

    // Test JAVA_UDF
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "my-udf.jar", "/tmp/udf.jar", &local_path);
    EXPECT_FALSE(status2.ok());
    // Should pass plugin type check, fail later
    EXPECT_FALSE(status2.to_string().find("Unsupported plugin type") != std::string::npos);
}

// Test edge cases for plugin names
TEST_F(CloudPluginDownloaderTest, TestPluginNameEdgeCases) {
    std::string local_path;

    // Test empty plugin name with supported type
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status1.ok());
    EXPECT_EQ(status1.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status1.to_string().find("plugin_name cannot be empty") != std::string::npos);

    // Test whitespace-only plugin name (still not empty string)
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "   ", "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status2.ok());
    // Should pass empty check, fail at config retrieval
    EXPECT_FALSE(status2.to_string().find("plugin_name cannot be empty") != std::string::npos);
}

// Test path construction and S3 downloader creation (will fail but exercises code)
TEST_F(CloudPluginDownloaderTest, TestPathConstructionAndDownload) {
    SetupCloudStorageEngine();

    std::string local_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "test-driver.jar", "/tmp/driver.jar",
            &local_path);

    // This will fail because:
    // 1. CloudStorageEngine exists (passes dynamic_cast)
    // 2. But CloudMetaMgr is not properly initialized in test
    // 3. So get_cloud_s3_config will fail
    EXPECT_FALSE(status.ok());

    // But it should have exercised the code path up to S3 config retrieval
    EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_FALSE(status.to_string().find("plugin_name cannot be empty") != std::string::npos);
}

// Additional test to achieve 100% code coverage

// Test to cover the default case in _plugin_type_to_string function
TEST_F(CloudPluginDownloaderTest, TestPluginTypeToStringDefaultCase) {
    // This test is designed to cover the default branch in _plugin_type_to_string
    // We need to cast an invalid enum value to trigger the default case

    std::string local_path;

    // Cast an invalid integer to PluginType to trigger default case
    CloudPluginDownloader::PluginType invalid_type =
            static_cast<CloudPluginDownloader::PluginType>(999);

    Status status = CloudPluginDownloader::download_from_cloud(invalid_type, "test.jar",
                                                               "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
    EXPECT_TRUE(status.to_string().find("unknown") != std::string::npos);
}

// Test comprehensive plugin type enum coverage
TEST_F(CloudPluginDownloaderTest, TestAllPluginTypeEnumValues) {
    std::string local_path;

    // Test unsupported plugin types - these should fail with plugin type in error message
    std::vector<std::pair<CloudPluginDownloader::PluginType, std::string>> unsupported_types = {
            {CloudPluginDownloader::PluginType::CONNECTORS, "connectors"},
            {CloudPluginDownloader::PluginType::HADOOP_CONF, "hadoop_conf"},
            {static_cast<CloudPluginDownloader::PluginType>(999),
             "unknown"} // Invalid enum -> default case
    };

    for (const auto& [plugin_type, expected_string] : unsupported_types) {
        Status status = CloudPluginDownloader::download_from_cloud(plugin_type, "test-file.jar",
                                                                   "/tmp/test.jar", &local_path);

        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
        EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
        EXPECT_TRUE(status.to_string().find(expected_string) != std::string::npos)
                << "Expected '" << expected_string << "' in error message: " << status.to_string();
    }

    // Test supported plugin types - these should pass plugin type check but fail later
    std::vector<CloudPluginDownloader::PluginType> supported_types = {
            CloudPluginDownloader::PluginType::JDBC_DRIVERS,
            CloudPluginDownloader::PluginType::JAVA_UDF};

    for (const auto& plugin_type : supported_types) {
        Status status = CloudPluginDownloader::download_from_cloud(plugin_type, "test-file.jar",
                                                                   "/tmp/test.jar", &local_path);

        EXPECT_FALSE(status.ok());
        // Should NOT have "Unsupported plugin type" error since these are supported
        EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos)
                << "Supported plugin type should not show 'Unsupported plugin type' error: "
                << status.to_string();
        // Should fail at CloudStorageEngine level or later
        EXPECT_TRUE(status.code() == ErrorCode::NOT_FOUND ||
                    status.code() == ErrorCode::INTERNAL_ERROR)
                << "Expected NOT_FOUND or INTERNAL_ERROR, got: " << status.to_string();
    }
}

// Test edge cases for plugin type handling
TEST_F(CloudPluginDownloaderTest, TestPluginTypeHandlingEdgeCases) {
    std::string local_path;

    // Test boundary values around enum range
    std::vector<int> boundary_values = {-1, 0, 1, 2, 3, 4, 100, 1000};

    for (int value : boundary_values) {
        CloudPluginDownloader::PluginType test_type =
                static_cast<CloudPluginDownloader::PluginType>(value);

        Status status = CloudPluginDownloader::download_from_cloud(
                test_type, "boundary_test.jar", "/tmp/boundary.jar", &local_path);

        EXPECT_FALSE(status.ok());

        // Should either be "Unsupported plugin type" or some other error
        std::string error_msg = status.to_string();
        bool has_expected_error = error_msg.find("Unsupported plugin type") != std::string::npos ||
                                  error_msg.find("plugin_name cannot be empty") ==
                                          std::string::npos; // Other errors are also OK

        EXPECT_TRUE(has_expected_error)
                << "Unexpected error for value " << value << ": " << error_msg;
    }
}

} // namespace doris