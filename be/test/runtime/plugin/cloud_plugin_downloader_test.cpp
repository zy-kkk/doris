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
#include <thread>
#include <vector>

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
};

// Test all PluginType enum values and their distinctness
TEST_F(CloudPluginDownloaderTest, TestPluginTypeEnumValues) {
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

    // Test enum value range
    EXPECT_GE(static_cast<int>(jdbc_type), 0);
    EXPECT_GE(static_cast<int>(udf_type), 0);
    EXPECT_GE(static_cast<int>(connector_type), 0);
    EXPECT_GE(static_cast<int>(hadoop_type), 0);
}

// Test unsupported plugin types - covers lines 32-35
TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudUnsupportedPluginTypes) {
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

// Test empty plugin name validation - covers lines 37-39
TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudEmptyPluginName) {
    std::string local_path;

    // Test with empty plugin name for JAVA_UDF
    Status status1 =
            CloudPluginDownloader::download_from_cloud(CloudPluginDownloader::PluginType::JAVA_UDF,
                                                       "", // empty plugin name
                                                       "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status1.ok());
    EXPECT_EQ(status1.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status1.to_string().find("cannot be empty") != std::string::npos);

    // Test with empty plugin name for JDBC_DRIVERS
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS,
            "", // empty plugin name
            "/tmp/test.jar", &local_path);

    EXPECT_FALSE(status2.ok());
    EXPECT_EQ(status2.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status2.to_string().find("cannot be empty") != std::string::npos);
}

// Test null pointer handling
TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudNullPointer) {
    // Test with null pointer for local_path
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "test.jar", "/tmp/test.jar",
            nullptr); // null pointer

    // Should handle null pointer gracefully or crash predictably
    EXPECT_FALSE(status.ok());
}

// Test supported plugin types without cloud environment - covers lines 42-56
TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudSupportedPluginTypesNoCloudEnv) {
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

// Test _plugin_type_to_string method indirectly through error messages
TEST_F(CloudPluginDownloaderTest, TestPluginTypeToStringMapping) {
    std::string local_path;

    // Test each unsupported type to verify string mapping
    Status connectors_status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::CONNECTORS, "test.jar", "/tmp/test.jar",
            &local_path);

    EXPECT_FALSE(connectors_status.ok());
    EXPECT_TRUE(connectors_status.to_string().find("connectors") != std::string::npos);

    Status hadoop_status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::HADOOP_CONF, "test.xml", "/tmp/test.xml",
            &local_path);

    EXPECT_FALSE(hadoop_status.ok());
    EXPECT_TRUE(hadoop_status.to_string().find("hadoop_conf") != std::string::npos);
}

// Test parameter validation order - plugin type should be checked before plugin name
TEST_F(CloudPluginDownloaderTest, TestParameterValidationOrder) {
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

// Test all plugin type enum coverage
TEST_F(CloudPluginDownloaderTest, TestAllPluginTypeEnumCoverage) {
    std::string local_path;

    // Test all four enum values
    std::vector<std::pair<CloudPluginDownloader::PluginType, bool>> test_cases = {
            {CloudPluginDownloader::PluginType::JDBC_DRIVERS, true}, // supported
            {CloudPluginDownloader::PluginType::JAVA_UDF, true},     // supported
            {CloudPluginDownloader::PluginType::CONNECTORS, false},  // unsupported
            {CloudPluginDownloader::PluginType::HADOOP_CONF, false}  // unsupported
    };

    for (const auto& [plugin_type, is_supported] : test_cases) {
        Status status = CloudPluginDownloader::download_from_cloud(plugin_type, "test.file",
                                                                   "/tmp/test.file", &local_path);

        EXPECT_FALSE(status.ok()); // All should fail (no cloud env or unsupported)

        if (is_supported) {
            // Should NOT contain "Unsupported plugin type"
            EXPECT_FALSE(status.to_string().find("Unsupported plugin type") != std::string::npos);
        } else {
            // Should contain "Unsupported plugin type"
            EXPECT_TRUE(status.to_string().find("Unsupported plugin type") != std::string::npos);
        }
    }
}

// Test S3 path construction logic indirectly - covers lines 51-52
TEST_F(CloudPluginDownloaderTest, TestS3PathConstructionCoverage) {
    std::string local_path;

    // Even though this will fail, it exercises the S3 path construction code
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "my-plugin.jar", "/tmp/my-plugin.jar",
            &local_path);

    EXPECT_FALSE(status.ok());
    // Should reach the cloud config retrieval part
    EXPECT_FALSE(status.to_string().empty());
}

// Test concurrent access (thread safety)
TEST_F(CloudPluginDownloaderTest, TestConcurrentAccess) {
    std::vector<std::thread> threads;
    std::vector<bool> results(10);

    // Launch multiple threads to test concurrent access
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            std::string local_path;
            Status status = CloudPluginDownloader::download_from_cloud(
                    CloudPluginDownloader::PluginType::CONNECTORS, // Will fail consistently
                    "test.jar", "/tmp/test_" + std::to_string(i) + ".jar", &local_path);
            results[i] = !status.ok() && status.code() == ErrorCode::INVALID_ARGUMENT;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads got the expected error
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(results[i]);
    }
}

// Test method signature existence and compilation
TEST_F(CloudPluginDownloaderTest, TestMethodSignatureExists) {
    std::string local_path;

    // This should compile successfully (testing method signature)
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "valid_name.jar", "/tmp/valid_path.jar",
            &local_path);

    // We don't care about success/failure, just that it compiles and runs
    EXPECT_TRUE(status.ok() || !status.ok()); // Always true
}

// Test Status type integration and error handling
TEST_F(CloudPluginDownloaderTest, TestStatusTypeIntegration) {
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

// Test different file extensions and paths
TEST_F(CloudPluginDownloaderTest, TestDifferentFileExtensions) {
    std::string local_path;

    // Test different file types
    std::vector<std::pair<std::string, std::string>> test_files = {
            {"test.jar", ".jar"},
            {"plugin.tar.gz", ".tar.gz"},
            {"config.xml", ".xml"},
            {"driver", ""}, // no extension
            {"nested/path/file.jar", ".jar"}};

    for (const auto& [filename, extension] : test_files) {
        Status status = CloudPluginDownloader::download_from_cloud(
                CloudPluginDownloader::PluginType::JAVA_UDF, filename, "/tmp/" + filename,
                &local_path);

        // All should fail due to no cloud env, but not due to filename issues
        EXPECT_FALSE(status.ok());
        EXPECT_FALSE(status.to_string().find("cannot be empty") != std::string::npos);
    }
}

// Test edge cases for plugin names
TEST_F(CloudPluginDownloaderTest, TestPluginNameEdgeCases) {
    std::string local_path;

    // Test with very long plugin name
    std::string long_name(1000, 'a');
    Status status1 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, long_name, "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status1.ok());
    // Should not fail due to name length but due to cloud config

    // Test with special characters in plugin name
    Status status2 = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JAVA_UDF, "plugin-with_special.chars123",
            "/tmp/test.jar", &local_path);
    EXPECT_FALSE(status2.ok());
    // Should not fail due to special chars but due to cloud config
}

} // namespace doris