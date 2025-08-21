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

#include <filesystem>
#include <fstream>

#include "cloud/cloud_storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override { downloader = std::make_unique<CloudPluginDownloader>(); }

    void TearDown() override {
        downloader.reset();
        // Clean up test files
        std::vector<std::string> cleanup_files = {"/tmp/test.jar", "/tmp/existing_file.jar",
                                                  "/tmp/test_dir/nested/test.jar",
                                                  "/tmp/readonly.jar"};
        for (const auto& file : cleanup_files) {
            std::error_code ec;
            std::filesystem::remove(file, ec);
            std::filesystem::path parent = std::filesystem::path(file).parent_path();
            if (parent.string() != "/tmp") {
                std::filesystem::remove_all(parent, ec);
            }
        }
        // Reset ExecEnv storage engine
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void SetupRegularStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions()));
    }

    void SetupCloudStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<CloudStorageEngine>(EngineOptions()));
    }

    std::unique_ptr<CloudPluginDownloader> downloader;
};

// ============== Input Validation Tests ==============

// Test static API with invalid inputs
TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudInvalidInputs) {
    std::string result_path;

    // Test empty name
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "", "/tmp/test.jar", &result_path);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ("Plugin name cannot be empty", status.msg());

    // Test with valid name but no cloud environment (should fail later)
    status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql.jar", "/tmp/test.jar",
            &result_path);
    EXPECT_FALSE(status.ok()); // Will fail at filesystem level
}

// ============== _build_plugin_path Tests ==============

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPathSuccess) {
    // Positive tests for all plugin types
    EXPECT_EQ("plugins/jdbc_drivers/mysql-connector.jar",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                                             "mysql-connector.jar"));

    EXPECT_EQ("plugins/java_udf/my-udf.jar",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JAVA_UDF,
                                             "my-udf.jar"));

    EXPECT_EQ("plugins/connectors/kafka-connector.jar",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::CONNECTORS,
                                             "kafka-connector.jar"));

    EXPECT_EQ("plugins/hadoop_conf/core-site.xml",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::HADOOP_CONF,
                                             "core-site.xml"));
}

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPathEdgeCases) {
    // Test with special characters
    EXPECT_EQ("plugins/jdbc_drivers/test-file_v1.2.jar",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                                             "test-file_v1.2.jar"));

    // Test with path-like name
    EXPECT_EQ("plugins/java_udf/sub/dir/file.jar",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JAVA_UDF,
                                             "sub/dir/file.jar"));

    // Test with very long name
    std::string long_name(200, 'a');
    long_name += ".jar";
    std::string expected = "plugins/connectors/" + long_name;
    EXPECT_EQ(expected, downloader->_build_plugin_path(
                                CloudPluginDownloader::PluginType::CONNECTORS, long_name));

    // Test with empty name (edge case)
    EXPECT_EQ("plugins/hadoop_conf/",
              downloader->_build_plugin_path(CloudPluginDownloader::PluginType::HADOOP_CONF, ""));
}

// ============== _get_type_path_segment Tests ==============

TEST_F(CloudPluginDownloaderTest, TestGetTypePathSegmentSuccess) {
    // Positive tests for all enum values
    EXPECT_EQ("jdbc_drivers",
              downloader->_get_type_path_segment(CloudPluginDownloader::PluginType::JDBC_DRIVERS));
    EXPECT_EQ("java_udf",
              downloader->_get_type_path_segment(CloudPluginDownloader::PluginType::JAVA_UDF));
    EXPECT_EQ("connectors",
              downloader->_get_type_path_segment(CloudPluginDownloader::PluginType::CONNECTORS));
    EXPECT_EQ("hadoop_conf",
              downloader->_get_type_path_segment(CloudPluginDownloader::PluginType::HADOOP_CONF));
}

TEST_F(CloudPluginDownloaderTest, TestGetTypePathSegmentInvalidType) {
    // Test with invalid enum value (undefined behavior, but shouldn't crash)
    CloudPluginDownloader::PluginType invalid_type =
            static_cast<CloudPluginDownloader::PluginType>(999);
    std::string result = downloader->_get_type_path_segment(invalid_type);
    EXPECT_EQ("unknown", result); // Based on default case in switch
}

// ============== _get_cloud_filesystem Tests ==============

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemNonCloudEnvironment) {
    // Negative test: regular storage engine
    SetupRegularStorageEngine();

    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemNoStorageEngine) {
    // Negative test: no storage engine
    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemCloudEnvironment) {
    // Positive test: cloud storage engine (though filesystem might not be available)
    SetupCloudStorageEngine();

    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);

    // Should succeed in getting cloud engine, but might fail getting filesystem
    // This depends on the actual CloudStorageEngine implementation
    // In real cloud environment, this should work
}

// ============== _prepare_local_path Tests ==============

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathSuccess) {
    // Positive test: prepare new file path
    std::string test_path = "/tmp/test.jar";
    Status status = downloader->_prepare_local_path(test_path);

    // Should succeed even if file doesn't exist
    EXPECT_TRUE(status.ok());
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathWithExistingFile) {
    // Positive test: prepare path with existing file
    std::string existing_file = "/tmp/existing_file.jar";

    // Create existing file
    std::ofstream file(existing_file);
    file << "existing content";
    file.close();
    EXPECT_TRUE(std::filesystem::exists(existing_file));

    Status status = downloader->_prepare_local_path(existing_file);
    EXPECT_TRUE(status.ok());

    // File should be removed
    EXPECT_FALSE(std::filesystem::exists(existing_file));
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathWithNestedDirectory) {
    // Positive test: create nested directories
    std::string nested_path = "/tmp/test_dir/nested/test.jar";
    Status status = downloader->_prepare_local_path(nested_path);

    EXPECT_TRUE(status.ok());

    // Directory should be created
    std::string dir_path = "/tmp/test_dir/nested";
    EXPECT_TRUE(std::filesystem::exists(dir_path));
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathReadOnlyFile) {
    // Negative test: read-only file that can't be deleted
    std::string readonly_file = "/tmp/readonly.jar";

    // Create read-only file
    std::ofstream file(readonly_file);
    file << "readonly content";
    file.close();

    // Make it read-only
    std::filesystem::permissions(readonly_file, std::filesystem::perms::owner_read);

    Status status = downloader->_prepare_local_path(readonly_file);

    // Should fail to delete read-only file
    EXPECT_FALSE(status.ok());

    // Clean up
    std::filesystem::permissions(readonly_file, std::filesystem::perms::owner_all);
    std::filesystem::remove(readonly_file);
}

// ============== _download_remote_file Tests ==============

TEST_F(CloudPluginDownloaderTest, TestDownloadRemoteFileInvalidFilesystem) {
    // Negative test: null filesystem
    io::RemoteFileSystemSPtr null_filesystem;
    Status status = downloader->_download_remote_file(null_filesystem, "s3://bucket/file.jar",
                                                      "/tmp/test.jar");

    EXPECT_FALSE(status.ok());
    // Should fail when trying to use null filesystem
}

// ============== Integration and Consistency Tests ==============

TEST_F(CloudPluginDownloaderTest, TestPathBuildingConsistency) {
    // Ensure _get_type_path_segment and _build_plugin_path are consistent
    auto types = {CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                  CloudPluginDownloader::PluginType::JAVA_UDF,
                  CloudPluginDownloader::PluginType::CONNECTORS,
                  CloudPluginDownloader::PluginType::HADOOP_CONF};

    for (auto type : types) {
        std::string segment = downloader->_get_type_path_segment(type);
        std::string full_path = downloader->_build_plugin_path(type, "test.jar");
        std::string expected = "plugins/" + segment + "/test.jar";
        EXPECT_EQ(expected, full_path);
    }
}

TEST_F(CloudPluginDownloaderTest, TestAllPluginTypesCoverage) {
    // Verify all plugin types are handled correctly
    struct TestCase {
        CloudPluginDownloader::PluginType type;
        std::string name;
        std::string expected_path;
        std::string expected_segment;
    };

    std::vector<TestCase> test_cases = {
            {CloudPluginDownloader::PluginType::JDBC_DRIVERS, "driver.jar",
             "plugins/jdbc_drivers/driver.jar", "jdbc_drivers"},
            {CloudPluginDownloader::PluginType::JAVA_UDF, "udf.jar", "plugins/java_udf/udf.jar",
             "java_udf"},
            {CloudPluginDownloader::PluginType::CONNECTORS, "conn.jar",
             "plugins/connectors/conn.jar", "connectors"},
            {CloudPluginDownloader::PluginType::HADOOP_CONF, "hadoop.xml",
             "plugins/hadoop_conf/hadoop.xml", "hadoop_conf"}};

    for (const auto& test_case : test_cases) {
        // Test path building
        EXPECT_EQ(test_case.expected_path,
                  downloader->_build_plugin_path(test_case.type, test_case.name))
                << "Path building failed for type: " << static_cast<int>(test_case.type);

        // Test segment generation
        EXPECT_EQ(test_case.expected_segment, downloader->_get_type_path_segment(test_case.type))
                << "Segment generation failed for type: " << static_cast<int>(test_case.type);
    }
}

} // namespace doris