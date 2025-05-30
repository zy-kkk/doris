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

#pragma once

#include "jni_connector.h"

namespace doris::vectorized {

/**
 * JNI Writer implementation for writing data
 */
class JniWriter : public IJniConnector {
public:
    JniWriter(std::string connector_class, std::map<std::string, std::string> scanner_params)
            : IJniConnector(std::move(connector_class), std::move(scanner_params)) {}

    ~JniWriter() override = default;

    /**
     * Open writer
     */
    Status open_writer(RuntimeState* state, RuntimeProfile* profile);

    /**
     * Write block data
     */
    Status write(Block* block);

    /**
     * Finish writing
     */
    Status finish();

    /**
     * Get performance metrics from java writer
     */
    std::map<std::string, std::string> get_statistics(JNIEnv* env) override;

    /**
     * Close writer and release jni resources.
     */
    Status close() override;

private:
    // Writer JNI method IDs
    jmethodID _jni_writer_write_data = nullptr;
    jmethodID _jni_writer_finish_write = nullptr;

    // Writer specific timers
    RuntimeProfile::Counter* _open_writer_time = nullptr;
    RuntimeProfile::Counter* _write_data_time = nullptr;
    RuntimeProfile::Counter* _finish_write_time = nullptr;

    Status _init_jni_writer(JNIEnv* env);
};

} // namespace doris::vectorized 