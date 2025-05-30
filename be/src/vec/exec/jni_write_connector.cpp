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

#include "jni_write_connector.h"

#include <glog/logging.h>

#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/core/block.h"

namespace doris::vectorized {

Status JniWriter::open_writer(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    ADD_TIMER(_profile, _connector_name);
    _open_writer_time = ADD_CHILD_TIMER(_profile, "OpenWriterTime", _connector_name);
    _write_data_time = ADD_CHILD_TIMER(_profile, "WriteTime", _connector_name);
    _finish_write_time = ADD_CHILD_TIMER(_profile, "FinishTime", _connector_name);

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    SCOPED_TIMER(_open_writer_time);
    _scanner_params.emplace("time_zone", _state->timezone());
    RETURN_IF_ERROR(_init_jni_writer(env));
    
    env->CallVoidMethod(_jni_object, _jni_open);
    RETURN_ERROR_IF_EXC(env);
    _jni_object_initialized = true;
    return Status::OK();
}

Status JniWriter::write(Block* block) {
    if (!_jni_object_initialized) {
        return Status::InternalError("Writer is not opened.");
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    SCOPED_TIMER(_write_data_time);

    std::unique_ptr<long[]> meta_data;
    RETURN_IF_ERROR(IJniConnector::to_java_table(block, meta_data));
    long meta_address = reinterpret_cast<long>(meta_data.get());
    auto table_schema = IJniConnector::parse_table_schema(block);

    std::map<std::string, std::string> write_params = {
        {"meta_address", std::to_string(meta_address)},
        {"required_fields", table_schema.first},
        {"columns_types", table_schema.second}
    };

    jobject hashmap_object = JniUtil::convert_to_java_map(env, write_params);
    env->CallVoidMethod(_jni_object, _jni_writer_write_data, hashmap_object);
    env->DeleteLocalRef(hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    return Status::OK();
}

Status JniWriter::finish() {
    if (!_jni_object_initialized) {
        return Status::InternalError("Writer is not opened.");
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    SCOPED_TIMER(_finish_write_time);

    env->CallVoidMethod(_jni_object, _jni_writer_finish_write);
    RETURN_ERROR_IF_EXC(env);

    return Status::OK();
}

std::map<std::string, std::string> JniWriter::get_statistics(JNIEnv* env) {
    if (_jni_get_statistics == nullptr || _jni_object == nullptr) {
        return std::map<std::string, std::string> {};
    }

    try {
        jobject metrics = env->CallObjectMethod(_jni_object, _jni_get_statistics);
        if (env->ExceptionCheck()) {
            LOG(WARNING) << "Exception in get_statistics: " << JniUtil::GetJniExceptionMsg(env).to_string();
            env->ExceptionClear();
            if (metrics != nullptr) {
                env->DeleteLocalRef(metrics);
            }
            return std::map<std::string, std::string> {};
        }

        if (metrics == nullptr) {
            return std::map<std::string, std::string> {};
        }

        std::map<std::string, std::string> result = JniUtil::convert_to_cpp_map(env, metrics);
        env->DeleteLocalRef(metrics);
        return result;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception in get_statistics: " << e.what();
        return std::map<std::string, std::string> {};
    } catch (...) {
        LOG(WARNING) << "Unknown exception in get_statistics";
        return std::map<std::string, std::string> {};
    }
}

Status JniWriter::close() {
    if (!_closed) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        if (_jni_object_initialized && _jni_object != nullptr) {
            env->CallVoidMethod(_jni_object, _jni_close);
            env->DeleteGlobalRef(_jni_object);
        }
        if (_jni_class != nullptr) {
            env->DeleteGlobalRef(_jni_class);
        }
        _closed = true;
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

Status JniWriter::_init_jni_writer(JNIEnv* env) {
    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_class));
    if (_jni_class == nullptr) {
        return Status::InternalError("Fail to get JniWriter class.");
    }

    jmethodID writer_constructor = env->GetMethodID(_jni_class, "<init>", "(Ljava/util/Map;)V");
    RETURN_ERROR_IF_EXC(env);

    jobject hashmap_object = JniUtil::convert_to_java_map(env, _scanner_params);
    jobject jni_writer_obj = env->NewObject(_jni_class, writer_constructor, hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    env->DeleteLocalRef(hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    _jni_open = env->GetMethodID(_jni_class, "open", "()V");
    _jni_writer_write_data = env->GetMethodID(_jni_class, "write", "(Ljava/util/Map;)V");
    _jni_writer_finish_write = env->GetMethodID(_jni_class, "finish", "()V");
    _jni_close = env->GetMethodID(_jni_class, "close", "()V");
    _jni_release_column = env->GetMethodID(_jni_class, "releaseColumn", "(I)V");
    _jni_release_table = env->GetMethodID(_jni_class, "releaseTable", "()V");
    _jni_get_statistics = env->GetMethodID(_jni_class, "getStatistics", "()Ljava/util/Map;");
    RETURN_ERROR_IF_EXC(env);

    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_writer_obj, &_jni_object));
    env->DeleteLocalRef(jni_writer_obj);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

} // namespace doris::vectorized 