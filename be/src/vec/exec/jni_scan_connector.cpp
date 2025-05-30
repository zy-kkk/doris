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

#include "jni_scan_connector.h"

#include <glog/logging.h>

#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/core/block.h"

namespace doris::vectorized {

Status JniScanner::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    ADD_TIMER(_profile, _connector_name);
    _open_scanner_time = ADD_CHILD_TIMER(_profile, "OpenScannerTime", _connector_name);
    _java_scan_time = ADD_CHILD_TIMER(_profile, "JavaScanTime", _connector_name);
    _java_append_data_time = ADD_CHILD_TIMER(_profile, "JavaAppendDataTime", _connector_name);
    _java_create_vector_table_time = ADD_CHILD_TIMER(_profile, "JavaCreateVectorTableTime", _connector_name);
    _fill_block_time = ADD_CHILD_TIMER(_profile, "FillBlockTime", _connector_name);
    _max_time_split_weight_counter = _profile->add_conditition_counter(
            "MaxTimeSplitWeight", TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; },
            _connector_name);
    _java_scan_watcher = 0;
    
    JNIEnv* env = nullptr;
    int batch_size = 0;
    if (!_is_table_schema) {
        batch_size = _state->batch_size();
    }
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    SCOPED_RAW_TIMER(&_jni_scanner_open_watcher);
    _scanner_params.emplace("time_zone", _state->timezone());
    RETURN_IF_ERROR(_init_jni_scanner(env, batch_size));
    
    env->CallVoidMethod(_jni_object, _jni_open);
    RETURN_ERROR_IF_EXC(env);
    _jni_object_initialized = true;
    return Status::OK();
}

Status JniScanner::init(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _generate_predicates(colname_to_value_range);
    return Status::OK();
}

Status JniScanner::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    long meta_address = 0;
    {
        SCOPED_RAW_TIMER(&_java_scan_watcher);
        SCOPED_TIMER(_java_scan_time);
        meta_address = env->CallLongMethod(_jni_object, _jni_scanner_get_next_batch);
    }
    RETURN_ERROR_IF_EXC(env);
    if (meta_address == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    _set_meta(meta_address);
    long num_rows = _table_meta.next_meta_as_long();
    if (num_rows == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_block(block, num_rows));
    *read_rows = num_rows;
    *eof = false;
    env->CallVoidMethod(_jni_object, _jni_release_table);
    RETURN_ERROR_IF_EXC(env);
    _has_read += num_rows;
    return Status::OK();
}

Status JniScanner::get_table_schema(std::string& table_schema_str) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    auto* jstr = static_cast<jstring>(env->CallObjectMethod(_jni_object, _jni_scanner_get_table_schema));
    RETURN_ERROR_IF_EXC(env);
    table_schema_str = env->GetStringUTFChars(jstr, nullptr);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

std::map<std::string, std::string> JniScanner::get_statistics(JNIEnv* env) {
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

Status JniScanner::close() {
    if (!_closed) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        if (_jni_object_initialized && _jni_object != nullptr) {
            COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
            COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);

            auto _append = static_cast<int64_t>(env->CallLongMethod(_jni_object, _jni_scanner_get_append_data_time));
            COUNTER_UPDATE(_java_append_data_time, _append);

            auto _create = static_cast<int64_t>(env->CallLongMethod(_jni_object, _jni_scanner_get_create_vector_table_time));
            COUNTER_UPDATE(_java_create_vector_table_time, _create);

            COUNTER_UPDATE(_java_scan_time, _java_scan_watcher - _append - _create);

            _max_time_split_weight_counter->conditional_update(
                    _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                    _self_split_weight);

            env->CallVoidMethod(_jni_object, _jni_release_table);
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

Status JniScanner::_init_jni_scanner(JNIEnv* env, int batch_size) {
    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_class));
    if (_jni_class == nullptr) {
        return Status::InternalError("Fail to get JniScanner class.");
    }

    jmethodID scanner_constructor = env->GetMethodID(_jni_class, "<init>", "(ILjava/util/Map;)V");
    RETURN_ERROR_IF_EXC(env);

    jobject hashmap_object = JniUtil::convert_to_java_map(env, _scanner_params);
    jobject jni_scanner_obj = env->NewObject(_jni_class, scanner_constructor, batch_size, hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    env->DeleteLocalRef(hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    _jni_open = env->GetMethodID(_jni_class, "open", "()V");
    _jni_scanner_get_next_batch = env->GetMethodID(_jni_class, "getNextBatchMeta", "()J");
    _jni_scanner_get_append_data_time = env->GetMethodID(_jni_class, "getAppendDataTime", "()J");
    _jni_scanner_get_create_vector_table_time = env->GetMethodID(_jni_class, "getCreateVectorTableTime", "()J");
    _jni_scanner_get_table_schema = env->GetMethodID(_jni_class, "getTableSchema", "()Ljava/lang/String;");
    _jni_close = env->GetMethodID(_jni_class, "close", "()V");
    _jni_release_column = env->GetMethodID(_jni_class, "releaseColumn", "(I)V");
    _jni_release_table = env->GetMethodID(_jni_class, "releaseTable", "()V");
    _jni_get_statistics = env->GetMethodID(_jni_class, "getStatistics", "()Ljava/util/Map;");
    RETURN_ERROR_IF_EXC(env);

    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_scanner_obj, &_jni_object));
    env->DeleteLocalRef(jni_scanner_obj);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniScanner::_fill_block(Block* block, size_t num_rows) {
    SCOPED_RAW_TIMER(&_fill_block_watcher);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    for (int i = 0; i < _column_names.size(); ++i) {
        auto& column_with_type_and_name = block->get_by_name(_column_names[i]);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(_fill_column(_table_meta, column_ptr, column_type, num_rows));
        env->CallVoidMethod(_jni_object, _jni_release_column, i);
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

void JniScanner::_generate_predicates(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (colname_to_value_range == nullptr) {
        return;
    }
    for (const auto& kv : *colname_to_value_range) {
        const std::string& column_name = kv.first;
        const ColumnValueRangeType& col_val_range = kv.second;
        std::visit([&](auto&& range) { _parse_value_range(range, column_name); }, col_val_range);
    }
}

} // namespace doris::vectorized 