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

#include <jni.h>
#include <cstring>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "exec/olap_utils.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/profile_collector.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
template <typename T>
class ColumnDecimal;
template <typename T>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

// Forward declarations
class JniScanner;
class JniWriter;

/**
 * Base interface for JNI connectors
 */
class IJniConnector : public ProfileCollector {
public:
    class TableMetaAddress {
    private:
        long* _meta_ptr;
        int _meta_index;

    public:
        TableMetaAddress() {
            _meta_ptr = nullptr;
            _meta_index = 0;
        }

        TableMetaAddress(long meta_addr) {
            _meta_ptr = static_cast<long*>(reinterpret_cast<void*>(meta_addr));
            _meta_index = 0;
        }

        void set_meta(long meta_addr) {
            _meta_ptr = static_cast<long*>(reinterpret_cast<void*>(meta_addr));
            _meta_index = 0;
        }

        long next_meta_as_long() { return _meta_ptr[_meta_index++]; }

        void* next_meta_as_ptr() { return reinterpret_cast<void*>(_meta_ptr[_meta_index++]); }
    };

    /**
     * The predicates that can be pushed down to java side.
     * Reference to java class org.apache.doris.common.jni.vec.ScanPredicate
     */
    template <typename CppType>
    struct ScanPredicate {
        ScanPredicate() = default;
        ~ScanPredicate() = default;
        std::string column_name;
        SQLFilterOp op;
        std::vector<const CppType*> values;
        int scale;

        explicit ScanPredicate(std::string column_name) : column_name(std::move(column_name)) {}

        ScanPredicate(const ScanPredicate& other)
                : column_name(other.column_name), op(other.op), scale(other.scale) {
            for (auto v : other.values) {
                values.emplace_back(v);
            }
        }

        int length() {
            // name_length(4) + column_name + operator(4) + scale(4) + num_values(4)
            int len = 4 + column_name.size() + 4 + 4 + 4;
            if constexpr (std::is_same_v<CppType, StringRef>) {
                for (const StringRef* s : values) {
                    // string_length(4) + string
                    len += 4 + s->size;
                }
            } else {
                int type_len = sizeof(CppType);
                // value_length(4) + value
                len += (4 + type_len) * values.size();
            }
            return len;
        }

        /**
         * The value ranges can be stored as byte array as following format:
         * number_filters(4) | length(4) | column_name | op(4) | scale(4) | num_values(4) | value_length(4) | value | ...
         * The read method is implemented in org.apache.doris.common.jni.vec.ScanPredicate#parseScanPredicates
         */
        int write(std::unique_ptr<char[]>& predicates, int origin_length) {
            int num_filters = 0;
            if (origin_length != 0) {
                num_filters = *reinterpret_cast<int*>(predicates.get());
            } else {
                origin_length = 4;
            }
            num_filters += 1;
            int new_length = origin_length + length();
            char* new_bytes = new char[new_length];
            if (origin_length != 4) {
                memcpy(new_bytes, predicates.get(), origin_length);
            }
            *reinterpret_cast<int*>(new_bytes) = num_filters;

            char* char_ptr = new_bytes + origin_length;
            *reinterpret_cast<int*>(char_ptr) = column_name.size();
            char_ptr += 4;
            memcpy(char_ptr, column_name.data(), column_name.size());
            char_ptr += column_name.size();
            *reinterpret_cast<int*>(char_ptr) = op;
            char_ptr += 4;
            *reinterpret_cast<int*>(char_ptr) = scale;
            char_ptr += 4;
            *reinterpret_cast<int*>(char_ptr) = values.size();
            char_ptr += 4;
            if constexpr (std::is_same_v<CppType, StringRef>) {
                for (const StringRef* s : values) {
                    *reinterpret_cast<int*>(char_ptr) = s->size;
                    char_ptr += 4;
                    memcpy(char_ptr, s->data, s->size);
                    char_ptr += s->size;
                }
            } else {
                // FIXME: it can not handle decimal type correctly.
                // but this logic is deprecated and not used.
                // so may be deleted or fixed later.
                for (const CppType* v : values) {
                    int type_len = sizeof(CppType);
                    *reinterpret_cast<int*>(char_ptr) = type_len;
                    char_ptr += 4;
                    *reinterpret_cast<CppType*>(char_ptr) = *v;
                    char_ptr += type_len;
                }
            }

            predicates.reset(new_bytes);
            return new_length;
        }
    };

    IJniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params)
            : _connector_class(std::move(connector_class)),
              _scanner_params(std::move(scanner_params)) {
        // Use java class name as connector name
        _connector_name = split(_connector_class, "/").back();
    }

    ~IJniConnector() override = default;

    /**
     * Close connector and release jni resources.
     */
    virtual Status close() = 0;

    /**
     * Get performance metrics from java scanner/writer
     */
    virtual std::map<std::string, std::string> get_statistics(JNIEnv* env) = 0;

    // Static utility methods
    static std::string get_jni_type(const DataTypePtr& data_type);
    static std::string get_jni_type_with_different_string(const DataTypePtr& data_type);

    static Status to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                std::unique_ptr<long[]>& meta);

    static Status to_java_table(Block* block, std::unique_ptr<long[]>& meta);

    static std::pair<std::string, std::string> parse_table_schema(Block* block,
                                                                  const ColumnNumbers& arguments,
                                                                  bool ignore_column_name = true);

    static std::pair<std::string, std::string> parse_table_schema(Block* block);

    static Status fill_block(Block* block, const ColumnNumbers& arguments, long table_address);

protected:
    void _collect_profile_before_close() override;

    std::string _connector_name;
    std::string _connector_class;
    std::map<std::string, std::string> _scanner_params;

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

    // General JNI objects and classes
    bool _closed = false;
    bool _jni_object_initialized = false;
    jclass _jni_class;
    jobject _jni_object;

    // General JNI method IDs
    jmethodID _jni_open;
    jmethodID _jni_close;
    jmethodID _jni_get_statistics;
    jmethodID _jni_release_column;
    jmethodID _jni_release_table;

    TableMetaAddress _table_meta;

    // Static utility methods
    static Status _fill_column(TableMetaAddress& address, ColumnPtr& doris_column,
                               DataTypePtr& data_type, size_t num_rows);

    static Status _fill_string_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      size_t num_rows);

    static Status _fill_map_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                   DataTypePtr& data_type, size_t num_rows);

    static Status _fill_array_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                     DataTypePtr& data_type, size_t num_rows);

    static Status _fill_struct_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      DataTypePtr& data_type, size_t num_rows);

    static Status _fill_column_meta(const ColumnPtr& doris_column, const DataTypePtr& data_type,
                                    std::vector<long>& meta_data);

    template <typename COLUMN_TYPE, typename CPP_TYPE>
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy(column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE>
    static long _get_fixed_length_column_address(const IColumn& doris_column) {
        return reinterpret_cast<long>(assert_cast<const COLUMN_TYPE&>(doris_column).get_data().data());
    }
};

/**
 * Backward compatibility wrapper for JniConnector
 * This class maintains the original interface while delegating to the appropriate implementation
 */
class JniConnector {
public:
    // Scanner constructor
    JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params,
                 std::vector<std::string> column_names, int64_t self_split_weight = -1);

    // Table schema constructor
    JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params);

    ~JniConnector() = default;

    // Scanner methods
    Status open(RuntimeState* state, RuntimeProfile* profile);
    Status init(const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof);
    Status get_table_schema(std::string& table_schema_str);

    // Writer methods
    Status open_writer(RuntimeState* state, RuntimeProfile* profile);
    Status write(Block* block);
    Status finish();

    // Common methods
    std::map<std::string, std::string> get_statistics(JNIEnv* env);
    Status close();

    // Static utility methods
    static std::string get_jni_type(const DataTypePtr& data_type);
    static std::string get_jni_type_with_different_string(const DataTypePtr& data_type);
    static Status to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                std::unique_ptr<long[]>& meta);
    static Status to_java_table(Block* block, std::unique_ptr<long[]>& meta);
    static std::pair<std::string, std::string> parse_table_schema(Block* block,
                                                                  const ColumnNumbers& arguments,
                                                                  bool ignore_column_name = true);
    static std::pair<std::string, std::string> parse_table_schema(Block* block);
    static Status fill_block(Block* block, const ColumnNumbers& arguments, long table_address);

private:
    std::unique_ptr<IJniConnector> _impl;
    bool _is_writer = false;
};

} // namespace doris::vectorized
