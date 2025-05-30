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

#include "jni_connector.h"
#include "jni_scan_connector.h"
#include "jni_write_connector.h"

#include <glog/logging.h>

#include <sstream>
#include <variant>

#include "jni.h"
#include "runtime/decimalv2_value.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"

namespace doris {
class RuntimeProfile;
} // namespace doris

namespace doris::vectorized {

#define FOR_FIXED_LENGTH_TYPES(M)                                           \
    M(PrimitiveType::TYPE_TINYINT, ColumnVector<Int8>, Int8)                \
    M(PrimitiveType::TYPE_BOOLEAN, ColumnVector<UInt8>, UInt8)              \
    M(PrimitiveType::TYPE_SMALLINT, ColumnVector<Int16>, Int16)             \
    M(PrimitiveType::TYPE_INT, ColumnVector<Int32>, Int32)                  \
    M(PrimitiveType::TYPE_BIGINT, ColumnVector<Int64>, Int64)               \
    M(PrimitiveType::TYPE_LARGEINT, ColumnVector<Int128>, Int128)           \
    M(PrimitiveType::TYPE_FLOAT, ColumnVector<Float32>, Float32)            \
    M(PrimitiveType::TYPE_DOUBLE, ColumnVector<Float64>, Float64)           \
    M(PrimitiveType::TYPE_DECIMALV2, ColumnDecimal<Decimal128V2>, Int128)   \
    M(PrimitiveType::TYPE_DECIMAL128I, ColumnDecimal<Decimal128V3>, Int128) \
    M(PrimitiveType::TYPE_DECIMAL32, ColumnDecimal<Decimal<Int32>>, Int32)  \
    M(PrimitiveType::TYPE_DECIMAL64, ColumnDecimal<Decimal<Int64>>, Int64)  \
    M(PrimitiveType::TYPE_DATE, ColumnVector<Int64>, Int64)                 \
    M(PrimitiveType::TYPE_DATEV2, ColumnVector<UInt32>, UInt32)             \
    M(PrimitiveType::TYPE_DATETIME, ColumnVector<Int64>, Int64)             \
    M(PrimitiveType::TYPE_DATETIMEV2, ColumnVector<UInt64>, UInt64)         \
    M(PrimitiveType::TYPE_IPV4, ColumnVector<IPv4>, IPv4)                   \
    M(PrimitiveType::TYPE_IPV6, ColumnVector<IPv6>, IPv6)

// =================== IJniConnector Base Implementation ===================

void IJniConnector::_collect_profile_before_close() {
    if (_jni_object_initialized && _profile != nullptr) {
        JNIEnv* env = nullptr;
        Status st = JniUtil::GetJNIEnv(&env);
        if (!st) {
            LOG(WARNING) << "failed to get jni env when collect profile: " << st;
            return;
        }
        for (const auto& metric : get_statistics(env)) {
            std::vector<std::string> type_and_name = split(metric.first, ":");
            if (type_and_name.size() != 2) {
                LOG(WARNING) << "Name of JNI metric should be pattern like 'metricType:metricName'";
                continue;
            }
            long metric_value = std::stol(metric.second);
            RuntimeProfile::Counter* counter;
            if (type_and_name[0] == "timer") {
                counter = ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name);
            } else if (type_and_name[0] == "counter") {
                counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT, _connector_name);
            } else if (type_and_name[0] == "bytes") {
                counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES, _connector_name);
            } else {
                LOG(WARNING) << "Type of JNI metric should be timer, counter or bytes";
                continue;
            }
            COUNTER_UPDATE(counter, metric_value);
        }
    }
}

// =================== JniConnector Wrapper Implementation ===================

JniConnector::JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params,
                           std::vector<std::string> column_names, int64_t self_split_weight) {
    _impl = std::make_unique<JniScanner>(std::move(connector_class), std::move(scanner_params),
                                         std::move(column_names), self_split_weight);
    _is_writer = false;
}

JniConnector::JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params) {
    _impl = std::make_unique<JniScanner>(std::move(connector_class), std::move(scanner_params));
    _is_writer = false;
}

Status JniConnector::open(RuntimeState* state, RuntimeProfile* profile) {
    if (!_is_writer) {
        return static_cast<JniScanner*>(_impl.get())->open(state, profile);
    }
    return Status::InternalError("Cannot call open on writer mode");
}

Status JniConnector::init(const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (!_is_writer) {
        return static_cast<JniScanner*>(_impl.get())->init(colname_to_value_range);
    }
    return Status::InternalError("Cannot call init on writer mode");
}

Status JniConnector::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (!_is_writer) {
        return static_cast<JniScanner*>(_impl.get())->get_next_block(block, read_rows, eof);
    }
    return Status::InternalError("Cannot call get_next_block on writer mode");
}

Status JniConnector::get_table_schema(std::string& table_schema_str) {
    if (!_is_writer) {
        return static_cast<JniScanner*>(_impl.get())->get_table_schema(table_schema_str);
    }
    return Status::InternalError("Cannot call get_table_schema on writer mode");
}

Status JniConnector::open_writer(RuntimeState* state, RuntimeProfile* profile) {
    if (_impl == nullptr) {
        _impl = std::make_unique<JniWriter>("", std::map<std::string, std::string>());
        _is_writer = true;
    }
    if (_is_writer) {
        return static_cast<JniWriter*>(_impl.get())->open_writer(state, profile);
    }
    return Status::InternalError("Cannot call open_writer on scanner mode");
}

Status JniConnector::write(Block* block) {
    if (_is_writer) {
        return static_cast<JniWriter*>(_impl.get())->write(block);
    }
    return Status::InternalError("Cannot call write on scanner mode");
}

Status JniConnector::finish() {
    if (_is_writer) {
        return static_cast<JniWriter*>(_impl.get())->finish();
    }
    return Status::InternalError("Cannot call finish on scanner mode");
}

std::map<std::string, std::string> JniConnector::get_statistics(JNIEnv* env) {
    return _impl->get_statistics(env);
}

Status JniConnector::close() {
    return _impl->close();
}

std::string JniConnector::get_jni_type(const DataTypePtr& data_type) {
    return IJniConnector::get_jni_type(data_type);
}

std::string JniConnector::get_jni_type_with_different_string(const DataTypePtr& data_type) {
    return IJniConnector::get_jni_type_with_different_string(data_type);
}

Status JniConnector::to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                   std::unique_ptr<long[]>& meta) {
    return IJniConnector::to_java_table(block, num_rows, arguments, meta);
}

Status JniConnector::to_java_table(Block* block, std::unique_ptr<long[]>& meta) {
    return IJniConnector::to_java_table(block, meta);
}

std::pair<std::string, std::string> JniConnector::parse_table_schema(Block* block,
                                                                     const ColumnNumbers& arguments,
                                                                     bool ignore_column_name) {
    return IJniConnector::parse_table_schema(block, arguments, ignore_column_name);
}

std::pair<std::string, std::string> JniConnector::parse_table_schema(Block* block) {
    return IJniConnector::parse_table_schema(block);
}

Status JniConnector::fill_block(Block* block, const ColumnNumbers& arguments, long table_address) {
    return IJniConnector::fill_block(block, arguments, table_address);
}

} // namespace doris::vectorized

// =================== Static Utility Methods Implementation ===================

namespace doris::vectorized {

std::string IJniConnector::get_jni_type(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR:
        [[fallthrough]];
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_STRING:
        return "string";
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        auto* struct_type = reinterpret_cast<const DataTypeStruct*>(type.get());
        buffer << "struct<";
        for (int i = 0; i < struct_type->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << struct_type->get_element_names()[i] << ":"
                   << get_jni_type(struct_type->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        auto* array_type = reinterpret_cast<const DataTypeArray*>(type.get());
        buffer << "array<" << get_jni_type(array_type->get_nested_type()) << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        auto* map_type = reinterpret_cast<const DataTypeMap*>(type.get());
        buffer << "map<" << get_jni_type(map_type->get_key_type()) << ","
               << get_jni_type(map_type->get_value_type()) << ">";
        return buffer.str();
    }
    default:
        return "unsupported";
    }
}

std::string IJniConnector::get_jni_type_with_different_string(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (data_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR: {
        buffer << "varchar("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << data_type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_CHAR: {
        buffer << "char("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_STRING:
        return "string";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        const auto* type_struct =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        buffer << "struct<";
        for (int i = 0; i < type_struct->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << type_struct->get_element_name(i) << ":"
                   << get_jni_type_with_different_string(type_struct->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        const auto* type_arr = assert_cast<const DataTypeArray*>(remove_nullable(data_type).get());
        buffer << "array<" << get_jni_type_with_different_string(type_arr->get_nested_type())
               << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        const auto* type_map = assert_cast<const DataTypeMap*>(remove_nullable(data_type).get());
        buffer << "map<" << get_jni_type_with_different_string(type_map->get_key_type()) << ","
               << get_jni_type_with_different_string(type_map->get_value_type()) << ">";
        return buffer.str();
    }
    default:
        return "unsupported";
    }
}

Status IJniConnector::to_java_table(Block* block, std::unique_ptr<long[]>& meta) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return to_java_table(block, block->rows(), arguments, meta);
}

Status IJniConnector::to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                   std::unique_ptr<long[]>& meta) {
    std::vector<long> meta_data;
    meta_data.emplace_back(num_rows);
    for (size_t i : arguments) {
        auto& column_with_type_and_name = block->get_by_position(i);
        RETURN_IF_ERROR(_fill_column_meta(column_with_type_and_name.column,
                                          column_with_type_and_name.type, meta_data));
    }

    meta.reset(new long[meta_data.size()]);
    memcpy(meta.get(), meta_data.data(), meta_data.size() * 8);
    return Status::OK();
}

std::pair<std::string, std::string> IJniConnector::parse_table_schema(Block* block,
                                                                     const ColumnNumbers& arguments,
                                                                     bool ignore_column_name) {
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    for (int i = 0; i < arguments.size(); ++i) {
        std::string type = IJniConnector::get_jni_type(block->get_by_position(arguments[i]).type);
        if (i == 0) {
            if (ignore_column_name) {
                required_fields << "_col_" << arguments[i];
            } else {
                required_fields << block->get_by_position(arguments[i]).name;
            }
            columns_types << type;
        } else {
            if (ignore_column_name) {
                required_fields << ","
                                << "_col_" << arguments[i];
            } else {
                required_fields << "," << block->get_by_position(arguments[i]).name;
            }
            columns_types << "#" << type;
        }
    }
    return std::make_pair(required_fields.str(), columns_types.str());
}

std::pair<std::string, std::string> IJniConnector::parse_table_schema(Block* block) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return parse_table_schema(block, arguments, true);
}

Status IJniConnector::fill_block(Block* block, const ColumnNumbers& arguments, long table_address) {
    if (table_address == 0) {
        return Status::InternalError("table_address is 0");
    }
    TableMetaAddress table_meta(table_address);
    long num_rows = table_meta.next_meta_as_long();
    for (size_t i : arguments) {
        if (block->get_by_position(i).column.get() == nullptr) {
            auto return_type = block->get_data_type(i);
            bool result_nullable = return_type->is_nullable();
            ColumnUInt8::MutablePtr null_col = nullptr;
            if (result_nullable) {
                return_type = remove_nullable(return_type);
                null_col = ColumnUInt8::create();
            }
            auto res_col = return_type->create_column();
            if (result_nullable) {
                block->replace_by_position(
                        i, ColumnNullable::create(std::move(res_col), std::move(null_col)));
            } else {
                block->replace_by_position(i, std::move(res_col));
            }
        } else if (is_column_const(*(block->get_by_position(i).column))) {
            auto doris_column = block->get_by_position(i).column->convert_to_full_column_if_const();
            bool is_nullable = block->get_by_position(i).type->is_nullable();
            block->replace_by_position(i, is_nullable ? make_nullable(doris_column) : doris_column);
        }
        auto& column_with_type_and_name = block->get_by_position(i);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(_fill_column(table_meta, column_ptr, column_type, num_rows));
    }
    return Status::OK();
}

// TODO: Add remaining static utility methods (_fill_column, _fill_string_column, etc.)
// These would be copied from the original backup file but are quite lengthy

} // namespace doris::vectorized 