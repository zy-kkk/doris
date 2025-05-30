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
 * JNI Scanner implementation for reading data
 */
class JniScanner : public IJniConnector {
public:
    /**
     * Use configuration map to provide scan information. The java side should determine how the parameters
     * are parsed. For example, using "required_fields=col0,col1,...,colN" to provide the scan fields.
     * @param connector_class Java scanner class
     * @param scanner_params Provided configuration map
     * @param column_names Fields to read, also the required_fields in scanner_params
     */
    JniScanner(std::string connector_class, std::map<std::string, std::string> scanner_params,
               std::vector<std::string> column_names, int64_t self_split_weight = -1)
            : IJniConnector(std::move(connector_class), std::move(scanner_params)),
              _column_names(std::move(column_names)),
              _self_split_weight(self_split_weight) {}

    /**
     * Just use to get the table schema.
     * @param connector_class Java scanner class
     * @param scanner_params Provided configuration map
     */
    JniScanner(std::string connector_class, std::map<std::string, std::string> scanner_params)
            : IJniConnector(std::move(connector_class), std::move(scanner_params)) {
        _is_table_schema = true;
    }

    ~JniScanner() override = default;

    /**
     * Open java scanner, and get the following scanner methods by jni:
     * 1. getNextBatchMeta: read next batch and return the address of meta information
     * 2. close: close java scanner, and release jni resources
     * 3. releaseColumn: release a single column
     * 4. releaseTable: release current batch, which will also release columns and meta information
     */
    Status open(RuntimeState* state, RuntimeProfile* profile);

    /**
     * Should call before open, parse the pushed down filters. The value ranges can be stored as byte array in heap:
     * number_filters(4) | length(4) | column_name | op(4) | scale(4) | num_values(4) | value_length(4) | value | ...
     * Then, pass the byte array address in configuration map, like "push_down_predicates=${address}"
     */
    Status init(const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    /**
     * Call java side function JniScanner.getNextBatchMeta. The columns information are stored as long array:
     *                            | number of rows |
     *                            | null indicator start address of fixed length column-A |
     *                            | data column start address of the fixed length column-A  |
     *                            | ... |
     *                            | null indicator start address of variable length column-B |
     *                            | offset column start address of the variable length column-B |
     *                            | data column start address of the variable length column-B |
     *                            | ... |
     */
    Status get_next_block(Block* block, size_t* read_rows, bool* eof);

    /**
     * Call java side function JniScanner.getTableSchema.
     *
     * The schema information are stored as json format
     */
    Status get_table_schema(std::string& table_schema_str);

    /**
     * Get performance metrics from java scanner
     */
    std::map<std::string, std::string> get_statistics(JNIEnv* env) override;

    /**
     * Close scanner and release jni resources.
     */
    Status close() override;

private:
    std::vector<std::string> _column_names;
    int32_t _self_split_weight;
    bool _is_table_schema = false;

    RuntimeProfile::Counter* _open_scanner_time = nullptr;
    RuntimeProfile::Counter* _java_scan_time = nullptr;
    RuntimeProfile::Counter* _java_append_data_time = nullptr;
    RuntimeProfile::Counter* _java_create_vector_table_time = nullptr;
    RuntimeProfile::Counter* _fill_block_time = nullptr;
    std::map<std::string, RuntimeProfile::Counter*> _scanner_profile;
    RuntimeProfile::ConditionCounter* _max_time_split_weight_counter = nullptr;

    int64_t _jni_scanner_open_watcher = 0;
    int64_t _java_scan_watcher = 0;
    int64_t _fill_block_watcher = 0;

    size_t _has_read = 0;

    // Scanner JNI method IDs
    jmethodID _jni_scanner_get_next_batch;
    jmethodID _jni_scanner_get_table_schema;
    jmethodID _jni_scanner_get_append_data_time = nullptr;
    jmethodID _jni_scanner_get_create_vector_table_time = nullptr;

    int _predicates_length = 0;
    std::unique_ptr<char[]> _predicates;

    /**
     * Set the address of meta information, which is returned by org.apache.doris.common.jni.JniScanner#getNextBatchMeta
     */
    void _set_meta(long meta_addr) { _table_meta.set_meta(meta_addr); }

    Status _init_jni_scanner(JNIEnv* env, int batch_size);
    Status _fill_block(Block* block, size_t num_rows);

    void _generate_predicates(
            const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    template <PrimitiveType primitive_type>
    void _parse_value_range(const ColumnValueRange<primitive_type>& col_val_range,
                            const std::string& column_name) {
        using CppType = std::conditional_t<primitive_type == TYPE_HLL, StringRef,
                                           typename PrimitiveTypeTraits<primitive_type>::CppType>;

        if (col_val_range.is_fixed_value_range()) {
            ScanPredicate<CppType> in_predicate(column_name);
            in_predicate.op = SQLFilterOp::FILTER_IN;
            in_predicate.scale = col_val_range.scale();
            for (const auto& value : col_val_range.get_fixed_value_set()) {
                in_predicate.values.emplace_back(&value);
            }
            if (!in_predicate.values.empty()) {
                _predicates_length = in_predicate.write(_predicates, _predicates_length);
            }
            return;
        }

        const CppType high_value = col_val_range.get_range_max_value();
        const CppType low_value = col_val_range.get_range_min_value();
        const SQLFilterOp high_op = col_val_range.get_range_high_op();
        const SQLFilterOp low_op = col_val_range.get_range_low_op();

        // orc can only push down is_null. When col_value_range._contain_null = true, only indicating that
        // value can be null, not equals null, so ignore _contain_null in col_value_range
        if (col_val_range.is_high_value_maximum() && high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL &&
            col_val_range.is_low_value_mininum() && low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL) {
            return;
        }

        if (low_value < high_value) {
            if (!col_val_range.is_low_value_mininum() ||
                SQLFilterOp::FILTER_LARGER_OR_EQUAL != low_op) {
                ScanPredicate<CppType> low_predicate(column_name);
                low_predicate.scale = col_val_range.scale();
                low_predicate.op = low_op;
                low_predicate.values.emplace_back(col_val_range.get_range_min_value_ptr());
                _predicates_length = low_predicate.write(_predicates, _predicates_length);
            }
            if (!col_val_range.is_high_value_maximum() ||
                SQLFilterOp::FILTER_LESS_OR_EQUAL != high_op) {
                ScanPredicate<CppType> high_predicate(column_name);
                high_predicate.scale = col_val_range.scale();
                high_predicate.op = high_op;
                high_predicate.values.emplace_back(col_val_range.get_range_max_value_ptr());
                _predicates_length = high_predicate.write(_predicates, _predicates_length);
            }
        }
    }
};

} // namespace doris::vectorized 