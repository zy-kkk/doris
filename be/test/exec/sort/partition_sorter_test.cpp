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

#include "exec/sort/partition_sorter.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "exec/sort/heap_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/sort/topn_sorter.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

struct PartitionSorterTest : public testing::Test {
    void SetUp() override {
        row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeInt64>()}, &pool));

        ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    }
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<PartitionSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VExprContextSPtrs ordering_expr_ctxs;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
};

TEST_F(PartitionSorterTest, PerPartitionOffsetAcrossBlocksAndReset) {
    for (int batch_size : {1, 2, 4096}) {
        _state._batch_size = batch_size;
        for (int64_t offset : {0, 1, 4, 9, 20}) {
            SCOPED_TRACE(offset);
            SortCursorCmp previous;
            sorter = PartitionSorter::create_unique(ordering_expr_ctxs, offset > 0 ? 1 : -1, offset,
                                                    &pool, is_asc_order, nulls_first, *row_desc,
                                                    &_state, nullptr, false, offset + 3,
                                                    TopNAlgorithm::ROW_NUMBER, &previous);
            sorter->init_profile(&_profile);
            for (int iteration = 0; iteration < 2; ++iteration) {
                if (iteration) sorter->reset_sorter_state(&_state);
                for (int start : {0, 3, 6}) {
                    Block block = ColumnHelper::create_block<DataTypeInt64>(
                            {start + 2, start, start + 1});
                    ASSERT_TRUE(sorter->append_block(&block).ok());
                }
                ASSERT_TRUE(sorter->prepare_for_read(false).ok());
                bool eos = false;
                std::vector<int64_t> actual;
                int calls = 0;
                while (!eos) {
                    ASSERT_LT(calls++, 20);
                    Block output;
                    ASSERT_TRUE(sorter->get_next(&_state, &output, &eos).ok());
                    for (size_t row = 0; row < output.rows(); ++row) {
                        actual.push_back(output.get_by_position(0).column->get_int(row));
                    }
                }
                std::vector<int64_t> expected;
                for (int64_t value = offset; value < std::min<int64_t>(9, offset + 3); ++value) {
                    expected.push_back(value);
                }
                EXPECT_EQ(expected, actual);
                EXPECT_EQ(expected.size(), sorter->get_output_rows());
            }
        }
    }
}

TEST_F(PartitionSorterTest, PerPartitionOffsetBoundsInterleavedBlocks) {
    constexpr int64_t rows_per_block = 1024;
    constexpr int64_t block_count = 3;
    constexpr int64_t total_rows = rows_per_block * block_count;
    constexpr int64_t top_k = 3;
    for (bool ascending : {true, false}) {
        is_asc_order = {ascending};
        for (int64_t offset : {0, 1, 1025}) {
            for (int64_t outer_limit : {-1, 1}) {
                for (int batch_size : {1, 17}) {
                    SCOPED_TRACE(::testing::Message() << ascending << ":" << offset << ":"
                                                      << outer_limit << ":" << batch_size);
                    _state._batch_size = batch_size;
                    SortCursorCmp previous;
                    sorter = PartitionSorter::create_unique(
                            ordering_expr_ctxs, outer_limit, offset, &pool, is_asc_order,
                            nulls_first, *row_desc, &_state, nullptr, false, offset + top_k,
                            TopNAlgorithm::ROW_NUMBER, &previous);
                    sorter->init_profile(&_profile);
                    for (int iteration = 0; iteration < 2; ++iteration) {
                        if (iteration != 0) {
                            sorter->reset_sorter_state(&_state);
                        }
                        for (int64_t residue = 0; residue < block_count; ++residue) {
                            std::vector<int64_t> values;
                            for (int64_t row = rows_per_block; row > 0; --row) {
                                values.push_back((row - 1) * block_count + residue);
                            }
                            Block input = ColumnHelper::create_block<DataTypeInt64>(values);
                            ASSERT_TRUE(sorter->append_block(&input).ok());
                        }
                        // An outer LIMIT can reduce each block only when the partition offset is zero.
                        const int64_t retained_rows = offset == 0 && outer_limit >= 0
                                                              ? std::min(outer_limit, top_k)
                                                              : offset + top_k;
                        EXPECT_EQ(sorter->data_size(),
                                  block_count * std::min(rows_per_block, retained_rows) *
                                          sizeof(int64_t));
                        ASSERT_TRUE(sorter->prepare_for_read(false).ok());
                        std::vector<int64_t> actual;
                        bool eos = false;
                        int calls = 0;
                        while (!eos) {
                            ASSERT_LT(calls++, 10);
                            Block output;
                            ASSERT_TRUE(sorter->get_next(&_state, &output, &eos).ok());
                            for (size_t row = 0; row < output.rows(); ++row) {
                                actual.push_back(output.get_by_position(0).column->get_int(row));
                            }
                        }
                        std::vector<int64_t> expected;
                        for (int64_t rank = offset; rank < offset + top_k; ++rank) {
                            expected.push_back(ascending ? rank : total_rows - 1 - rank);
                        }
                        EXPECT_EQ(expected, actual);
                        EXPECT_EQ(top_k, sorter->get_output_rows());
                    }
                }
            }
        }
    }
}

TEST_F(PartitionSorterTest, PartialSortPreservesRankTies) {
    for (auto algorithm : {TopNAlgorithm::RANK, TopNAlgorithm::DENSE_RANK}) {
        SortCursorCmp previous;
        sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                                nulls_first, *row_desc, &_state, nullptr, false, 2,
                                                algorithm, &previous);
        sorter->init_profile(&_profile);
        Block input = ColumnHelper::create_block<DataTypeInt64>({3, 2, 2, 2, 1, 1, 1});
        ASSERT_TRUE(sorter->append_block(&input).ok());
        EXPECT_EQ(7 * sizeof(int64_t), sorter->data_size());
        ASSERT_TRUE(sorter->prepare_for_read(false).ok());
        Block output;
        bool eos = false;
        ASSERT_TRUE(sorter->get_next(&_state, &output, &eos).ok());
        const std::vector<int64_t> expected = algorithm == TopNAlgorithm::RANK
                                                      ? std::vector<int64_t> {1, 1, 1}
                                                      : std::vector<int64_t> {1, 1, 1, 2, 2, 2};
        EXPECT_TRUE(ColumnHelper::block_equal(output,
                                              ColumnHelper::create_block<DataTypeInt64>(expected)));
        EXPECT_TRUE(eos);
    }
}

TEST_F(PartitionSorterTest, PartialSortUsesEffectiveRowNumber) {
    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, true, 2,
                                            TopNAlgorithm::DENSE_RANK, nullptr);
    sorter->init_profile(&_profile);
    Block input = ColumnHelper::create_block<DataTypeInt64>({3, 2, 2, 2, 1, 1, 1});
    ASSERT_TRUE(sorter->append_block(&input).ok());
    EXPECT_EQ(2 * sizeof(int64_t), sorter->data_size());
    ASSERT_TRUE(sorter->prepare_for_read(false).ok());
    Block output;
    bool eos = false;
    ASSERT_TRUE(sorter->get_next(&_state, &output, &eos).ok());
    EXPECT_TRUE(
            ColumnHelper::block_equal(output, ColumnHelper::create_block<DataTypeInt64>({1, 1})));
    EXPECT_TRUE(eos);
}

TEST_F(PartitionSorterTest, test_partition_sorter_read_row_num) {
    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::ROW_NUMBER, nullptr);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10})));
    }

    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({100, 111})));
    }
}

TEST_F(PartitionSorterTest, test_partition_sorter_DENSE_RANK) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::DENSE_RANK, &previous_row);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10, 100, 111})));
    }

    sorter->reset_sorter_state(&_state);
}

TEST_F(PartitionSorterTest, test_partition_sorter_RANK) {
    SortCursorCmp previous_row;

    sorter = PartitionSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                            nulls_first, *row_desc, &_state, nullptr, false, 20,
                                            TopNAlgorithm::RANK, &previous_row);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({4, 5, 6, 7});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({100, 111});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        auto st = sorter->prepare_for_read(false);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        bool eos = false;
        Block block;
        EXPECT_TRUE(sorter->get_next(&_state, &block, &eos).ok());
        std::cout << block.dump_data() << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>(
                               {1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 10, 100, 111})));
    }

    sorter->reset_sorter_state(&_state);
}

} // namespace doris
