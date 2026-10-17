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

package org.apache.doris.planner;

import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.thrift.TPartTopNPhase;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class PartitionSortNodeTest {
    private PartitionSortNode node(WindowFuncType function, PartitionTopnPhase phase) {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(1));
        SortInfo sort = new SortInfo(Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), tuple);
        return new PartitionSortNode(new PlanNodeId(1),
                new EmptySetNode(new PlanNodeId(0), Lists.newArrayList(tuple.getId())),
                function, Collections.emptyList(), sort, false, 4, phase);
    }

    @Test
    void testGroupWindowUsesDistinctWireTypeInBothPhases() {
        for (PartitionTopnPhase phase : new PartitionTopnPhase[] {
                PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN, PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN}) {
            PartitionSortNode node = node(WindowFuncType.ROW_NUMBER, phase);
            node.setGroupTopN();
            boolean global = phase == PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN;
            if (global) {
                node.setPartitionOffset(1);
            }
            TPlanNode wire = new TPlanNode();
            node.toThrift(wire);
            Assertions.assertEquals(TPlanNodeType.GROUP_TOPN_NODE, wire.getNodeType());
            Assertions.assertEquals(4, wire.getPartitionSortNode().getPartitionInnerLimit());
            Assertions.assertEquals(global, wire.getPartitionSortNode().isSetPartitionOffset());
            Assertions.assertEquals(global ? TPartTopNPhase.TWO_PHASE_GLOBAL : TPartTopNPhase.TWO_PHASE_LOCAL,
                    wire.getPartitionSortNode().getPtopnPhase());
            if (global) {
                Assertions.assertEquals(1, wire.getPartitionSortNode().getPartitionOffset());
            }
        }
    }

    @Test
    void testWindowPrefilterKeepsExistingWireType() {
        for (WindowFuncType function : new WindowFuncType[] {
                WindowFuncType.ROW_NUMBER, WindowFuncType.RANK, WindowFuncType.DENSE_RANK}) {
            PartitionSortNode node = node(function, PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN);
            TPlanNode wire = new TPlanNode();
            node.toThrift(wire);
            Assertions.assertEquals(TPlanNodeType.PARTITION_SORT_NODE, wire.getNodeType());
            Assertions.assertFalse(wire.getPartitionSortNode().isSetPartitionOffset());
            Assertions.assertThrows(IllegalArgumentException.class, () -> node.setPartitionOffset(1));
        }
    }

    @Test
    void testOffsetRequiresGlobalGroupWindow() {
        PartitionSortNode local = node(WindowFuncType.ROW_NUMBER, PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN);
        local.setGroupTopN();
        Assertions.assertThrows(IllegalArgumentException.class, () -> local.setPartitionOffset(1));
        PartitionSortNode global = node(WindowFuncType.ROW_NUMBER, PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN);
        global.setGroupTopN();
        Assertions.assertThrows(IllegalArgumentException.class, () -> global.setPartitionOffset(-1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> node(WindowFuncType.RANK, PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN).setGroupTopN());
    }
}
