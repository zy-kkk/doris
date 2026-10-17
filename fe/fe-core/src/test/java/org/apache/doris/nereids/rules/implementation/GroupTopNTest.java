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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterPushDownVisitor;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor;
import org.apache.doris.nereids.processor.post.materialize.MaterializeProbeVisitor;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughVectorSearchTopN;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGroupTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGroupTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GroupTopNTest {
    @Test
    public void testPreservesChildTraitsWithoutMakingEachGroupGloballyUnique() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE);
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference constant = new SlotReference("constant", IntegerType.INSTANCE);
        DataTrait.Builder builder = new DataTrait.Builder();
        builder.addUniqueSlot(id);
        builder.addUniformSlot(constant);
        DataTrait trait = builder.build();
        LogicalProperties properties = new LogicalProperties(
                () -> ImmutableList.of(query, distance, id, constant), () -> trait);
        LogicalEmptyRelation input = new LogicalEmptyRelation(new RelationId(1),
                ImmutableList.of(query, distance, id, constant), Optional.empty(), Optional.of(properties));
        LogicalGroupTopN<Plan> logical = new LogicalGroupTopN<>(ImmutableList.of(query),
                ImmutableList.of(new OrderKey(distance, true, false)), 1, 0, input);
        PhysicalGroupTopN<Plan> physical = new PhysicalGroupTopN<>(logical.getGroupKeys(), logical.getOrderKeys(),
                1, 0, SortPhase.GATHER_SORT, null, input);
        for (Plan plan : ImmutableList.of(logical, physical)) {
            DataTrait result = plan.getLogicalProperties().getTrait();
            Assertions.assertTrue(result.isUnique(id));
            Assertions.assertTrue(result.isUniform(constant));
            Assertions.assertFalse(result.isUnique(distance));
            Assertions.assertFalse(result.isUniform(distance));
        }
    }

    @Test
    public void testRewriteDoesNotPushFiltersThroughBatchWindow() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE);
        VectorSearch function = Mockito.mock(VectorSearch.class);
        VectorSearchTableValuedFunction catalogFunction = Mockito.mock(VectorSearchTableValuedFunction.class);
        Mockito.when(function.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        Mockito.when(function.getCatalogFunction()).thenReturn(catalogFunction);
        LogicalProperties properties = new LogicalProperties(() -> ImmutableList.of(query, distance),
                () -> DataTrait.EMPTY_TRAIT);
        LogicalTVFRelation relation = new LogicalTVFRelation(new RelationId(1), function, ImmutableList.of(),
                Optional.empty(), Optional.of(properties));
        LogicalGroupTopN<LogicalTVFRelation> topN = new LogicalGroupTopN<>(ImmutableList.of(query),
                ImmutableList.of(new OrderKey(distance, true, false)), 2, 1, relation);
        LogicalFilter<LogicalGroupTopN<LogicalTVFRelation>> filter = new LogicalFilter<>(
                ImmutableSet.of(new GreaterThan(distance, new IntegerLiteral(0))), topN);
        Mockito.when(catalogFunction.isBatchSearch()).thenReturn(true);
        Rule rule = new PushDownFilterThroughVectorSearchTopN().build();
        Assertions.assertFalse(rule.getPattern().matchPlanTree(filter));
    }

    @Test
    public void testAdjustNullablePreservesUnchangedNodeAndUpdatesBothKeys() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE, false);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE, false);
        LogicalEmptyRelation input = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(query, distance));
        LogicalGroupTopN<Plan> topN = new LogicalGroupTopN<>(ImmutableList.of(query),
                ImmutableList.of(new OrderKey(distance, false, true)), 2, 1, input);
        // Keep the leaf fixed to isolate the GroupTopN visitor from child rewrites.
        AdjustNullable adjust = new AdjustNullable(true) {
            @Override
            public Plan visitLogicalEmptyRelation(LogicalEmptyRelation relation, Map<ExprId, Slot> replaceMap) {
                return relation;
            }
        };
        Assertions.assertSame(topN, topN.accept(adjust, new HashMap<>()));
        Map<ExprId, Slot> replacements = new HashMap<>();
        replacements.put(query.getExprId(), query.withNullable(true));
        replacements.put(distance.getExprId(), distance.withNullable(true));
        LogicalGroupTopN<?> rewritten = (LogicalGroupTopN<?>) topN.accept(adjust, replacements);
        Assertions.assertTrue(rewritten.getGroupKeys().get(0).nullable());
        Assertions.assertTrue(rewritten.getOrderKeys().get(0).getExpr().nullable());
        Assertions.assertFalse(rewritten.getOrderKeys().get(0).isAsc());
        Assertions.assertTrue(rewritten.getOrderKeys().get(0).isNullFirst());
        Assertions.assertEquals(2, rewritten.getLimit());
        Assertions.assertEquals(1, rewritten.getOffset());
        Assertions.assertSame(input, rewritten.child());
    }

    @Test
    public void testDeepCopyRemapsGroupAndOrderSlotsToCopiedChild() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE);
        LogicalEmptyRelation input = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(query, distance));
        LogicalGroupTopN<Plan> topN = new LogicalGroupTopN<>(ImmutableList.of(query),
                ImmutableList.of(new OrderKey(distance, false, true)), 3, 2, input);
        LogicalGroupTopN<?> copied = (LogicalGroupTopN<?>) topN.accept(
                LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
        Assertions.assertNotEquals(query.getExprId(), copied.child().getOutput().get(0).getExprId());
        Assertions.assertNotEquals(distance.getExprId(), copied.child().getOutput().get(1).getExprId());
        Assertions.assertEquals(copied.child().getOutput().get(0), copied.getGroupKeys().get(0));
        Assertions.assertEquals(copied.child().getOutput().get(1), copied.getOrderKeys().get(0).getExpr());
        Assertions.assertFalse(copied.getOrderKeys().get(0).isAsc());
        Assertions.assertTrue(copied.getOrderKeys().get(0).isNullFirst());
        Assertions.assertEquals(3, copied.getLimit());
        Assertions.assertEquals(2, copied.getOffset());
    }

    @Test
    public void testLazyProbeKeepsGroupAndOrderKeysMaterialized() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE);
        SlotReference payload = new SlotReference("payload", IntegerType.INSTANCE)
                .withColumn(new Column("payload", Type.INT));
        VectorSearch function = Mockito.mock(VectorSearch.class);
        Mockito.when(function.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        List<Slot> output = ImmutableList.of(query, distance, payload);
        LogicalProperties properties = new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT);
        PhysicalTVFRelation relation = new PhysicalTVFRelation(new RelationId(1), function,
                ImmutableList.of(), properties);
        PhysicalGroupTopN<Plan> local = new PhysicalGroupTopN<>(ImmutableList.of(query),
                ImmutableList.of(new OrderKey(distance, true, false)), 3, 0,
                SortPhase.LOCAL_SORT, properties, relation);
        PhysicalGroupTopN<Plan> global = new PhysicalGroupTopN<>(local.getGroupKeys(), local.getOrderKeys(),
                2, 1, SortPhase.GATHER_SORT, properties, local);
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        Assertions.assertFalse(global.accept(visitor, new MaterializeProbeVisitor.ProbeContext(query)).isPresent());
        Assertions.assertFalse(global.accept(visitor, new MaterializeProbeVisitor.ProbeContext(distance)).isPresent());
        Assertions.assertTrue(global.accept(visitor, new MaterializeProbeVisitor.ProbeContext(payload)).isPresent());
    }

    @Test
    public void testTwoPhaseWindowRequiresGatherAndBlocksFilters() {
        ConnectContext previous = ConnectContext.get();
        try {
            SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE);
            SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE);
            LogicalProperties properties = new LogicalProperties(() -> ImmutableList.of(query, distance),
                    () -> DataTrait.EMPTY_TRAIT);
            LogicalEmptyRelation input = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(query, distance),
                    Optional.empty(), Optional.of(properties));
            LogicalGroupTopN<Plan> logical = new LogicalGroupTopN<>(ImmutableList.of(query),
                    ImmutableList.of(new OrderKey(distance, true, false)), 1, 2, input);
            // K=1 is per group: distance is neither uniform nor globally unique.
            Assertions.assertFalse(logical.getLogicalProperties().getTrait().isUniform(distance));
            Assertions.assertFalse(logical.getLogicalProperties().getTrait().isUnique(distance));
            CascadesContext context = MemoTestUtils.createCascadesContext(logical);
            PhysicalGroupTopN<?> global = (PhysicalGroupTopN<?>) new LogicalGroupTopNToPhysicalGroupTopN()
                    .build().transform(logical, context).get(0);
            PhysicalGroupTopN<?> local = (PhysicalGroupTopN<?>) global.child();
            Assertions.assertTrue(global.isGlobal());
            Assertions.assertFalse(local.isGlobal());
            Assertions.assertEquals(1, global.getLimit());
            Assertions.assertEquals(2, global.getOffset());
            Assertions.assertEquals(3, local.getLimit());
            Assertions.assertEquals(0, local.getOffset());
            for (PhysicalGroupTopN<?> phase : ImmutableList.of(local, global)) {
                GroupExpression expression = new GroupExpression(phase);
                new Group(null, expression, phase.getLogicalProperties());
                Assertions.assertEquals(ImmutableList.of(ImmutableList.of(
                        phase.isGlobal() ? PhysicalProperties.GATHER : PhysicalProperties.ANY)),
                        new RequestPropertyDeriver(ConnectContext.get(), PhysicalProperties.ANY)
                                .getRequestChildrenPropertyList(expression));
                Assertions.assertFalse(new RuntimeFilterPushDownVisitor().visitPhysicalGroupTopN(phase, null));
                Assertions.assertFalse(new TopnFilterPushDownVisitor(null).visitPhysicalGroupTopN(phase, null));
            }
            Assertions.assertEquals(global.getGroupKeys(),
                    global.withChildren(ImmutableList.<Plan>of(global.child())).getGroupKeys());
            PhysicalProperties orderedGather = new PhysicalProperties(
                    PhysicalProperties.GATHER.getDistributionSpec(), new OrderSpec(global.getOrderKeys()));
            Assertions.assertEquals(PhysicalProperties.GATHER,
                    new ChildOutputPropertyDeriver(ImmutableList.of(orderedGather))
                            .visitPhysicalGroupTopN(global, null));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
