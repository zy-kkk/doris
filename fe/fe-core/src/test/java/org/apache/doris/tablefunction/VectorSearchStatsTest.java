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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableName;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.InitJoinOrder;
import org.apache.doris.nereids.rules.rewrite.RewriteCteChildren;
import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.Numbers;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGroupTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGroupTopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class VectorSearchStatsTest {
    @Test
    public void testUnboundMatrixSummaryBoundsWorkWithoutChangingProperties() {
        String[] matrices = {
                "[[1,2],[3,4]]",
                "[" + StringUtils.repeat("[0],", 20000) + "[0]]",
                StringUtils.repeat("[", 10000) + "0" + StringUtils.repeat("]", 10000),
                "[[1,2],[3]]",
                "[[{\"value\":1}]]"
        };
        for (int index = 0; index < matrices.length; index++) {
            Properties properties = new Properties(ImmutableMap.of("query_vectors", matrices[index]));
            String summary = VectorSearch.summarizeProperties(properties);
            Assertions.assertEquals("'query_vectors' = '" + (index == 0 ? "<2 x 2 matrix>" : "<query matrix>")
                    + "'", summary);
            Assertions.assertEquals(matrices[index], properties.getMap().get("query_vectors"));
        }
    }

    @Test
    public void testBatchCandidateStatsAndSingleQueryUnchanged() {
        withTable(context -> {
            LogicalTVFRelation batch = analyze("'query_vectors'='[[1,2],[3,4],[1,2],[5,6]]', "
                    + "'top_k'='3', 'offset'='1'", context);
            Statistics stats = batch.getFunction().computeStats(batch.getOutput());
            Slot query = queryIndex(batch);
            Assertions.assertEquals(16, stats.getRowCount());
            ColumnStatistic column = stats.findColumnStatistics(query);
            Assertions.assertFalse(column.isUnKnown);
            Assertions.assertEquals(4, column.ndv);
            Assertions.assertEquals(0, column.minValue);
            Assertions.assertEquals(3, column.maxValue);
            Assertions.assertEquals(0, column.numNulls);
            Assertions.assertEquals(4, column.avgSizeByte);
            // Projection may omit the generated column without losing the row estimate.
            Assertions.assertEquals(16, batch.getFunction().computeStats(
                    ImmutableList.of(batch.getOutput().get(0))).getRowCount());
            LogicalTVFRelation single = analyze("'query_vector'='[1,2]', 'top_k'='3', 'offset'='1'", context);
            Statistics singleStats = single.getFunction().computeStats(single.getOutput());
            Assertions.assertEquals(0, singleStats.getRowCount());
            Assertions.assertTrue(singleStats.columnStatistics().values().stream().allMatch(s -> s.isUnKnown));
            return null;
        });
    }

    @Test
    public void testLogicalAndGlobalWindowsAndLocalDriverEstimate() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE, false);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE, false);
        List<Slot> output = ImmutableList.of(query, distance);
        Statistics input = new Statistics(16, ImmutableMap.of(query,
                new ColumnStatisticBuilder(16).setNdv(4).setNumNulls(0).build(),
                distance, ColumnStatistic.UNKNOWN));
        Assertions.assertEquals(12, estimateWindow(output, input, 3, 1, null).getRowCount());
        Assertions.assertEquals(12, estimateWindow(output, input, 3, 1, SortPhase.GATHER_SORT).getRowCount());
        Assertions.assertEquals(16, estimateWindow(output, input, 4, 0, SortPhase.LOCAL_SORT).getRowCount());
        // Statistics cover every local driver. Four drivers may each return their own window.
        Statistics multiDriver = input.withRowCount(64);
        Assertions.assertEquals(64,
                estimateWindow(output, multiDriver, 4, 0, SortPhase.LOCAL_SORT).getRowCount());
        Assertions.assertEquals(12,
                estimateWindow(output, multiDriver, 3, 1, SortPhase.GATHER_SORT).getRowCount());
        // Uneven group sizes cannot safely subtract NDV * offset from the total input.
        Assertions.assertEquals(5, estimateWindow(output, input.withRowCount(7), 3, 2, null).getRowCount());
        Assertions.assertEquals(0, estimateWindow(output, input.withRowCount(7), 3, 7, null).getRowCount());
        Assertions.assertEquals(0, estimateWindow(output, input, 0, 0, null).getRowCount());
    }

    @Test
    public void testFilterCardinalityAndKnownEmptyInputSurviveWindow() {
        SlotReference query = new SlotReference("query_index", IntegerType.INSTANCE, false);
        SlotReference distance = new SlotReference("_distance", IntegerType.INSTANCE, false);
        List<Slot> output = ImmutableList.of(query, distance);
        Statistics input = new Statistics(16, ImmutableMap.of(query, new ColumnStatisticBuilder(16)
                .setNdv(4).setNumNulls(0).setMinValue(0).setMaxValue(3).build(),
                distance, ColumnStatistic.UNKNOWN));
        for (int value : new int[] {1, 99}) {
            Group child = group(output, input);
            LogicalFilter<GroupPlan> filter = new LogicalFilter<>(ImmutableSet.of(
                    new EqualTo(query, new IntegerLiteral(value))), new GroupPlan(child));
            GroupExpression expression = new GroupExpression(filter, ImmutableList.of(child));
            Group owner = new Group(null, expression, filter.getLogicalProperties());
            StatsCalculator.estimate(expression, null);
            Statistics filtered = owner.getStatistics();
            Assertions.assertEquals(value == 1 ? 4 : 0, filtered.getRowCount());
            for (SortPhase phase : new SortPhase[] {null, SortPhase.LOCAL_SORT, SortPhase.GATHER_SORT}) {
                Statistics result = estimateWindow(output, filtered, phase == SortPhase.LOCAL_SORT ? 4 : 3,
                        phase == SortPhase.LOCAL_SORT ? 0 : 1, phase);
                Assertions.assertEquals(value == 1 ? (phase == SortPhase.LOCAL_SORT ? 4 : 3) : 0,
                        result.getRowCount());
                Assertions.assertTrue(result.findColumnStatistics(query).ndv <= result.getRowCount());
            }
        }
        // Derivation must not normalize the caller's column map in place.
        Assertions.assertEquals(4, input.findColumnStatistics(query).ndv);
    }

    @Test
    public void testInitJoinOrderUsesGroupWindowInsteadOfCandidates() {
        withTable(context -> {
            LogicalTVFRelation relation = analyze("'query_vectors'='[[1,2],[3,4]]', "
                    + "'top_k'='1', 'offset'='20000'", context);
            LogicalGroupTopN<LogicalTVFRelation> topN = new LogicalGroupTopN<>(
                    ImmutableList.of(queryIndex(relation)),
                    ImmutableList.of(new OrderKey(relation.getOutput().get(2), true, false)), 1, 20000, relation);
            LogicalTVFRelation numbers = new LogicalTVFRelation(new RelationId(2),
                    new Numbers(new Properties(ImmutableMap.of("number", "100"))), ImmutableList.of());
            LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.CROSS_JOIN, topN, numbers, null);
            CascadesContext cascades = CascadesContext.initContext(
                    context.getStatementContext(), join, PhysicalProperties.ANY);
            context.getSessionVariable().setDisableJoinReorder(false);
            context.getSessionVariable().enableInitJoinOrder = true;
            LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) new InitJoinOrder().build()
                    .transform(join, cascades).get(0);
            Assertions.assertEquals(40002, relation.getStats().getRowCount());
            Assertions.assertEquals(2, topN.getStats().getRowCount());
            Assertions.assertEquals(100, numbers.getStats().getRowCount());
            Assertions.assertSame(numbers, rewritten.left());
            Assertions.assertSame(topN, rewritten.right());
            return null;
        });
    }

    @Test
    public void testRewriteGroupWindowRespectsCachedStatsAndDeepDerive() {
        withTable(context -> {
            LogicalTVFRelation relation = analyze("'query_vectors'='[[1,2],[3,4]]', "
                    + "'top_k'='1', 'offset'='20000'", context);
            LogicalGroupTopN<LogicalTVFRelation> topN = new LogicalGroupTopN<>(
                    ImmutableList.of(queryIndex(relation)),
                    ImmutableList.of(new OrderKey(relation.getOutput().get(2), true, false)), 1, 20000, relation);
            StatsDerive.DeriveContext deriveContext = new StatsDerive.DeriveContext();
            Statistics stats = topN.accept(new StatsDerive(false), deriveContext);
            Assertions.assertEquals(2, stats.getRowCount());
            Assertions.assertSame(stats, topN.getStats());
            Statistics cached = stats.withRowCount(100);
            topN.setStatistics(cached);
            Assertions.assertSame(cached, topN.accept(new StatsDerive(false), deriveContext));
            Assertions.assertEquals(2, topN.accept(new StatsDerive(true), deriveContext).getRowCount());
            Assertions.assertEquals(40002, relation.getStats().getRowCount());
            Assertions.assertEquals(2, relation.getStats().findColumnStatistics(queryIndex(relation)).ndv);
            return null;
        });
    }

    @Test
    public void testCteRewriteDerivesGroupWindowForProducerAndConsumer() {
        withTable(context -> {
            LogicalTVFRelation relation = analyze("'query_vectors'='[[1,2],[3,4]]', "
                    + "'top_k'='1', 'offset'='20000'", context);
            LogicalGroupTopN<LogicalTVFRelation> topN = new LogicalGroupTopN<>(
                    ImmutableList.of(queryIndex(relation)),
                    ImmutableList.of(new OrderKey(relation.getOutput().get(2), true, false)), 1, 20000, relation);
            CTEId cteId = new CTEId(1);
            LogicalCTEProducer<?> producer = new LogicalCTEProducer<>(cteId, topN);
            LogicalCTEConsumer consumer = new LogicalCTEConsumer(new RelationId(2), cteId, "batch", producer);
            LogicalCTEAnchor<?, ?> anchor = new LogicalCTEAnchor<>(cteId, producer, consumer);
            CascadesContext cascades = CascadesContext.initContext(
                    context.getStatementContext(), anchor, PhysicalProperties.ANY);
            new RewriteCteChildren(ImmutableList.of(), false).visitLogicalCTEAnchor(anchor, cascades);
            Assertions.assertEquals(40002, relation.getStats().getRowCount());
            Assertions.assertEquals(2, topN.getStats().getRowCount());
            Assertions.assertEquals(2, producer.getStats().getRowCount());
            Statistics consumerStats = consumer.accept(new StatsDerive(false), new StatsDerive.DeriveContext());
            Assertions.assertEquals(2, consumerStats.getRowCount());
            Slot consumerQuery = consumer.getOutput().stream()
                    .filter(slot -> "query_index".equals(slot.getName())).findFirst().get();
            Assertions.assertEquals(2, consumerStats.findColumnStatistics(consumerQuery).ndv);
            return null;
        });
    }

    @Test
    public void testPreparedExecuteRebindsQueryCountAndWindow() {
        withTable(context -> {
            Pair<LogicalPlan, StatementContext> parsed = new NereidsParser().parseMultiple(
                    "select * from vector_search('table'='catalog.db.items', 'column'='embedding', "
                            + "'use_index'='false', 'query_vectors'=?, 'top_k'=?, 'offset'=?)").get(0);
            StatementContext prepare = MemoTestUtils.createStatementContext(context, "");
            prepare.setPrepareStage(true);
            analyze(parsed.first, prepare);
            List<Placeholder> parameters = parsed.second.getPlaceholders();
            String[] matrices = {"[[1,2]]", "[[3,4],[1,2],[3,4]]", "[[5,6],[7,8]]"};
            int[] counts = {1, 3, 2};
            int[] limits = {3, 2, 5};
            int[] offsets = {1, 3, 0};
            for (int index = 0; index < matrices.length; index++) {
                StatementContext execute = MemoTestUtils.createStatementContext(context, "");
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                        new StringLiteral(matrices[index]));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(),
                        new IntegerLiteral(limits[index]));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(2).getPlaceholderId(),
                        new IntegerLiteral(offsets[index]));
                LogicalTVFRelation relation = analyze(parsed.first, execute);
                Statistics input = relation.getFunction().computeStats(relation.getOutput());
                Assertions.assertEquals(counts[index] * (limits[index] + offsets[index]), input.getRowCount());
                Assertions.assertEquals(counts[index], input.findColumnStatistics(queryIndex(relation)).ndv);
                // The group keys refer to the analyzed TVF output, as they do in the real planner.
                LogicalGroupTopN<Plan> topN = new LogicalGroupTopN<>(ImmutableList.of(queryIndex(relation)),
                        ImmutableList.of(new OrderKey(relation.getOutput().get(2), true, false)),
                        limits[index], offsets[index], relation);
                Group child = group(relation.getOutput(), input);
                GroupExpression expression = new GroupExpression(topN, ImmutableList.of(child));
                Group owner = new Group(null, expression, topN.getLogicalProperties());
                StatsCalculator.estimate(expression, null);
                Assertions.assertEquals(counts[index] * limits[index], owner.getStatistics().getRowCount());
            }
            return null;
        });
    }

    @Test
    public void testDirectBatchWhereFiltersCandidatesBeforeWindow() {
        withTable(context -> {
            for (String alias : new String[] {"", " AS hits"}) {
                String qualifier = alias.isEmpty() ? "" : "hits.";
                CascadesContext cascades = analyzeSql("SELECT * FROM " + batchSearch() + alias
                        + " WHERE " + qualifier + "id > 0 AND " + qualifier + "_distance > 0", context);
                assertCandidateFilter(cascades.getRewritePlan(), true);
                Rewriter.getWholeTreeRewriter(cascades).execute();
                assertCandidateFilter(cascades.getRewritePlan(), true);
            }
            return null;
        });
    }

    @Test
    public void testDerivedTableWhereFiltersCompletedWindow() {
        withTable(context -> {
            for (String projection : new String[] {"*", "id, _distance, query_index"}) {
                CascadesContext cascades = analyzeSql("SELECT * FROM (SELECT " + projection
                        + " FROM " + batchSearch() + ") hits WHERE hits.id > 0", context);
                assertCandidateFilter(cascades.getRewritePlan(), false);
                Rewriter.getWholeTreeRewriter(cascades).execute();
                assertCandidateFilter(cascades.getRewritePlan(), false);
            }
            // The inner WHERE remains a candidate filter; the outer WHERE must stay above it.
            CascadesContext cascades = analyzeSql("SELECT * FROM (SELECT * FROM " + batchSearch()
                    + " WHERE id > 0) hits WHERE hits._distance > 0", context);
            Rewriter.getWholeTreeRewriter(cascades).execute();
            LogicalGroupTopN<?> window = batchWindow(cascades.getRewritePlan());
            LogicalFilter<?> candidateFilter = (LogicalFilter<?>) window.child();
            Assertions.assertTrue(candidateFilter.getInputSlots().stream()
                    .allMatch(slot -> "id".equals(slot.getName())));
            Assertions.assertTrue(cascades.getRewritePlan().anyMatch(plan -> plan instanceof LogicalFilter
                    && plan.anyMatch(LogicalGroupTopN.class::isInstance)));
            return null;
        });
    }

    @Test
    public void testCteConsumerWhereFiltersCompletedWindow() {
        withTable(context -> {
            for (boolean materialize : new boolean[] {false, true}) {
                context.getSessionVariable().enableCTEMaterialize = materialize;
                for (String consumers : new String[] {"SELECT * FROM hits WHERE id > 0",
                        "SELECT * FROM hits WHERE id > 0 UNION ALL SELECT * FROM hits WHERE id < 100"}) {
                    CascadesContext cascades = analyzeSql("WITH hits AS (SELECT * FROM " + batchSearch()
                            + ") " + consumers, context);
                    assertCandidateFilter(cascades.getRewritePlan(), false);
                    Rewriter.getWholeTreeRewriter(cascades).execute();
                    List<LogicalGroupTopN<?>> windows = cascades.getRewritePlan().collectToList(
                            LogicalGroupTopN.class::isInstance);
                    Assertions.assertFalse(windows.isEmpty());
                    for (LogicalGroupTopN<?> window : windows) {
                        Assertions.assertFalse(window.child().anyMatch(LogicalFilter.class::isInstance),
                                cascades.getRewritePlan().treeString());
                    }
                }
            }
            return null;
        });
    }

    @Test
    public void testJoinPredicatesFilterCompletedWindow() {
        withTable(context -> {
            for (String join : new String[] {"LEFT JOIN", "INNER JOIN"}) {
                for (String predicate : new String[] {" ON hits.id > 0", " ON n.number = hits.id WHERE hits.id > 0"}) {
                    CascadesContext cascades = analyzeSql("SELECT hits.id FROM numbers('number'='1') n "
                            + join + " " + batchSearch() + " hits" + predicate, context);
                    assertCandidateFilter(cascades.getRewritePlan(), false);
                    Rewriter.getWholeTreeRewriter(cascades).execute();
                    assertCandidateFilter(cascades.getRewritePlan(), false);
                }
            }
            return null;
        });
    }

    @Test
    public void testCorrelatedBatchWhereRequiresCompletedWindowBoundary() {
        withTable(context -> {
            for (String subquery : new String[] {
                    "EXISTS (SELECT 1 FROM " + batchSearch() + " hits WHERE hits.id=n.number)",
                    "n.number IN (SELECT id FROM " + batchSearch() + " hits WHERE hits.id=n.number)",
                    "n.number=(SELECT max(id) FROM " + batchSearch()
                            + " hits WHERE hits.id=n.number AND hits._distance > 0)"}) {
                org.apache.doris.nereids.exceptions.AnalysisException error = Assertions.assertThrows(
                        org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyzeSql("SELECT n.number FROM numbers('number'='2') n WHERE "
                                + subquery, context));
                Assertions.assertTrue(error.getMessage().contains(
                        "Correlated predicates in a direct batch vector_search WHERE are not supported"));
            }
            // A derived table explicitly puts the correlated predicate after the batch window.
            CascadesContext cascades = analyzeSql("SELECT n.number FROM numbers('number'='2') n WHERE EXISTS "
                    + "(SELECT 1 FROM (SELECT * FROM " + batchSearch()
                    + ") hits WHERE hits.id=n.number)", context);
            Rewriter.getWholeTreeRewriter(cascades).execute();
            assertCandidateFilter(cascades.getRewritePlan(), false);
            return null;
        });
    }

    @Test
    public void testBatchCandidateWhereAllowsSubqueriesUsingTvfOutput() {
        withTable(context -> {
            for (String predicate : new String[] {
                    "EXISTS (SELECT 1 FROM numbers('number'='2') n WHERE n.number=hits.id)",
                    "hits.id IN (SELECT number FROM numbers('number'='2'))",
                    "hits.id=(SELECT max(number) FROM numbers('number'='2') n WHERE n.number=hits.id)"}) {
                CascadesContext cascades = analyzeSql("SELECT * FROM " + batchSearch()
                        + " hits WHERE " + predicate, context);
                Rewriter.getWholeTreeRewriter(cascades).execute();
                LogicalGroupTopN<?> window = batchWindow(cascades.getRewritePlan());
                Assertions.assertTrue(window.child().anyMatch(LogicalJoin.class::isInstance),
                        cascades.getRewritePlan().treeString());
            }
            return null;
        });
    }

    private String batchSearch() {
        return "vector_search('table'='catalog.db.items', 'column'='embedding', 'use_index'='false', "
                + "'query_vectors'='[[1,2],[3,4]]', 'top_k'='3', 'offset'='1')";
    }

    private CascadesContext analyzeSql(String sql, ConnectContext context) {
        StatementContext statement = MemoTestUtils.createStatementContext(context, sql);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        CascadesContext cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        return cascades;
    }

    private LogicalGroupTopN<?> batchWindow(Plan plan) {
        List<LogicalGroupTopN<?>> windows = plan.collectToList(LogicalGroupTopN.class::isInstance);
        Assertions.assertEquals(1, windows.size(), plan.treeString());
        LogicalGroupTopN<?> window = windows.get(0);
        Assertions.assertEquals(3, window.getLimit());
        Assertions.assertEquals(1, window.getOffset());
        return window;
    }

    private void assertCandidateFilter(Plan plan, boolean expected) {
        LogicalGroupTopN<?> window = batchWindow(plan);
        List<LogicalFilter<?>> filters = window.child().collectToList(LogicalFilter.class::isInstance);
        Assertions.assertEquals(expected ? 1 : 0, filters.size(), plan.treeString());
        if (expected) {
            LogicalFilter<?> filter = filters.get(0);
            Assertions.assertInstanceOf(LogicalTVFRelation.class, filter.child());
            Assertions.assertEquals(2, filter.getConjuncts().size());
            Assertions.assertTrue(filter.child().getOutputSet().containsAll(filter.getInputSlots()));
        }
    }

    private Statistics estimateWindow(List<Slot> output, Statistics input, long limit, long offset, SortPhase phase) {
        Group child = group(output, input);
        Plan plan = phase == null
                ? new LogicalGroupTopN<>(ImmutableList.of(output.get(0)),
                        ImmutableList.of(new OrderKey(output.get(1), true, false)), limit, offset, new GroupPlan(child))
                : new PhysicalGroupTopN<>(ImmutableList.of(output.get(0)),
                        ImmutableList.of(new OrderKey(output.get(1), true, false)), limit, offset, phase,
                        child.getLogicalProperties(), new GroupPlan(child));
        GroupExpression expression = new GroupExpression(plan, ImmutableList.of(child));
        Group owner = new Group(null, expression, plan.getLogicalProperties());
        StatsCalculator.estimate(expression, null);
        return owner.getStatistics();
    }

    private Group group(List<Slot> output, Statistics stats) {
        LogicalEmptyRelation relation = new LogicalEmptyRelation(new RelationId(1), output);
        Group group = new Group(null, new GroupExpression(relation), relation.getLogicalProperties());
        group.setStatistics(new Statistics(stats).withExpressionToColumnStats(new HashMap<>(stats.columnStatistics())));
        return group;
    }

    private Slot queryIndex(LogicalTVFRelation relation) {
        return relation.getOutput().stream().filter(slot -> "query_index".equals(slot.getName())).findFirst().get();
    }

    private LogicalTVFRelation analyze(String properties, ConnectContext context) {
        LogicalPlan plan = new NereidsParser().parseMultiple("select * from vector_search("
                + "'table'='catalog.db.items', 'column'='embedding', 'use_index'='false', " + properties + ")")
                .get(0).first;
        return analyze(plan, MemoTestUtils.createStatementContext(context, ""));
    }

    private LogicalTVFRelation analyze(LogicalPlan plan, StatementContext statement) {
        CascadesContext cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        return cascades.getRewritePlan().<LogicalTVFRelation>collectToList(LogicalTVFRelation.class::isInstance).get(0);
    }

    private void withTable(Function<ConnectContext, Void> test) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        Field vector = new Field("embedding", FieldType.nullable(new ArrowType.FixedSizeList(2)),
                Collections.singletonList(Field.nullable("item",
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        Schema schema = new Schema(ImmutableList.of(Field.nullable("id", new ArrowType.Int(64, true)), vector));
        LanceTableMetadata metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 42, schema,
                Collections.emptyList(), ImmutableMap.of("id", 0, "embedding", 1), Collections.emptyList());
        Mockito.when(table.loadMetadataForSearch(Mockito.any(LanceRefSelector.class))).thenReturn(metadata);
        Mockito.when(table.loadBasicMetadata(Mockito.any(LanceRefSelector.class))).thenReturn(metadata);
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            test.apply(context);
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
