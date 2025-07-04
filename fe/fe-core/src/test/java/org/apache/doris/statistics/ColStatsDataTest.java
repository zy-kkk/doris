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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ColStatsDataTest {
    @Test
    public void testConstructNotNull() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add("100");
        values.add("200");
        values.add("300");
        values.add("min");
        values.add("max");
        values.add("400");
        values.add("500");
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertEquals("id", data.statsId.id);
        Assertions.assertEquals(10000, data.statsId.catalogId);
        Assertions.assertEquals(20000, data.statsId.dbId);
        Assertions.assertEquals(30000, data.statsId.tblId);
        Assertions.assertEquals(0, data.statsId.idxId);
        Assertions.assertEquals("col", data.statsId.colId);
        Assertions.assertNull(data.statsId.partId);
        Assertions.assertEquals(100, data.count);
        Assertions.assertEquals(200, data.ndv);
        Assertions.assertEquals(300, data.nullCount);
        Assertions.assertEquals("min", data.minLit);
        Assertions.assertEquals("max", data.maxLit);
        Assertions.assertEquals(400, data.dataSizeInBytes);
        Assertions.assertEquals("500", data.updateTime);
        Assertions.assertNull(data.hotValues);
    }

    @Test
    public void testConstructNull() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertEquals("id", data.statsId.id);
        Assertions.assertEquals(10000, data.statsId.catalogId);
        Assertions.assertEquals(20000, data.statsId.dbId);
        Assertions.assertEquals(30000, data.statsId.tblId);
        Assertions.assertEquals(0, data.statsId.idxId);
        Assertions.assertEquals("col", data.statsId.colId);
        Assertions.assertNull(data.statsId.partId);
        Assertions.assertEquals(0, data.count);
        Assertions.assertEquals(0, data.ndv);
        Assertions.assertEquals(0, data.nullCount);
        Assertions.assertNull(data.minLit);
        Assertions.assertNull(data.maxLit);
        Assertions.assertEquals(0, data.dataSizeInBytes);
        Assertions.assertNull(data.updateTime);
        Assertions.assertNull(data.hotValues);
    }

    @Test
    public void testToColumnStatisticUnknown() {
        // Test column is null
        try (MockedStatic<StatisticsUtil> mocked = Mockito.mockStatic(StatisticsUtil.class)) {
            // 设置静态方法的行为
            mocked.when(() -> StatisticsUtil.findColumn(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
                    ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
                    .thenReturn(null);

            List<String> values = Lists.newArrayList();
            values.add("id");
            values.add("10000");
            values.add("20000");
            values.add("30000");
            values.add("0");
            values.add("col");
            values.add(null);
            values.add("100");
            values.add("200");
            values.add("300");
            values.add("min");
            values.add("max");
            values.add("400");
            values.add("500");
            values.add(null);
            ResultRow row = new ResultRow(values);
            ColStatsData data = new ColStatsData(row);
            ColumnStatistic columnStatistic = data.toColumnStatistic();
            Assertions.assertEquals(ColumnStatistic.UNKNOWN, columnStatistic);
        }
    }

    @Test
    public void testToColumnStatisticNormal() {
        try (MockedStatic<StatisticsUtil> mocked = Mockito.mockStatic(StatisticsUtil.class)) {
            // 设置静态方法的行为
            mocked.when(() -> StatisticsUtil.findColumn(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
                            ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
                    .thenReturn(new Column("colName", PrimitiveType.STRING));
            List<String> values = Lists.newArrayList();
            values.add("id");
            values.add("10000");
            values.add("20000");
            values.add("30000");
            values.add("0");
            values.add("col");
            values.add(null);
            values.add("100");
            values.add("200");
            values.add("300");
            values.add("null");
            values.add("null");
            values.add("400");
            values.add("500");
            values.add(null);
            ResultRow row = new ResultRow(values);
            ColStatsData data = new ColStatsData(row);
            ColumnStatistic columnStatistic = data.toColumnStatistic();
            Assertions.assertEquals(100, columnStatistic.count);
            Assertions.assertEquals(200, columnStatistic.ndv);
            Assertions.assertEquals(300, columnStatistic.numNulls);
            Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.minValue);
            Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.maxValue);
            Assertions.assertEquals(400, columnStatistic.dataSize);
            Assertions.assertEquals("500", columnStatistic.updatedTime);
        }
    }

    @Test
    public void testIsNull() {
        ColStatsData stats = new ColStatsData();
        Assertions.assertTrue(stats.isNull(null));
        Assertions.assertTrue(stats.isNull("null"));
        Assertions.assertTrue(stats.isNull("NuLl"));
        Assertions.assertFalse(stats.isNull(""));
        Assertions.assertFalse(stats.isNull(" "));
        Assertions.assertFalse(stats.isNull("123"));
    }

    @Test
    public void testIsValid() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add("100"); // count
        values.add("1100"); // ndv
        values.add("300"); // null
        values.add("min");
        values.add("max");
        values.add("400");
        values.add("500");
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // Set count = 200
        values.set(7, "200");
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertTrue(data.isValid());

        // Set ndv = 0, min/max is not null
        values.set(8, "0");
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // Set min to null, min/max is not null
        values.set(10, null);
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // Set max to null, min/max are all null
        values.set(11, null);
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertTrue(data.isValid());

        // Set min to not null, min/max is not null
        values.set(10, "min");
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // Set min and max to null, nullNum = 0
        values.set(9, "0");
        values.set(10, "nuLl");
        values.set(11, null);
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // nullNum = 19, count = 200
        values.set(9, "19");
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());

        // nullNum = 21, count = 200, so count < nullNum * 10
        values.set(9, "21");
        row = new ResultRow(values);
        data = new ColStatsData(row);
        Assertions.assertTrue(data.isValid());

        // Empty table stats is valid.
        data = new ColStatsData();
        Assertions.assertTrue(data.isValid());
    }
}
