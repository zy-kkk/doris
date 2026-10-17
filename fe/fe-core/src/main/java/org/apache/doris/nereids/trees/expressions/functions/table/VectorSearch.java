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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

import com.google.common.base.Suppliers;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Lance vector_search relation TVF. */
public class VectorSearch extends TableValuedFunction {
    private final boolean deferQueryVector;
    private final Supplier<String> explainString = Suppliers.memoize(
            () -> getName() + "(" + summarizeProperties(getTVFProperties()) + ")");

    public VectorSearch(Properties properties) {
        this(properties, false);
    }

    public VectorSearch(Properties properties, boolean deferQueryVector) {
        super(VectorSearchTableValuedFunction.NAME, properties);
        this.deferQueryVector = deferQueryVector;
    }

    @Override
    public String toString() {
        return explainString.get();
    }

    /** Also used before TVF binding by EXPLAIN ALL PLAN; never change the executable properties. */
    public static String summarizeProperties(Properties properties) {
        return properties.getMap().entrySet().stream()
                .map(kv -> "'" + kv.getKey() + "' = '" + (kv.getKey().equalsIgnoreCase("query_vectors")
                        ? summarizeMatrix(kv.getValue()) : kv.getValue()) + "'")
                .collect(Collectors.joining(", "));
    }

    private static String summarizeMatrix(String json) {
        // EXPLAIN may visit unbound, invalid input. Bound display work without changing the
        // executable properties or introducing another input-validation limit.
        if (json.length() > 64 * 1024) {
            return "<query matrix>";
        }
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.beginArray();
            int queries = 0;
            int dimension = 0;
            while (reader.hasNext()) {
                if (++queries > 128 || reader.peek() != JsonToken.BEGIN_ARRAY) {
                    return "<query matrix>";
                }
                reader.beginArray();
                int elements = 0;
                while (reader.hasNext()) {
                    if (reader.peek() != JsonToken.NUMBER) {
                        return "<query matrix>";
                    }
                    reader.nextString();
                    elements++;
                }
                reader.endArray();
                if (queries == 1) {
                    dimension = elements;
                } else if (dimension != elements) {
                    return "<query matrix>";
                }
            }
            reader.endArray();
            return reader.peek() == JsonToken.END_DOCUMENT
                    ? "<" + queries + " x " + dimension + " matrix>" : "<query matrix>";
        } catch (IOException | RuntimeException e) {
            return "<query matrix>";
        }
    }

    @Override
    public Statistics computeStats(List<Slot> slots) {
        VectorSearchTableValuedFunction function = (VectorSearchTableValuedFunction) getCatalogFunction();
        if (!function.isBatchSearch() || deferQueryVector) {
            return super.computeStats(slots);
        }
        int queries = function.getNumQueries();
        // Split/driver counts are unavailable here. Estimate one candidate window per query;
        // this is not a bound on the candidates produced by all splits.
        double rows = queries * ((double) function.getTopK() + function.getOffset());
        Map<Expression, ColumnStatistic> columnToStatistics = new HashMap<>();
        for (Slot slot : slots) {
            ColumnStatistic column = ColumnStatistic.UNKNOWN;
            if (VectorSearchTableValuedFunction.QUERY_INDEX_COLUMN.equals(slot.getName())) {
                column = new ColumnStatisticBuilder(rows).setNdv(queries).setNumNulls(0)
                        .setAvgSizeByte(4).setMinValue(0).setMaxValue(queries - 1)
                        .setMinExpr(new IntLiteral(0)).setMaxExpr(new IntLiteral(queries - 1)).build();
            }
            columnToStatistics.put(slot, column);
        }
        return new Statistics(rows, columnToStatistics);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return new VectorSearchTableValuedFunction(arguments, deferQueryVector);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build vector_search(): " + t.getMessage(), t);
        }
    }
}
