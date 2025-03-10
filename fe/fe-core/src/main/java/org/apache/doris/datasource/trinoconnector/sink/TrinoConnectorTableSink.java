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

package org.apache.doris.datasource.trinoconnector.sink;

import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.TrinoConnectorInsertCommandContext;
import org.apache.doris.planner.BaseExternalTableDataSink;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TTrinoConnnectorTableSink;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class TrinoConnectorTableSink extends BaseExternalTableDataSink {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorTableSink.class);
    private final TrinoConnectorExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_JNI);
        }};

    public TrinoConnectorTableSink(TrinoConnectorExternalTable targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TRINO CONNECTOR TABLE SINK\n");
        sb.append(prefix).append("  CATALOG: ").append(targetTable.getCatalog().getName()).append("\n");
        sb.append(prefix).append("  DATABASE: ").append(targetTable.getDbName()).append("\n");
        sb.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
        return sb.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TDataSink tDataSink = new TDataSink(TDataSinkType.TRINO_CONNECTOR_TABLE_SINK);
        TTrinoConnnectorTableSink tTrinoConnectorTableSink = new TTrinoConnnectorTableSink();

        // Set basic table information
        tTrinoConnectorTableSink.setCatalogName(targetTable.getCatalog().getName());
        tTrinoConnectorTableSink.setDbName(targetTable.getDbName());
        tTrinoConnectorTableSink.setTableName(targetTable.getName());

        // Set Trino connector options
        if (insertCtx.isPresent() && insertCtx.get() instanceof TrinoConnectorInsertCommandContext) {
            TrinoConnectorInsertCommandContext trinoCtx = (TrinoConnectorInsertCommandContext) insertCtx.get();
            if (trinoCtx.getTrinoConnectorOptions() != null) {
                tTrinoConnectorTableSink.setTrinoConnectorOptions(trinoCtx.getTrinoConnectorOptions());
            }
        }

        // Set table properties from the target table
        if (targetTable.getCatalog().getCatalogProperty() != null) {
            if (tTrinoConnectorTableSink.getTrinoConnectorOptions() == null) {
                tTrinoConnectorTableSink.setTrinoConnectorOptions(
                        targetTable.getCatalog().getCatalogProperty().getProperties());
            } else {
                tTrinoConnectorTableSink.getTrinoConnectorOptions()
                        .putAll(targetTable.getCatalog().getCatalogProperty().getProperties());
            }
        }

        // Set transaction flag
        tTrinoConnectorTableSink.setUseTransaction(false);

        tDataSink.setTrinoConnectorTableSink(tTrinoConnectorTableSink);
        this.tDataSink = tDataSink;
    }
}
