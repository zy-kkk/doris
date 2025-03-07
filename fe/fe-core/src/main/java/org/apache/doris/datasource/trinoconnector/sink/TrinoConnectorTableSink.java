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
import org.apache.doris.planner.BaseExternalTableDataSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class TrinoConnectorTableSink extends BaseExternalTableDataSink {

    private final TrinoConnectorExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_JNI);
        }};

    public TrinoConnectorTableSink(TrinoConnectorExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("TRINO TABLE SINK:\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
    }
}
