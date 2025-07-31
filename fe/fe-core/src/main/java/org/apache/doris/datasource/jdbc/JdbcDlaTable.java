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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Abstract base class for JDBC DLA tables.
 * Similar to HMSDlaTable in Hive connector.
 */
public abstract class JdbcDlaTable {

    protected final JdbcExternalTable jdbcTable;

    public JdbcDlaTable(JdbcExternalTable table) {
        this.jdbcTable = table;
    }

    public abstract PartitionType getPartitionType(Optional<MvccSnapshot> snapshot);

    public abstract Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException;

    public abstract List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot);

    public abstract Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
            throws AnalysisException;

    public abstract MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException;

    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        // For JDBC tables, use current timestamp as table snapshot
        return new MTMVTimestampSnapshot(System.currentTimeMillis());
    }

    public long getNewestUpdateVersionOrTime() {
        // Return current timestamp for JDBC tables
        return System.currentTimeMillis();
    }

    public boolean needAutoRefresh() {
        return true;
    }

    public boolean isPartitionColumnAllowNull() {
        return false;
    }

    public boolean isValidRelatedTable() {
        return true;
    }
}
