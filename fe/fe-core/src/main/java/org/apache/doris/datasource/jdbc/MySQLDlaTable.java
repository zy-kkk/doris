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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL DLA table implementation.
 * Similar to HiveDlaTable in Hive connector.
 */
public class MySQLDlaTable extends JdbcDlaTable {
    private static final Logger LOG = LogManager.getLogger(MySQLDlaTable.class);

    private MySQLPartitionCache partitionCache;

    public MySQLDlaTable(JdbcExternalTable table) {
        super(table);
        this.partitionCache = getMySQLPartitionCache();
    }

    private MySQLPartitionCache getMySQLPartitionCache() {
        JdbcExternalCatalog catalog = (JdbcExternalCatalog) jdbcTable.getCatalog();
        
        // Try to get existing cache or create new one
        try {
            // In a real implementation, you might want to store this in the catalog
            // For now, create a new cache instance
            return new MySQLPartitionCache(catalog);
        } catch (Exception e) {
            LOG.warn("Failed to get MySQL partition cache, creating new one: {}", e.getMessage());
            return new MySQLPartitionCache(catalog);
        }
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);
            return cacheValue.isPartitioned() ? PartitionType.LIST : PartitionType.UNPARTITIONED;
        } catch (Exception e) {
            LOG.warn("Failed to get partition type for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            return PartitionType.UNPARTITIONED;
        }
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);
            return cacheValue.getPartitionColumns().stream()
                    .map(Column::getName)
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            LOG.warn("Failed to get partition column names for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            return Collections.emptySet();
        }
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);
            List<Column> columns = cacheValue.getPartitionColumns();
            LOG.info("MySQLDlaTable.getPartitionColumns for table {}.{}, found {} partition columns: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), columns.size(), 
                    columns.stream().map(Column::getName).collect(Collectors.toList()));
            return columns;
        } catch (Exception e) {
            LOG.warn("Failed to get partition columns for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);
            if (!cacheValue.isPartitioned()) {
                return Collections.emptyMap();
            }

            // Return a copy of partition items
            Map<String, PartitionItem> items = Maps.newHashMap();
            items.putAll(cacheValue.getPartitionItems());
            return items;
        } catch (Exception e) {
            LOG.warn("Failed to get partition items for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            throw new AnalysisException("Failed to get partition items: " + e.getMessage());
        }
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);

            if (!cacheValue.isPartitioned()) {
                // For non-partitioned tables, return table snapshot
                return getTableSnapshot(snapshot);
            }

            MySQLPartitionInfo partitionInfo = cacheValue.getPartitionInfo(partitionName);
            if (partitionInfo == null) {
                throw new AnalysisException("Partition not found: " + partitionName
                        + " in table " + jdbcTable.getDbName() + "." + jdbcTable.getName());
            }

            // Use partition's modified time as snapshot
            return new MTMVTimestampSnapshot(partitionInfo.getModifiedTime());
        } catch (Exception e) {
            LOG.warn("Failed to get partition snapshot for {}:{}.{}: {}",
                    partitionName, jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            throw new AnalysisException("Failed to get partition snapshot: " + e.getMessage());
        }
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);

            if (!cacheValue.isPartitioned()) {
                // For non-partitioned tables, use current timestamp
                return new MTMVTimestampSnapshot(System.currentTimeMillis());
            }

            // For partitioned tables, use the maximum modified time across all partitions
            long maxModifiedTime = cacheValue.getPartitionInfos().stream()
                    .mapToLong(MySQLPartitionInfo::getModifiedTime)
                    .max()
                    .orElse(System.currentTimeMillis());

            return new MTMVTimestampSnapshot(maxModifiedTime);
        } catch (Exception e) {
            LOG.warn("Failed to get table snapshot for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            // Fallback to current timestamp
            return new MTMVTimestampSnapshot(System.currentTimeMillis());
        }
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        try {
            MySQLPartitionCacheValue cacheValue = partitionCache.getPartitionValues(jdbcTable);

            if (!cacheValue.isPartitioned()) {
                return System.currentTimeMillis();
            }

            return cacheValue.getPartitionInfos().stream()
                    .mapToLong(MySQLPartitionInfo::getModifiedTime)
                    .max()
                    .orElse(System.currentTimeMillis());
        } catch (Exception e) {
            LOG.warn("Failed to get newest update time for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            return System.currentTimeMillis();
        }
    }

    @Override
    public boolean needAutoRefresh() {
        return true;
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        // MySQL partition columns generally don't allow null
        return false;
    }

    @Override
    public boolean isValidRelatedTable() {
        try {
            // Check if we can successfully load partition information
            partitionCache.getPartitionValues(jdbcTable);
            return true;
        } catch (Exception e) {
            LOG.warn("Table {}.{} is not valid as related table: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
            return false;
        }
    }

    /**
     * Invalidate partition cache for this table.
     */
    public void invalidateCache() {
        try {
            partitionCache.invalidateTable(jdbcTable.getDbName(), jdbcTable.getName());
        } catch (Exception e) {
            LOG.warn("Failed to invalidate cache for table {}.{}: {}",
                    jdbcTable.getDbName(), jdbcTable.getName(), e.getMessage());
        }
    }
}
