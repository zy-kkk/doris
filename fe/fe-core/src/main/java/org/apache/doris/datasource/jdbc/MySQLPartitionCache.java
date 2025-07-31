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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.client.JdbcClient;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Cache for MySQL partition information.
 * Similar to HiveMetaStoreCache in Hive connector.
 */
public class MySQLPartitionCache {
    private static final Logger LOG = LogManager.getLogger(MySQLPartitionCache.class);

    private final JdbcExternalCatalog catalog;
    private final Cache<TableIdentifier, MySQLPartitionCacheValue> partitionCache;

    private static final String PARTITION_QUERY =
            "SELECT "
                    + "    PARTITION_NAME, "
                    + "    PARTITION_DESCRIPTION, "
                    + "    PARTITION_METHOD, "
                    + "    PARTITION_EXPRESSION, "
                    + "    TABLE_ROWS, "
                    + "    DATA_LENGTH, "
                    + "    IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME "
                    + "FROM INFORMATION_SCHEMA.PARTITIONS "
                    + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "  AND PARTITION_NAME IS NOT NULL "
                    + "  AND (PARTITION_METHOD = 'RANGE' OR PARTITION_METHOD = 'RANGE COLUMNS') "
                    + "ORDER BY PARTITION_ORDINAL_POSITION";

    private static final String PARTITION_COLUMNS_QUERY =
            "SELECT "
                    + "    COLUMN_NAME, "
                    + "    DATA_TYPE, "
                    + "    IS_NULLABLE "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "  AND COLUMN_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    public MySQLPartitionCache(JdbcExternalCatalog catalog) {
        this.catalog = catalog;
        this.partitionCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    }

    public MySQLPartitionCacheValue getPartitionValues(JdbcExternalTable table) {
        TableIdentifier tableId = new TableIdentifier(table.getDbName(), table.getName());
        return partitionCache.get(tableId, key -> loadPartitionValues(table));
    }

    public void invalidateTable(String dbName, String tableName) {
        TableIdentifier tableId = new TableIdentifier(dbName, tableName);
        partitionCache.invalidate(tableId);
    }

    public void invalidateAll() {
        partitionCache.invalidateAll();
    }

    private MySQLPartitionCacheValue loadPartitionValues(JdbcExternalTable table) {
        try {
            JdbcClient jdbcClient = catalog.getJdbcClient();
            try (Connection connection = jdbcClient.getConnection()) {
                // Load partition information
                List<MySQLPartitionInfo> partitionInfos = loadPartitionInfos(connection, table);

                // Load partition columns
                List<Column> partitionColumns = loadPartitionColumns(connection, table);

                return new MySQLPartitionCacheValue(partitionInfos, partitionColumns);
            }
        } catch (Exception e) {
            LOG.warn("Failed to load partition values for table {}.{}: {}",
                    table.getDbName(), table.getName(), e.getMessage(), e);

            // Return empty cache value on error
            try {
                return new MySQLPartitionCacheValue(Lists.newArrayList(), Lists.newArrayList());
            } catch (AnalysisException ae) {
                throw new RuntimeException("Failed to create empty partition cache value", ae);
            }
        }
    }

    private List<MySQLPartitionInfo> loadPartitionInfos(Connection connection, JdbcExternalTable table)
            throws SQLException {
        List<MySQLPartitionInfo> partitionInfos = Lists.newArrayList();

        try (PreparedStatement ps = connection.prepareStatement(PARTITION_QUERY)) {
            ps.setString(1, table.getDbName());
            ps.setString(2, table.getName());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    try {
                        MySQLPartitionInfo partitionInfo = new MySQLPartitionInfo(rs);
                        partitionInfos.add(partitionInfo);
                    } catch (SQLException e) {
                        LOG.warn("Failed to parse partition info for table {}.{}: {}",
                                table.getDbName(), table.getName(), e.getMessage());
                    }
                }
            }
        }

        LOG.info("Loaded {} partitions for table {}.{}",
                partitionInfos.size(), table.getDbName(), table.getName());

        return partitionInfos;
    }

    private List<Column> loadPartitionColumns(Connection connection, JdbcExternalTable table)
            throws SQLException {
        List<Column> partitionColumns = Lists.newArrayList();

        // First, get the partition expression from the first partition
        String partitionExpression = null;
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT PARTITION_EXPRESSION FROM INFORMATION_SCHEMA.PARTITIONS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_METHOD IS NOT NULL LIMIT 1")) {
            ps.setString(1, table.getDbName());
            ps.setString(2, table.getName());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    partitionExpression = rs.getString("PARTITION_EXPRESSION");
                }
            }
        }

        if (partitionExpression == null) {
            LOG.warn("No partition expression found for table {}.{}", table.getDbName(), table.getName());
            return partitionColumns;
        }

        LOG.info("Found partition expression for table {}.{}: {}",
                table.getDbName(), table.getName(), partitionExpression);

        // Now get the column information for the partition expression
        // For RANGE COLUMNS, partition_expression is the column name directly
        // For RANGE, we need to extract it from the function
        String columnName = partitionExpression;
        if (partitionExpression.contains("(") && partitionExpression.contains(")")) {
            // Extract column name from function like YEAR(order_date) -> order_date
            columnName = partitionExpression.substring(
                    partitionExpression.indexOf('(') + 1,
                    partitionExpression.lastIndexOf(')')
            ).trim();
        }

        try (PreparedStatement ps = connection.prepareStatement(PARTITION_COLUMNS_QUERY)) {
            ps.setString(1, table.getDbName());
            ps.setString(2, table.getName());
            ps.setString(3, columnName);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    boolean nullable = "YES".equals(rs.getString("IS_NULLABLE"));

                    Type dorisType = convertMySQLTypeToDorisType(dataType);
                    Column column = new Column(colName, dorisType, nullable);
                    partitionColumns.add(column);

                    LOG.info("Found partition column: {} ({})", colName, dataType);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to load partition columns for table {}.{}, using empty list: {}",
                    table.getDbName(), table.getName(), e.getMessage());
        }

        LOG.info("Loaded {} partition columns for table {}.{}",
                partitionColumns.size(), table.getDbName(), table.getName());
        return partitionColumns;
    }

    private Type convertMySQLTypeToDorisType(String mysqlType) {
        // Simple type conversion - in production you'd want more comprehensive mapping
        String lowerType = mysqlType.toLowerCase();

        if (lowerType.contains("int")) {
            return Type.INT;
        } else if (lowerType.contains("bigint")) {
            return Type.BIGINT;
        } else if (lowerType.contains("varchar") || lowerType.contains("char")) {
            return Type.STRING;
        } else if (lowerType.contains("date")) {
            return Type.DATE;
        } else if (lowerType.contains("datetime") || lowerType.contains("timestamp")) {
            return Type.DATETIME;
        } else if (lowerType.contains("decimal")) {
            return ScalarType.createDecimalV3Type(10, 2); // Default precision
        } else {
            return Type.STRING; // Default to string for unknown types
        }
    }

    /**
     * Table identifier for cache key.
     */
    public static class TableIdentifier {
        private final String dbName;
        private final String tableName;

        public TableIdentifier(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableIdentifier that = (TableIdentifier) o;
            return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName);
        }

        @Override
        public String toString() {
            return dbName + "." + tableName;
        }
    }
}
