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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * MySQL partition information.
 */
public class MySQLPartitionInfo {
    private final String partitionName;
    private final String partitionDescription;
    private final String partitionMethod;
    private final String partitionExpression;
    private final long modifiedTime;
    private final long tableRows;
    private final long dataLength;

    public MySQLPartitionInfo(ResultSet rs) throws SQLException {
        this.partitionName = rs.getString("PARTITION_NAME");
        this.partitionDescription = rs.getString("PARTITION_DESCRIPTION");
        this.partitionMethod = rs.getString("PARTITION_METHOD");
        this.partitionExpression = rs.getString("PARTITION_EXPRESSION");
        this.tableRows = rs.getLong("TABLE_ROWS");
        this.dataLength = rs.getLong("DATA_LENGTH");

        // Get modified time - initialize with default value first
        long tempModifiedTime = System.currentTimeMillis();
        try {
            java.sql.Timestamp timestamp = rs.getTimestamp("MODIFIED_TIME");
            if (timestamp != null) {
                tempModifiedTime = timestamp.getTime();
            }
        } catch (SQLException e) {
            // Use default value
        }
        this.modifiedTime = tempModifiedTime;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getPartitionDescription() {
        return partitionDescription;
    }

    public String getPartitionMethod() {
        return partitionMethod;
    }

    public String getPartitionExpression() {
        return partitionExpression;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public long getTableRows() {
        return tableRows;
    }

    public long getDataLength() {
        return dataLength;
    }

    /**
     * Convert MySQL partition info to Doris PartitionItem.
     */
    public PartitionItem toPartitionItem() throws AnalysisException {
        if ("RANGE".equals(partitionMethod) || "RANGE COLUMNS".equals(partitionMethod)) {
            return parseRangePartitionItem();
        }

        // For non-range partitions, treat as list partition
        return parseListPartitionItem();
    }

    private PartitionItem parseRangePartitionItem() throws AnalysisException {
        if (StringUtils.isEmpty(partitionDescription)) {
            throw new AnalysisException("Empty partition description for range partition: " + partitionName);
        }

        // Parse MySQL partition description
        // Example: "2024-01-01" -> Range partition item
        try {
            List<PartitionValue> values = parsePartitionValues(partitionDescription);
            PartitionKey upperBound = PartitionKey.createPartitionKey(values,
                    Collections.emptyList()); // Column types will be inferred

            // Create range partition item with upper bound
            Range<PartitionKey> range = Range.lessThan(upperBound);
            return new RangePartitionItem(range);
        } catch (Exception e) {
            throw new AnalysisException("Failed to parse range partition description: "
                    + partitionDescription + ", error: " + e.getMessage());
        }
    }

    private PartitionItem parseListPartitionItem() throws AnalysisException {
        if (StringUtils.isEmpty(partitionDescription)) {
            // For non-partitioned or invalid partitions, create empty list partition
            return new ListPartitionItem(Collections.emptyList());
        }

        try {
            List<PartitionValue> values = parsePartitionValues(partitionDescription);
            List<PartitionKey> partitionKeys = Lists.newArrayList();

            for (PartitionValue value : values) {
                PartitionKey key = PartitionKey.createPartitionKey(Lists.newArrayList(value),
                        Collections.emptyList());
                partitionKeys.add(key);
            }

            return new ListPartitionItem(partitionKeys);
        } catch (Exception e) {
            throw new AnalysisException("Failed to parse list partition description: "
                    + partitionDescription + ", error: " + e.getMessage());
        }
    }

    private List<PartitionValue> parsePartitionValues(String description) throws AnalysisException {
        List<PartitionValue> values = Lists.newArrayList();

        if (StringUtils.isEmpty(description)) {
            return values;
        }

        // Simple parsing logic for MySQL partition description
        // This is a simplified implementation - real implementation would need more robust parsing
        String[] parts = description.split(",");
        for (String part : parts) {
            String trimmed = part.trim().replaceAll("'", "");
            // Create PartitionValue from string
            values.add(new PartitionValue(trimmed));
        }

        return values;
    }

    @Override
    public String toString() {
        return "MySQLPartitionInfo{"
                + "partitionName='" + partitionName + '\''
                + ", partitionDescription='" + partitionDescription + '\''
                + ", partitionMethod='" + partitionMethod + '\''
                + ", modifiedTime=" + modifiedTime
                + '}';
    }
}
