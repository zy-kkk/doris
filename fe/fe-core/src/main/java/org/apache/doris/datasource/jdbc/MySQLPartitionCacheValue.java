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
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Cache value for MySQL partition information.
 * Similar to HivePartitionValues in Hive connector.
 */
public class MySQLPartitionCacheValue {
    private final List<MySQLPartitionInfo> partitionInfos;
    private final List<Column> partitionColumns;
    private final Map<String, PartitionItem> partitionItems;
    private final boolean isPartitioned;

    public MySQLPartitionCacheValue(List<MySQLPartitionInfo> partitionInfos,
            List<Column> partitionColumns) throws AnalysisException {
        this.partitionInfos = partitionInfos;
        this.partitionColumns = partitionColumns;
        this.isPartitioned = !partitionInfos.isEmpty();
        this.partitionItems = buildPartitionItems();
    }

    private Map<String, PartitionItem> buildPartitionItems() throws AnalysisException {
        Map<String, PartitionItem> items = Maps.newHashMap();

        for (MySQLPartitionInfo partitionInfo : partitionInfos) {
            try {
                PartitionItem item = partitionInfo.toPartitionItem();
                items.put(partitionInfo.getPartitionName(), item);
            } catch (Exception e) {
                // Log warning but continue with other partitions
                // In production, you might want to use proper logging
                System.err.println("Failed to convert partition " + partitionInfo.getPartitionName()
                        + " to PartitionItem: " + e.getMessage());
            }
        }

        return items;
    }

    public List<MySQLPartitionInfo> getPartitionInfos() {
        return partitionInfos;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<String, PartitionItem> getPartitionItems() {
        return partitionItems;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public List<String> getPartitionNames() {
        List<String> names = Lists.newArrayList();
        for (MySQLPartitionInfo info : partitionInfos) {
            names.add(info.getPartitionName());
        }
        return names;
    }

    public MySQLPartitionInfo getPartitionInfo(String partitionName) {
        for (MySQLPartitionInfo info : partitionInfos) {
            if (info.getPartitionName().equals(partitionName)) {
                return info;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "MySQLPartitionCacheValue{"
                + "partitionCount=" + partitionInfos.size()
                + ", isPartitioned=" + isPartitioned
                + ", partitionColumns=" + partitionColumns.size()
                + '}';
    }
}
