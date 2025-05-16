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

package org.apache.doris.trinoconnector;

import org.apache.doris.common.jni.JniWriter;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheKey;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheValue;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.client.ClientCapabilities;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.type.InternalTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * TrinoConnectorJniWriter is used to write data to Trino connector.
 * It extends JniWriter to reuse the JNI infrastructure.
 */
public class TrinoConnectorJniWriter extends JniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniWriter.class);
    private static final String TRINO_CONNECTOR_PROPERTIES_PREFIX = "trino.";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private final String catalogNameString;
    private final String catalogCreateTime;

    private final String tableHandleString;
    private final String columnHandlesString;
    private final String columnMetadataString;
    private final Map<String, String> trinoConnectorOptionParams;

    private CatalogHandle catalogHandle;
    private Connector connector;
    private HandleResolver handleResolver;
    private Session session;
    private ObjectMapperProvider objectMapperProvider;


    private ConnectorPageSinkProvider pageSinkProvider;
    private ConnectorPageSink sink;
    private ConnectorInsertTableHandle insertTableHandle;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private ConnectorTableHandle connectorTableHandle;
    private List<ColumnHandle> columns;
    private List<TrinoColumnMetadata> columnMetadataList = Lists.newArrayList();
    private List<Type> trinoTypeList;

    private List<String> trinoConnectorAllFieldNames;

    private ConnectorMetadata metadata;

    public TrinoConnectorJniWriter(Map<String, String> params) {
        catalogNameString = params.get("catalog_name");
        tableHandleString = params.get("trino_connector_table_handle");
        columnHandlesString = params.get("trino_connector_column_handles");
        columnMetadataString = params.get("trino_connector_column_metadata");

        trinoConnectorOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_PROPERTIES_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_PROPERTIES_PREFIX.length()),
                                kv1 -> kv1.getValue()));
        catalogCreateTime = trinoConnectorOptionParams.remove("create_time");
    }

    @Override
    public void open() throws IOException {
        initConnector();
        this.pageSinkProvider = getConnectorPageSinkProvider();
        this.objectMapperProvider = generateObjectMapperProvider();
        initTrinoTableMetadata();
        parseRequiredTypes();
        connectorTransactionHandle = connector.beginTransaction(
                io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED,
                true,
                true);
        metadata = connector.getMetadata(session.toConnectorSession(catalogHandle), connectorTransactionHandle);
        insertTableHandle = metadata.beginInsert(
                session.toConnectorSession(catalogHandle),
                connectorTableHandle,
                columns,
                io.trino.spi.connector.RetryMode.NO_RETRIES);
        sink = pageSinkProvider.createPageSink(
                connectorTransactionHandle,
                session.toConnectorSession(catalogHandle),
                insertTableHandle,
                new ConnectorPageSinkId() {
                    @Override
                    public long getId() {
                        return 0;
                    }
                });
    }

    private ConnectorPageSinkProvider getConnectorPageSinkProvider() {
        ConnectorPageSinkProvider connectorPageSinkProvider = null;
        try {
            connectorPageSinkProvider = connector.getPageSinkProvider();
            Objects.requireNonNull(connectorPageSinkProvider,
                    String.format("Connector '%s' returned a null page sink provider", catalogNameString));
        } catch (UnsupportedOperationException e) {
            LOG.debug("exception when getPageSinkProvider: " + e.getMessage());
        }
        return connectorPageSinkProvider;
    }

    private void parseRequiredTypes() {
        trinoTypeList = Lists.newArrayList();
        for (int i = 0; i < fields.length; i++) {
            int index = trinoConnectorAllFieldNames.indexOf(fields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        fields[i], trinoConnectorAllFieldNames));
            }
            trinoTypeList.add(columnMetadataList.get(index).getType());
        }
    }

    @Override
    public void write(Map<String, String> params) throws IOException {
        if (sink == null) {
            throw new RuntimeException("ConnectorPageSink is not initialized");
        }
        vectorTable = VectorTable.createReadableTable(params);
        PageBuilder pageBuilder = new PageBuilder(trinoTypeList);
        for (int rowIdx = 0; rowIdx < vectorTable.getNumRows(); rowIdx++) {
            pageBuilder.declarePosition();
            for (int colIdx = 0; colIdx < fields.length; colIdx++) {
                Object value = getValueFromVectorTable(vectorTable, colIdx, rowIdx);
                appendValueToPageBuilder(pageBuilder, colIdx, value);
            }
        }
        Page page = pageBuilder.build();
        sink.appendPage(page);
    }

    @Override
    public void finish() throws Exception {
        if (sink == null) {
            LOG.warn("ConnectorPageSink is not initialized, nothing to finish");
            return;
        }
        try {
            CompletableFuture<Collection<Slice>> futureFragments = sink.finish();
            Collection<Slice> fragments = futureFragments.get();
            if (metadata != null) {
                try {
                    ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                    Optional<ConnectorOutputMetadata> outputMetadata =
                            metadata.finishInsert(
                                    connectorSession,
                                    insertTableHandle,
                                    fragments,
                                    Collections.emptyList());

                    if (outputMetadata.isPresent()) {
                        LOG.info("Successfully finished insert operation with metadata: {}",
                                outputMetadata.get());
                    } else {
                        LOG.info("Successfully finished insert operation without metadata");
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to finish insert operation", e);
                    try {
                        connector.rollback(connectorTransactionHandle);
                        LOG.info("Rolled back transaction after finishInsert failure");
                    } catch (Exception rollbackEx) {
                        LOG.warn("Failed to rollback transaction after finishInsert failure", rollbackEx);
                    }
                }
            } else {
                LOG.warn("Cannot finish insert: metadata is null");
            }
        } catch (Exception e) {
            LOG.warn("Failed to finish writing to Trino connector", e);
            if (sink != null) {
                try {
                    sink.abort();
                } catch (Exception abortEx) {
                    LOG.warn("Failed to abort sink during exception handling", abortEx);
                }
            }

            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (sink != null) {
            try {
                sink.abort();
            } catch (Exception e) {
                LOG.warn("Failed to abort ConnectorPageSink", e);
            }
        }
        if (connector != null && connectorTransactionHandle != null) {
            try {
                LOG.info("Attempting to rollback transaction to clean up temporary tables");
                connector.rollback(connectorTransactionHandle);
                LOG.info("Successfully rolled back transaction and cleaned up resources");
            } catch (Exception e) {
                LOG.warn("Failed to rollback transaction", e);
            }
        }
    }

    private Object getValueFromVectorTable(VectorTable table, int colIdx, int rowIdx) {
        if (table.getColumn(colIdx).isNullAt(rowIdx)) {
            return null;
        }

        ColumnType.Type dorisType = table.getColumnType(colIdx).getType();
        switch (dorisType) {
            case BOOLEAN:
                return table.getColumn(colIdx).getBoolean(rowIdx);
            case TINYINT:
                return table.getColumn(colIdx).getByte(rowIdx);
            case SMALLINT:
                return table.getColumn(colIdx).getShort(rowIdx);
            case INT:
                return table.getColumn(colIdx).getInt(rowIdx);
            case BIGINT:
                return table.getColumn(colIdx).getLong(rowIdx);
            case LARGEINT:
                return table.getColumn(colIdx).getBigInteger(rowIdx);
            case FLOAT:
                return table.getColumn(colIdx).getFloat(rowIdx);
            case DOUBLE:
                return table.getColumn(colIdx).getDouble(rowIdx);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return table.getColumn(colIdx).getDecimal(rowIdx);
            case DATEV2:
                return table.getColumn(colIdx).getDate(rowIdx);
            case DATETIMEV2:
                return table.getColumn(colIdx).getDateTime(rowIdx);
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                return table.getColumn(colIdx).getStringWithOffset(rowIdx);
            default:
                throw new RuntimeException("Unsupported type: " + dorisType);
        }
    }

    private void appendValueToPageBuilder(PageBuilder pageBuilder, int colIdx, Object value) {
        if (value == null) {
            pageBuilder.getBlockBuilder(colIdx).appendNull();
            return;
        }
        Type trinoType = trinoTypeList.get(colIdx);
        String typeName = trinoType.getDisplayName();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(colIdx);

        if (typeName.equals("boolean")) {
            trinoType.writeBoolean(blockBuilder, (Boolean) value);
        } else if (typeName.equals("tinyint")) {
            trinoType.writeLong(blockBuilder, ((Number) value).longValue());
        } else if (typeName.equals("smallint")) {
            trinoType.writeLong(blockBuilder, ((Number) value).longValue());
        } else if (typeName.equals("integer")) {
            trinoType.writeLong(blockBuilder, ((Number) value).longValue());
        } else if (typeName.equals("bigint")) {
            trinoType.writeLong(blockBuilder, ((Number) value).longValue());
        } else if (typeName.startsWith("decimal")) {
            trinoType.writeObject(blockBuilder, value);
        } else if (typeName.equals("real")) {
            trinoType.writeLong(blockBuilder, ((Number) value).longValue());
        } else if (typeName.equals("double")) {
            trinoType.writeDouble(blockBuilder, ((Number) value).doubleValue());
        } else if (typeName.equals("date")) {
            trinoType.writeObject(blockBuilder, value);
        } else if (typeName.startsWith("timestamp")) {
            trinoType.writeObject(blockBuilder, value);
        } else if (typeName.startsWith("varchar") || typeName.startsWith("char")) {
            io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice(value.toString());
            trinoType.writeSlice(blockBuilder, slice);
        } else if (typeName.startsWith("varbinary")) {
            if (value instanceof byte[]) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.wrappedBuffer((byte[]) value);
                trinoType.writeSlice(blockBuilder, slice);
            } else if (value instanceof String) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice((String) value);
                trinoType.writeSlice(blockBuilder, slice);
            } else {
                throw new RuntimeException("Unsupported value type for VARBINARY: " + value.getClass().getName());
            }
        } else {
            throw new RuntimeException("Unsupported Trino type: " + typeName);
        }
    }

    private void initConnector() {
        String connectorName = trinoConnectorOptionParams.remove("connector.name");

        TrinoConnectorCacheKey cacheKey = new TrinoConnectorCacheKey(catalogNameString, connectorName,
                catalogCreateTime);
        cacheKey.setProperties(this.trinoConnectorOptionParams);
        cacheKey.setTrinoConnectorPluginManager(TrinoConnectorPluginLoader.getTrinoConnectorPluginManager());
        TrinoConnectorCacheValue connectorCacheValue = TrinoConnectorCache.getConnector(cacheKey);
        this.catalogHandle = connectorCacheValue.getCatalogHandle();
        this.connector = connectorCacheValue.getConnector();
        this.handleResolver = connectorCacheValue.getHandleResolver();
        this.session = createSession(connectorCacheValue.getTrinoConnectorServicesProvider());
    }

    private ObjectMapperProvider generateObjectMapperProvider() {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.insertTableHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);

        TypeManager typeManager = new InternalTypeManager(
                TrinoConnectorPluginLoader.getTrinoConnectorPluginManager().getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(),
                typeManager);
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(
                io.trino.spi.type.Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        return objectMapperProvider;
    }

    private void initTrinoTableMetadata() {
        try {
            connectorTableHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    tableHandleString, ConnectorTableHandle.class, this.objectMapperProvider);
            columns = TrinoConnectorScannerUtils.decodeStringToList(
                    columnHandlesString, ColumnHandle.class, this.objectMapperProvider);
            columnMetadataList = TrinoConnectorScannerUtils.decodeStringToList(
                    columnMetadataString, TrinoColumnMetadata.class, this.objectMapperProvider);
            trinoTypeList = columnMetadataList.stream()
                    .map(TrinoColumnMetadata::getType)
                    .collect(Collectors.toList());
            trinoConnectorAllFieldNames = columnMetadataList.stream().map(columnMetadata -> columnMetadata.getName())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("Failed to initialize Trino table metadata", e);
            throw new RuntimeException("Failed to initialize Trino table metadata", e);
        }
    }
}
