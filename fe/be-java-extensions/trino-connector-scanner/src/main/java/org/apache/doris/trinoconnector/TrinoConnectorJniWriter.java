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
import com.google.common.collect.Maps;
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

    private boolean pageSinkInitialized = false;

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
        try {
            initConnector();
            this.pageSinkProvider = getConnectorPageSinkProvider();

            if (pageSinkProvider == null) {
                throw new IOException("Failed to get ConnectorPageSinkProvider from connector");
            }

            this.objectMapperProvider = generateObjectMapperProvider();

            try {
                initTrinoTableMetadata();
            } catch (Exception e) {
                printException(e);
            }

            LOG.info("Successfully initialized Trino connector for catalog: {}", catalogNameString);
        } catch (Exception e) {
            LOG.error("Failed to initialize Trino connector", e);
            printException(e);
            throw new IOException("Failed to initialize Trino connector: " + e.getMessage(), e);
        }
    }

    /**
     * 初始化PageSink，延迟到实际需要写入时创建，确保与实际写入列匹配
     */
    private void initPageSink(String[] requiredFields) throws IOException {
        if (pageSinkInitialized) {
            return;
        }

        try {
            connectorTransactionHandle = connector.beginTransaction(
                    io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED,
                    true,
                    true);
            LOG.info("Created new transaction handle: {}", connectorTransactionHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            metadata = connector.getMetadata(connectorSession, connectorTransactionHandle);
            if (metadata == null) {
                throw new IOException("Failed to get connector metadata");
            }

            List<ColumnHandle> selectedColumns = selectColumnsForInsert(requiredFields);

            insertTableHandle = metadata.beginInsert(
                    connectorSession,
                    connectorTableHandle,
                    selectedColumns,
                    io.trino.spi.connector.RetryMode.NO_RETRIES);

            if (insertTableHandle == null) {
                throw new IOException("Failed to begin insert operation, received null InsertTableHandle");
            }

            LOG.info("Successfully created InsertTableHandle: {}", insertTableHandle.getClass().getName());

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

            pageSinkInitialized = true;
            LOG.info("Successfully created ConnectorPageSink for table {} with required fields: {}",
                    tableHandleString, Arrays.toString(requiredFields));
        } catch (Exception e) {
            LOG.error("Failed to create PageSink", e);
            printException(e);
            throw new IOException("Failed to create PageSink: " + e.getMessage(), e);
        }
    }

    /**
     * 根据需要写入的字段名选择对应的ColumnHandle
     */
    private List<ColumnHandle> selectColumnsForInsert(String[] requiredFields) {
        List<ColumnHandle> selectedColumns = Lists.newArrayList();

        if (requiredFields == null || requiredFields.length == 0) {
            return columns;
        }

        Map<String, ColumnHandle> nameToHandleMap = Maps.newHashMap();
        for (int i = 0; i < trinoConnectorAllFieldNames.size(); i++) {
            nameToHandleMap.put(trinoConnectorAllFieldNames.get(i), columns.get(i));
        }

        for (String fieldName : requiredFields) {
            ColumnHandle handle = nameToHandleMap.get(fieldName);
            if (handle != null) {
                selectedColumns.add(handle);
            } else {
                LOG.warn("Required field {} not found in available columns", fieldName);
            }
        }

        if (selectedColumns.isEmpty()) {
            LOG.warn("No matching columns found, using all columns");
            return columns;
        }
        return selectedColumns;
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

    @Override
    public void write(Map<String, String> params) throws IOException {
        if (pageSinkProvider == null) {
            throw new RuntimeException("ConnectorPageSink provider is not initialized");
        }

        if (!params.containsKey("meta_address")) {
            throw new RuntimeException("Missing meta_address in params");
        }

        // 先创建读表
        vectorTable = VectorTable.createReadableTable(params);
        int numRows = vectorTable.getNumRows();
        if (numRows == 0) {
            LOG.info("No data to write");
            return;
        }

        LOG.info("Writing {} rows to Trino connector", numRows);

        try {
            // 处理需要写入的字段信息
            String requiredFieldsString = params.get("required_fields");

            String[] requiredFields;
            if (requiredFieldsString != null) {
                LOG.info("Required fields from params: {}", requiredFieldsString);
                String[] originalFields = requiredFieldsString.split(",");

                // 检查是否使用了_col_N格式，若是则转换为实际列名
                boolean usingColIndexFormat = originalFields[0].startsWith("_col_");

                if (usingColIndexFormat) {
                    requiredFields = new String[originalFields.length];
                    for (int i = 0; i < originalFields.length; i++) {
                        String field = originalFields[i];
                        try {
                            // 从"_col_N"格式中提取N，注意这里N是arguments[i]
                            // 不一定是连续的或从0开始的索引
                            int colIndex = Integer.parseInt(field.substring(5));

                            // 这里应该直接使用colIndex作为索引，而不是尝试转换
                            if (colIndex >= 0 && colIndex < trinoConnectorAllFieldNames.size()) {
                                requiredFields[i] = trinoConnectorAllFieldNames.get(colIndex);
                                LOG.info("Mapped column index {} to field name {}", colIndex, requiredFields[i]);
                            } else {
                                throw new RuntimeException("Column index out of bounds: " + colIndex
                                        + ", size: " + trinoConnectorAllFieldNames.size());
                            }
                        } catch (NumberFormatException e) {
                            throw new RuntimeException("Invalid column format: " + field);
                        }
                    }
                    LOG.info("Mapped column indexes to field names: {}", Arrays.toString(requiredFields));
                } else {
                    requiredFields = originalFields;
                }

                fields = requiredFields;
            } else if (fields == null || fields.length == 0) {
                fields = trinoConnectorAllFieldNames.toArray(new String[0]);
            }

            // 延迟初始化PageSink，确保使用正确的列集合
            initPageSink(fields);

            // 解析需要的类型
            parseRequiredTypes();

            PageBuilder pageBuilder = new PageBuilder(trinoTypeList);

            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                pageBuilder.declarePosition();

                for (int colIdx = 0; colIdx < fields.length; colIdx++) {
                    Object value = getValueFromVectorTable(vectorTable, colIdx, rowIdx);
                    appendValueToPageBuilder(pageBuilder, colIdx, value);
                }
            }

            Page page = pageBuilder.build();
            sink.appendPage(page);

            LOG.info("Successfully wrote {} rows to Trino connector", numRows);
        } catch (Exception e) {
            LOG.warn("Failed to write data to Trino connector", e);
            throw new IOException("Failed to write data: " + e.getMessage(), e);
        }
    }

    private void parseRequiredTypes() {
        if (fields == null || fields.length == 0) {
            fields = trinoConnectorAllFieldNames.toArray(new String[0]);
        }

        // 重新创建类型列表，确保与fields顺序一致
        trinoTypeList = Lists.newArrayList();
        Map<String, Type> nameToTypeMap = Maps.newHashMap();

        // 创建字段名到类型的映射
        for (int i = 0; i < trinoConnectorAllFieldNames.size(); i++) {
            nameToTypeMap.put(trinoConnectorAllFieldNames.get(i), columnMetadataList.get(i).getType());
        }

        for (String field : fields) {
            Type type = nameToTypeMap.get(field);
            if (type == null) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        field, trinoConnectorAllFieldNames));
            }
            trinoTypeList.add(type);
        }

        LOG.info("Parsed required types for fields: {}", Arrays.toString(fields));
        LOG.info("Fields will be mapped to these types: {}",
                trinoTypeList.stream().map(Type::getDisplayName).collect(Collectors.toList()));
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
            printException(e);

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
        // if (sink != null) {
        //     try {
        //         sink.abort();
        //     } catch (Exception e) {
        //         LOG.warn("Failed to abort ConnectorPageSink", e);
        //     }
        // }
        // if (connector != null && connectorTransactionHandle != null) {
        //     try {
        //         LOG.info("Attempting to rollback transaction to clean up temporary tables");
        //         connector.rollback(connectorTransactionHandle);
        //         LOG.info("Successfully rolled back transaction and cleaned up resources");
        //     } catch (Exception e) {
        //         LOG.warn("Failed to rollback transaction", e);
        //     }
        // }
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

        if (typeName.equals("boolean")) {
            trinoType.writeBoolean(pageBuilder.getBlockBuilder(colIdx), (Boolean) value);
        } else if (typeName.equals("tinyint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("smallint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("integer")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("bigint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.startsWith("decimal")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.equals("real")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("double")) {
            trinoType.writeDouble(pageBuilder.getBlockBuilder(colIdx), ((Number) value).doubleValue());
        } else if (typeName.equals("date")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.startsWith("timestamp")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.startsWith("varchar") || typeName.startsWith("char")) {
            io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice(value.toString());
            trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
        } else if (typeName.startsWith("varbinary")) {
            if (value instanceof byte[]) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.wrappedBuffer((byte[]) value);
                trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
            } else if (value instanceof String) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice((String) value);
                trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
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

    private Session createSession(TrinoConnectorServicesProvider trinoConnectorServicesProvider) {
        Set<SystemSessionPropertiesProvider> systemSessionProperties =
                ImmutableSet.<SystemSessionPropertiesProvider>builder()
                        .add(new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig().setTaskConcurrency(4),
                                new MemoryManagerConfig(),
                                TrinoConnectorPluginLoader.getFeaturesConfig(),
                                new OptimizerConfig(),
                                new NodeMemoryConfig(),
                                new DynamicFilterConfig(),
                                new NodeSchedulerConfig()))
                        .build();
        SessionPropertyManager sessionPropertyManager = CatalogServiceProviderModule.createSessionPropertyManager(
                systemSessionProperties, trinoConnectorServicesProvider);

        return Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setOriginalIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneId.systemDefault().toString()))
                .setLocale(Locale.ENGLISH)
                .setClientCapabilities(Arrays.stream(ClientCapabilities.values()).map(Enum::name)
                        .collect(ImmutableSet.toImmutableSet()))
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();
    }

    private void printException(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        LOG.error("Exception: " + stringWriter);
    }

    private String mapTrinoTypeToDorisType(String trinoType) {
        if (trinoType.equals("boolean")) {
            return "BOOLEAN";
        } else if (trinoType.equals("tinyint")) {
            return "TINYINT";
        } else if (trinoType.equals("smallint")) {
            return "SMALLINT";
        } else if (trinoType.equals("integer")) {
            return "INT";
        } else if (trinoType.equals("bigint")) {
            return "BIGINT";
        } else if (trinoType.startsWith("decimal")) {
            return "DECIMALV2";
        } else if (trinoType.equals("real")) {
            return "FLOAT";
        } else if (trinoType.equals("double")) {
            return "DOUBLE";
        } else if (trinoType.equals("date")) {
            return "DATEV2";
        } else if (trinoType.startsWith("timestamp")) {
            return "DATETIMEV2";
        } else if (trinoType.startsWith("varchar") || trinoType.startsWith("char")) {
            return "VARCHAR";
        } else if (trinoType.startsWith("varbinary")) {
            return "BINARY";
        } else {
            return "VARCHAR";
        }
    }
}
