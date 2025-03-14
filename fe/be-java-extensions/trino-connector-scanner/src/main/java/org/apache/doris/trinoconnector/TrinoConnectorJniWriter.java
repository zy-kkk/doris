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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheKey;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheValue;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.json.ObjectMapperProvider;
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
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

/**
 * TrinoConnectorJniWriter is used to write data to Trino connector.
 * It extends JniScanner to reuse the JNI infrastructure.
 */
public class TrinoConnectorJniWriter extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniWriter.class);
    private static final String TRINO_CONNECTOR_PROPERTIES_PREFIX = "trino.";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private final String catalogNameString;
    private final String catalogCreateTime;

    private final String tableHandleString;
    private final String columnHandlesString;
    private final String columnMetadataString;
    private final String transactionHandleString;
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
    private long[] appendDataTimeNs;

    private List<String> trinoConnectorAllFieldNames;

    public TrinoConnectorJniWriter(Map<String, String> params) {

        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);
        appendDataTimeNs = new long[fields.length];

        catalogNameString = params.get("catalog_name");
        tableHandleString = params.get("trino_connector_table_handle");
        columnHandlesString = params.get("trino_connector_column_handles");
        columnMetadataString = params.get("trino_connector_column_metadata");
        transactionHandleString = params.get("trino_connector_transaction_handle");

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
        // mock ObjectMapperProvider
        this.objectMapperProvider = generateObjectMapperProvider();
        initTrinoTableMetadata();
        parseRequiredTypes();
        
        try {
            // 获取表的插入句柄
            insertTableHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    tableHandleString, ConnectorInsertTableHandle.class, this.objectMapperProvider);
            
            // 创建ConnectorPageSink
            sink = pageSinkProvider.createPageSink(
                    connectorTransactionHandle,
                    session.toConnectorSession(catalogHandle),
                    insertTableHandle,
                    columns);
            
            LOG.info("Successfully created ConnectorPageSink for catalog: {}", catalogNameString);
        } catch (Exception e) {
            LOG.error("Failed to create ConnectorPageSink", e);
            throw new IOException("Failed to create ConnectorPageSink", e);
        }
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
        appendDataTimeNs = new long[fields.length];
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

    public void writeData(Map<String, String> params) throws Exception {
        if (sink == null) {
            throw new RuntimeException("ConnectorPageSink is not initialized");
        }
        
        // 创建可读的VectorTable
        VectorTable table = VectorTable.createReadableTable(params);
        int numRows = table.getNumRows();
        if (numRows == 0) {
            LOG.info("No data to write");
            return;
        }
        
        LOG.info("Writing {} rows to Trino connector", numRows);
        
        try {
            // 创建PageBuilder
            PageBuilder pageBuilder = new PageBuilder(trinoTypeList);
            
            // 将VectorTable中的数据转换为Page
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                pageBuilder.declarePosition();
                
                for (int colIdx = 0; colIdx < fields.length; colIdx++) {
                    // 获取列数据
                    Object value = getValueFromVectorTable(table, colIdx, rowIdx);
                    
                    // 将值写入PageBuilder
                    appendValueToPageBuilder(pageBuilder, colIdx, value);
                }
            }
            
            // 构建Page并写入
            Page page = pageBuilder.build();
            sink.appendPage(page);
            
            LOG.info("Successfully wrote {} rows to Trino connector", numRows);
        } catch (Exception e) {
            LOG.error("Failed to write data to Trino connector", e);
            printException(e);
            throw e;
        }
    }

    public void finishWrite() throws Exception {
        if (sink == null) {
            LOG.warn("ConnectorPageSink is not initialized, nothing to finish");
            return;
        }
        
        try {
            // 完成写入
            sink.finish();
            LOG.info("Successfully finished writing to Trino connector");
        } catch (Exception e) {
            LOG.error("Failed to finish writing to Trino connector", e);
            printException(e);
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
    }

    // 从VectorTable获取值
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

    // 将值写入PageBuilder
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
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), 
                    Float.floatToIntBits(((Number) value).floatValue()));
        } else if (typeName.equals("double")) {
            trinoType.writeDouble(pageBuilder.getBlockBuilder(colIdx), ((Number) value).doubleValue());
        } else if (typeName.equals("date")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.startsWith("timestamp")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.equals("varchar") || typeName.equals("char")) {
            trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), 
                    io.airlift.slice.Slices.utf8Slice(value.toString()));
        } else if (typeName.equals("varbinary")) {
            trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), 
                    io.airlift.slice.Slices.wrappedBuffer(value.toString().getBytes()));
        } else {
            throw new RuntimeException("Unsupported Trino type: " + typeName);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> statistics = new HashMap<>();
        statistics.put("writer_catalog", catalogNameString);
        return statistics;
    }

    @Override
    protected int getNext() throws IOException {
        return 0;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        return null;
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

        // create session
        this.session = createSession(connectorCacheValue.getTrinoConnectorServicesProvider());
    }

    private ObjectMapperProvider generateObjectMapperProvider() {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
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
            connectorTransactionHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    transactionHandleString, ConnectorTransactionHandle.class, this.objectMapperProvider);

            connectorTableHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    tableHandleString, ConnectorTableHandle.class, this.objectMapperProvider);

            columns = TrinoConnectorScannerUtils.decodeStringToList(
                    columnHandlesString, ColumnHandle.class, this.objectMapperProvider);

            columnMetadataList = TrinoConnectorScannerUtils.decodeStringToList(
                    columnMetadataString, TrinoColumnMetadata.class, this.objectMapperProvider);

            trinoTypeList = columnMetadataList.stream()
                    .map(TrinoColumnMetadata::getType)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("Failed to initialize Trino table metadata", e);
            throw new RuntimeException("Failed to initialize Trino table metadata", e);
        }
    }

    private Session createSession(TrinoConnectorServicesProvider trinoConnectorServicesProvider) {
        // 创建 Trino Session
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
}
