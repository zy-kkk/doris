public class TrinoConnectorJniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniWriter.class);

    private final List<Type> trinoTypeList;
    private final CompletableFuture<Collection<Slice>> sink;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final AtomicBoolean isFinished = new AtomicBoolean(false);

    public TrinoConnectorJniWriter(List<Type> trinoTypeList, CompletableFuture<Collection<Slice>> sink) {
        this.trinoTypeList = trinoTypeList;
        this.sink = sink;
    }

    public void writeData(long metaAddress) throws Exception {
        if (!isOpen.get()) {
            isOpen.set(true);
        }

        if (isFinished.get()) {
            LOG.warn("TrinoConnectorJniWriter is already finished, cannot write more data");
            return;
        }

        LOG.debug("Writing data to Trino connector with meta address: {}", metaAddress);
        
        try {
            // 从本地内存中读取数据
            VectorTable vectorTable = VectorTable.createReadableTable(metaAddress);
            int numRows = vectorTable.getNumRows();
            
            if (numRows == 0) {
                LOG.info("No rows to write");
                return;
            }
            
            LOG.debug("Converting {} rows from VectorTable to Trino Page", numRows);
            
            // 创建 Block 数组
            Block[] blocks = new Block[trinoTypeList.size()];
            
            // 为每一列创建 Block
            for (int i = 0; i < trinoTypeList.size(); i++) {
                Type trinoType = trinoTypeList.get(i);
                BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, numRows);
                
                // 获取列数据
                VectorColumn column = vectorTable.getColumn(i);
                
                // 根据类型将数据写入 BlockBuilder
                for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                    if (column.isNullAt(rowIndex)) {
                        blockBuilder.appendNull();
                        continue;
                    }
                    
                    // 根据 Trino 类型和 Doris 类型进行转换
                    appendValueToBlockBuilder(column, rowIndex, trinoType, blockBuilder);
                }
                
                blocks[i] = blockBuilder.build();
            }
            
            // 创建 Page 并写入
            Page page = new Page(blocks);
            CompletableFuture<Collection<Slice>> future = sink.appendPage(page);
            
            // 等待写入完成
            future.get();
            
            LOG.debug("Successfully wrote {} rows to Trino connector", numRows);
        } catch (Exception e) {
            LOG.error("Failed to write data to Trino connector", e);
            printException(e);
            throw e;
        }
    }
    
    private void appendValueToBlockBuilder(VectorColumn column, int rowIndex, Type trinoType, BlockBuilder blockBuilder) {
        ColumnType.Type dorisType = column.getColumnPrimitiveType();
        
        try {
            // 根据 Trino 的 Java 类型进行转换
            Class<?> javaType = trinoType.getJavaType();
            
            if (javaType == boolean.class) {
                boolean value = column.getBoolean(rowIndex);
                trinoType.writeBoolean(blockBuilder, value);
            } else if (javaType == long.class) {
                // 处理整数类型 (TINYINT, SMALLINT, INT, BIGINT, DATE, TIMESTAMP 等)
                long value;
                switch (dorisType) {
                    case TINYINT:
                        value = column.getByte(rowIndex);
                        break;
                    case SMALLINT:
                        value = column.getShort(rowIndex);
                        break;
                    case INT:
                        value = column.getInt(rowIndex);
                        break;
                    case BIGINT:
                    case LARGEINT:
                        value = column.getLong(rowIndex);
                        break;
                    case DATEV2:
                        // 将 LocalDate 转换为天数
                        value = column.getDate(rowIndex).toEpochDay();
                        break;
                    case DATETIMEV2:
                        // 将 LocalDateTime 转换为毫秒数
                        value = column.getDateTime(rowIndex).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        break;
                    default:
                        value = column.getLong(rowIndex);
                }
                trinoType.writeLong(blockBuilder, value);
            } else if (javaType == double.class) {
                // 处理浮点类型 (FLOAT, DOUBLE)
                double value;
                if (dorisType == ColumnType.Type.FLOAT) {
                    value = column.getFloat(rowIndex);
                } else {
                    value = column.getDouble(rowIndex);
                }
                trinoType.writeDouble(blockBuilder, value);
            } else if (javaType == Slice.class) {
                // 处理字符串类型 (CHAR, VARCHAR, STRING)
                String value = column.getStringWithOffset(rowIndex);
                trinoType.writeSlice(blockBuilder, Slices.utf8Slice(value));
            } else if (javaType == Object.class) {
                // 处理复杂类型 (DECIMAL, ARRAY, MAP, STRUCT 等)
                switch (dorisType) {
                    case DECIMALV2:
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        // 将 BigDecimal 写入
                        trinoType.writeObject(blockBuilder, column.getDecimal(rowIndex));
                        break;
                    default:
                        // 对于不支持的类型，写入 null
                        blockBuilder.appendNull();
                }
            } else {
                // 对于不支持的类型，写入 null
                LOG.warn("Unsupported type conversion: Doris type {} to Trino Java type {}", 
                        dorisType, javaType.getName());
                blockBuilder.appendNull();
            }
        } catch (Exception e) {
            LOG.error("Error converting value at row {}, column type {}: {}", 
                    rowIndex, dorisType, e.getMessage());
            blockBuilder.appendNull();
        }
    }
} 