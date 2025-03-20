# Trino Connector Table Sink

## 1. 背景
Apache Doris 目前提供了 JDBC Catalog 功能，允许用户通过 JDBC 连接访问外部数据源。然而，JDBC Catalog 存在一个明显的局限性：Doris 不提供驱动 JAR 包，用户需要自行管理和配置这些驱动，这导致某些功能无法顺利实现，同时增加了用户的使用难度。

面对这一问题，Trino Connector Catalog 提供了一种替代方案。Trino Connector 的优势在于它是自带驱动的，无需用户额外提供驱动 JAR 包，大大简化了配置流程并提高了兼容性。然而，目前 Trino Connector 仅支持数据读取(Scan)操作，不支持数据写入(Sink)功能，这限制了其在数据写入场景下的应用。

为满足用户对外部数据源读写一体化的需求，我们需要为 Trino Connector 增加 Sink 功能，使其能够支持将数据写入到外部数据源中。这将完善 Trino Connector 的功能体系，使其成为一个完整的读写解决方案，为用户提供更加便捷和强大的数据互通能力。

通过实现 Trino Connector Table Sink 功能，我们能够：
- 利用 Trino 丰富的连接器生态，支持更多外部数据源的写入操作
- 简化用户配置流程，无需额外管理驱动 JAR 包
- 提供与读取功能一致的用户体验，降低学习成本
- 为用户提供完整的读写解决方案，满足更复杂的数据处理需求

## 2. 功能介绍

### Trino Connector Table Sink 概述
Trino Connector Table Sink 是 Apache Doris 新增的一项功能，允许用户通过 Trino 连接器将数据写入到外部数据源中。该功能与现有的 Trino Connector 读取功能相辅相成，共同构成了完整的读写解决方案。用户可以使用统一的语法和配置，实现对外部数据源的无缝读写操作，极大地提升了数据集成的便利性和效率。

### 技术架构
Trino Connector Table Sink 功能的架构设计遵循 Doris 现有的 Table Sink 框架，同时针对 Trino 连接器的特点进行了优化。整体架构分为前端(FE)和后端(BE)两部分：

1. **前端(FE)部分**：负责 SQL 解析、计划生成和优化，将写入操作转换为可执行的物理计划
2. **后端(BE)部分**：负责执行具体的数据写入操作，通过 JNI 调用 Trino 连接器实现对外部数据源的写入

### 核心组件

#### BE 端组件
- **TrinoConnectorTableSinkOperator**：Pipeline 执行引擎中负责 Trino 数据写入的算子，处理数据的接收和转发
- **VTrinoConnectorTableWriter**：实现了具体的数据写入逻辑，负责将数据转换为 Trino 能够处理的格式并写入目标表
- **JNI 连接层**：连接 C++ 和 Java 层，实现数据和控制指令的传递

#### FE 端组件
- **TrinoConnectorTableSink**：定义了 Trino 连接器表接收器的基本接口和行为
- **LogicalTrinoConnectorTableSink/PhysicalTrinoConnectorTableSink**：逻辑和物理计划节点，描述了 Trino 表写入操作
- **TrinoConnectorTransactionManager**：管理 Trino 连接器写入过程中的事务，确保数据的一致性和完整性
- **TrinoConnectorInsertExecutor**：执行 Trino 连接器的插入操作，协调整个写入流程

#### Trino Connector JNI 层
- **TrinoConnectorJniWriter**：Java 端的写入接口，接收来自 BE 的数据并通过 Trino 连接器写入外部数据源
- **JNI 方法映射**：C++ 和 Java 之间的方法映射，实现数据和控制流的双向传递

通过这些组件的协同工作，Trino Connector Table Sink 功能能够高效、稳定地将数据写入到各种外部数据源中，为用户提供完整的数据集成解决方案。

## 3. 设计目标
- 性能目标
- 稳定性目标
- 兼容性目标

## 4. 主要功能
- 数据写入能力
- 事务支持
- 异常处理机制
- 资源管理

## 5. 实现细节
- BE 端实现
  - Pipeline Operator 实现
  - 数据写入流程
- FE 端实现
  - Sink 规则
  - 事务管理
  - 命令处理

## 6. 使用方法
- 配置参数
- 使用示例
- 最佳实践

## 7. 测试项
- 功能测试
- 性能测试
- 稳定性测试
- 兼容性测试
- 异常场景测试

## 8. 发布节奏
- 阶段一：内部测试
- 阶段二：小规模灰度
- 阶段三：全量发布
- 后续规划

## 9. 注意事项与限制
- 已知问题
- 使用限制
- 注意事项

## 10. FAQ
- 常见问题解答