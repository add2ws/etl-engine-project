# Etl-engine
**中文** | [English](README.en.md)

## 🚀 简介：高性能 ETL 引擎

**`etl-engine`** 是一个轻量级、面向开发者的 ETL（抽取、转换、加载）库，旨在成为 **Kettle (PDI) 的高性能替代方案**。它通过优化的数据流和并发控制，实现极致的数据同步效率。

-----

## 🔥 核心优势

我们专注于提供以下三大核心特性：

### 1\. 极致的速度 ⚡️

通过批量操作和高效的内存管道设计，显著提升数据处理和数据库 I/O 速度。

> 📊 **实测数据：** 处理 $200,000$ 条数据的插入/更新任务，`etl-engine` 的速度是 **Kettle 的 $\mathbf{2}$ 倍**。

### 2\. 运行稳健可靠 🛡️

基于**节点 (Node)** 和**管道 (Pipe)** 架构，内置背压机制和事务管理，确保在高并发和大规模数据同步场景下的任务稳定性和数据一致性。

### 3\. 易于扩展和定制化 🧩

所有功能都抽象为可插拔的**节点**。用户可以轻松继承基类，快速开发新的数据源（如 MongoDB、Redis）或自定义转换逻辑，满足特定的业务需求。

-----

## 🛠️ 使用示例

以下代码展示了如何快速构建一个将 \*\*Oracle 数据（抽取）\*\*通过 \*\*Upsert 方式同步到 PostgreSQL（加载）\*\*的 ETL 任务。

```java
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
// (假设的包路径)
import com.example.etl.engine.core.Pipe;
import com.example.etl.engine.core.Dataflow;
import com.example.etl.engine.util.Tuple2;

public class EtlDemo {
    public static void main(String[] args) {
        // 1. 获取数据源
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

        // 2. 创建输入节点 (抽取 Extract)
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from t_resident_info");

        // 3. 创建插入/更新节点 (加载 Load)
        // 批量大小 1000
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        // 设置唯一标识映射，用于判断 Insert 或 Update
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        // 4. 创建管道并连接节点
        Pipe pipe = new Pipe(1000); // 管道缓存大小 1000
        pipe.connect(sqlInputNode, upsertOutputNode);

        // 5. 启动数据流
        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.syncStart(5, TimeUnit.MINUTES); // 设置超时时间
    }
}
```

-----

## 🏗️ 架构概览

`etl-engine` 基于**流式处理**，核心由以下组件构成：

  * **Node (节点):** 数据的起点（`SqlInputNode`）、终点（`UpsertOutputNode`）和所有处理逻辑的载体。
  * **Pipe (管道):** 负责在节点间传递数据记录的高性能、有界队列。
  * **Dataflow (数据流):** 任务的编排器和执行入口。

-----

## 🤝 贡献与支持

我们欢迎社区贡献！如果您发现 Bug 或想添加新的数据源/转换节点，请随时提交 Pull Request。

  * **许可证:** [在此处填写许可证，例如 MIT 或 Apache 2.0]