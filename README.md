# Etl-engine
 [中文](README.md) | **English**

## 🚀 Introduction: High-Performance ETL Engine

**`etl-engine`** is a lightweight, developer-focused ETL (Extract, Transform, Load) library designed to be a **high-performance alternative to Kettle (PDI)**. It achieves ultimate data synchronization efficiency through optimized data flow and concurrency control.

-----

## 🔥 Core Advantages

We focus on delivering three core characteristics:

### 1\. Extreme Speed ⚡️

Significantly improves data processing and database I/O speed through batch operations and an efficient in-memory pipeline design.

> 📊 **Real-World Test:** For an insert/update task involving $200,000$ records, the speed of `etl-engine` is **$\mathbf{2}$ times faster than Kettle**.

### 2\. Robust and Stable Operation 🛡️

Built on a **Node** and **Pipe** architecture, it includes built-in backpressure mechanisms and transaction management, ensuring task stability and data consistency in high-concurrency and large-scale synchronization scenarios.

### 3\. Easy to Extend and Customize 🧩

All features are abstracted as pluggable **Nodes**. Users can easily inherit base classes to quickly develop new data sources (e.g., MongoDB, Redis) or custom transformation logic, meeting specific business requirements.

-----

## 🛠️ Usage Example

The following code demonstrates how to quickly build an ETL task that **extracts data from Oracle** and **synchronizes (Upsert) it to PostgreSQL (Load)**.

```java
// 1. Get Data Sources
DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

// 2. Create Input Node (Extract)
SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from t_resident_info");

// 3. Create Upsert/Update Node (Load)
// Batch size 1000
UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
// Set identity mapping for Insert or Update determination
upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

// 4. Create Pipe and Connect Nodes
Pipe pipe = new Pipe(1000); // Pipe buffer size 1000
pipe.connect(sqlInputNode, upsertOutputNode);

// 5. Start Dataflow
Dataflow dataflow = new Dataflow(sqlInputNode);
dataflow.syncStart(5, TimeUnit.MINUTES); // Set timeout
```

-----

## 🏗️ Architecture Overview

`etl-engine` is based on **stream processing**, composed of the following core components:

  * **Node:** The carrier for the start point (`SqlInputNode`), end point (`UpsertOutputNode`), and all processing logic in the data flow.
  * **Pipe:** A high-performance, bounded queue responsible for efficiently transferring data records between nodes.
  * **Dataflow:** The orchestrator and execution entry point for the task.

-----

## 🤝 Contribution and Support

We welcome community contributions\! If you find a bug or would like to add a new data source/transformation node, please feel free to submit a Pull Request.

  * **License:** [Insert License here, e.g., MIT or Apache 2.0]