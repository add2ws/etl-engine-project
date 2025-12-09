帮我编写一个开源项目的readme.md文档，要求言简意赅，突出数据同步速度快、运行稳健、易扩展3大特性。 文档支持中文和英文：
    项目名称：etl-engine
    项目描述：一个快速、稳健、易扩展的ETL库（对标Kettle），实测200000数据，插入/更新速度是Kettle的2倍
    使用代码示例：
        ```sql
        //获取数据源
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

        //创建表输入节点
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from t_resident_info");

        //创建插入/更新节点
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        //创建管道
        Pipe pipe = new Pipe(1000);
        //连接表输入和输出节点
        pipe.connect(sqlInputNode, upsertOutputNode);

        //创建数据流实例
        Dataflow dataflow = new Dataflow(sqlInputNode);
        //启动数据流
        dataflow.syncStart(5, TimeUnit.MINUTES);

        ```

