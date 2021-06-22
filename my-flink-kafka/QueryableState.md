## Add Maven dependency
```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-queryable-state-runtime_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## Enable queryable in env configuration
```
Configuration config = new Configuration();
config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
return StreamExecutionEnvironment.getExecutionEnvironment(config);
```
see [MyFlinkBankJob.java](src/main/java/my/flink/kafka/bank/job/MyFlinkBankJob.java#ENABLE_QUERYABLE_STATE_PROXY_SERVER)