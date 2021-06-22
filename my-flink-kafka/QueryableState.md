## Add Maven dependency
```xml
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
```java
Configuration config = new Configuration();
config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
return StreamExecutionEnvironment.getExecutionEnvironment(config);
```
The default proxy port is 9069. This can be changed by setting QueryableStateOptions.SERVER_PORT_RANGE.
see [MyFlinkBankJob.java](src/main/java/my/flink/kafka/bank/job/MyFlinkBankJob.java)

## StateDescriptor setQueryable 
```java
ValueStateDescriptor<Double> descriptor =
        new ValueStateDescriptor<>(
                "balance", // the state name
                TypeInformation.of(new TypeHint<Double>() {})); // type information
descriptor.setQueryable("balance");
balance = getRuntimeContext().getState(descriptor);
```
set the StateDescriptor queryable name. The client gets the queryable state by this name.  
 
Till now, the settings on the flink job is done. 

## Start the flink job
After start, check the log, search "Started Queryable State Proxy
```
[] - Started Queryable State Server @ /127.0.0.1:9067.
[] - Started Queryable State Proxy Server @ /127.0.0.1:9069.
```
The proxy is started successfully.

Then search "submitting job", the long String is "jobId", and it is needed when send query from client. 
```
[] - Submitting job c17657719aae409fdc0d88016b9cb9cb (my flink bank balance).
```

## client to query state
[MyFlinkClientApplication.java](../my-flink-client/src/main/java/my/flink/myflinkclient/MyFlinkClientApplication.java)

## client MAVEN
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-core</artifactId>
    <version>1.13.0</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-queryable-state-client-java</artifactId>
    <version>1.13.0</version>
</dependency>
```

## create client
```java
String proxyHost = "localhost";
int proxyPort = 9069;
QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);
```

## state descriptor, shall be consistent with the one on flink job side
```java
ValueStateDescriptor<Double> stateDescriptor =
        new ValueStateDescriptor<>(
                "",
                TypeInformation.of(new TypeHint<Double>() {}));
```

## read the value
```java
CompletableFuture<ValueState<Double>> completableFuture =
        client.getKvState(
                jobId,
                "balance",
                key,
                BasicTypeInfo.STRING_TYPE_INFO,
                stateDescriptor);

while(!completableFuture.isDone()) {
    Thread.sleep(100);
}

return completableFuture.get().value();
```