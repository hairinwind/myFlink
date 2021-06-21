

## my-flink
this is the my first java flink project.

## key point
- calculate in parallel
- read state
- working with kafka, send state to kafka topic
- update the application when it is a stream (keep running)
- transaction (ACID)

## Task slot vs parallelism
https://stackoverflow.com/questions/59734770/apache-flink-number-of-task-slot-vs-env-setparallelism  
Typically each slot will run one parallel instance of your pipeline. The parallelism of the job is therefore the same as the number of slots required to run it. 

## Operators
Operators transform one or more DataStreams into a new DataStream. 

## function vs Operators
TODO

## Operators and subtasks

## Watermark
Watermark is to group messages with the same timestamp.

## job manager HA
HA - multiple job managers, one is running as leader and the rest are the standby. 
if the leader crashes, one standby job manager is elected to be a leader.  Once the crashed one is recovered, it is a standby job manager now.  
It is like kafka replica and recover

## tumbling window vs sliding window vs session window
https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/operators/windows/
- tumbling windows have a fixed size/duration and do not overlap.
- sliding windows have a fixed size/duration but they can have overlapping. For example, you could have windows of size 10 minutes that slides by 5 minutes. With this you get every 5 minutes a window that contains the events that arrived during the last 10 minutes
- Session windows don't have fixed size and the session window closes when it does not receive elements for a certain period. Obviously,No overlap.

## union, join vs connect
- union is like sql union, concat two streams
- join is like sql join. need a key from left and right to join records from left and right streams.
- connect is connecting two streams. The map function has separated function to process left stream and right stream. These two functions can share some values. 

## connect two DataStream sample source
There is one example in flink-training. org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution

## split

## AllWindowedStream
AllWindowedStream is the reverse action of windowedStream. 

## dataStream.broadcast()
send the data to all downstream nodes/operators.

## Flink Tuple
limitation: 25 fields? notNull?

## job cancel vs stop
- cancel is stopping all operators right away. 
- stop is only available when all sources implements StoppableFunction. It notifies all sources and wait for them to be stopped (the stop function of StoppableFunction is executed). It allows the system to complete calculation on all received data.   

## statr the job with savepoint
```
bin/flink run -d -s <savepoint_folder> target.jar
```
Or after the job is started, get the job Id
```
bin/flink savepoint -m <Flink_server_with_port> <jobId> <savepoint_folder>
```
You can check the job manager log and search for "Staring job <jobId> from savepoint <savepoint_folder>"

## savePoint vs checkPoint
- checkpoint is 增量 incremental backup, the data is small (comparing with savepoint) and it take less time. It happens automatically once it is enabled. Checkpoint is applied automatically when failover happens.
- savepoint is full state snapshot. It needs to save more data and taks more time. It is triggered manually.

## checkpoint barrier
todo: how does checkpoint barrier implement exactly-once


## keyed state vs operator state
- keyed state is calculating state by key
- operator state: often used in source, like FlinkKafkaConsumer 

## flink-training
- https://github.com/apache/flink-training

## processFunction vs mapFunction
process function can manipulate the time, events and states and it is a low level function. 

## map vs FlatMap
map is to convert IN to OUT 
```
DataStream<Integer> integers = env.fromElements(1, 2, 3, 4, 5);

DataStream<Integer> doubleOdds = integers
    .filter(new FilterFunction<Integer>() {
        @Override
        public boolean filter(Integer value) {
            return ((value % 2) == 1);
        }
    })
    .map(new MapFunction<Integer, Integer>() {
        @Override
        public Integer map(Integer value) {
            return value * 2;
        }
    });

doubleOdds.print();
```

flatMap is useful anytime your function might have to cope with unparsable input or other error conditions. Or whenever a single input record might produce multiple output records, e.g., if it needs to unpack an array.
```
DataStream<Integer> integers = env.fromElements(1, 2, 3, 4, 5);

DataStream<Integer> doubleOdds = integers
    .flatMap(new FlatMapFunction<Integer, Integer>() {
        @Override
        public void flatMap(Integer value, Collector<Integer> out) {
            if ((value % 2) == 1) {
                out.collect(value * 2);
            }
        }
    });

doubleOdds.print();
```
These two code segments are doing the same work. 
You can see for FlatMapFunction, you need do out.collect(...) by yourself. It is kind of Optional.map(...) vs Optional.flatMap(...). The function in the map can return the value directly and java would wrap it in Optional while the function in flatMap, you have to wrap the return value into Optional by yourself. 

## ListState<T> and MapState<T>
- use ListState<T> rather than ValueState<List<T>>
- use MapState<UK, UV> rather than ValueState<HashMap<UK, UV>>

## state share
In general, Flink's design does not allow to read from or write to state of other subtasks of the same or different operators.  
broadcast state ?

