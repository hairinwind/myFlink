
## flink queryable state client
this is the client to get the flink queryable state  

## to test it
- run nc -l 9000
- run TextStreamWordCountQueryableJob in my-flink
- it outputs the jobId
- type some text on the console of "nc", e.g. type "a" a couple of times
- curl localhost:8080/state/{jobId}?key=a