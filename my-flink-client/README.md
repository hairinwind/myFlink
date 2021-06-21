
## flink queryable state client
this is the client to get the flink queryable state   
refer
- https://github.com/dataArtisans/flink-queryable_state_demo
- https://blog.csdn.net/wangpei1949/article/details/100608828
- https://github.com/silverbulletcj/FlinkQueryableState


## to test it
- run nc -l 9000
- run TextStreamWordCountQueryableJob in my-flink
- it outputs the jobId
- type some text on the console of "nc", e.g. type "a" a couple of times
- curl localhost:8080/state/{jobId}?key=a