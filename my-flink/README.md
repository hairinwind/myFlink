
## create the sample project
following this document https://ci.apache.org/projects/flink/flink-docs-release-1.2/quickstart/java_api_quickstart.html
```
mvn archetype:generate                               \
       -DarchetypeGroupId=org.apache.flink           \
       -DarchetypeArtifactId=flink-quickstart-java   \
       -DarchetypeVersion=1.2.1
```

## setup Flink on local machine
https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/tutorials/local_setup.html  

download the flink 
https://flink.apache.org/downloads.html

extract the tar

## run your code on flink
the project from flink-quickstart contains two java main class: WordCount and SocketTextStreamWordCount

### run WordCount
```
bin/flink run -c my.flink.WordCount ~/myworkspace/myFlink/my-flink/target/my-flink-1.0-SNAPSHOT.jar
```
Then you shall see the word count result

### run SocketTextStreamWordCount
As SocketTextStreamWordCount is listening on one port, we need start run netcat first.
```
nc -l 9000
```
Then run this
```
bin/flink run -c my.flink.SocketTextStreamWordCount ~/myworkspace/myFlink/my-flink/target/my-flink-1.0-SNAPSHOT.jar localhost 9000
```
Type something on the conosle of "nc" which is sending the text to localhost:9000  
Check the log
```
tail -f log/flink-*-taskexecutor-*.out
```
You shall see the count result. 

You can also check the job status from flink dashboard http://localhost:8081/

To stop the flink job, you need find the job id from the web UI, or from ```bin/flink list```
```
bin/flink stop --savepointPath /home/yao/myworkspace/flink-data <JOB_ID>
```

To start a job from a Savepoint
```
bin/flink run --detached --fromSavepoint /home/yao/myworkspace/flink-data/savepoint-99c29f-8eb6b230e77d \
       -c my.flink.SocketTextStreamWordCount ~/myworkspace/myFlink/my-flink/target/my-flink-1.0-SNAPSHOT.jar localhost 9000 
```

more CLI options: https://ci.apache.org/projects/flink/flink-docs-master/docs/deployment/cli/

## run it on kubernetes
### set docker repo to minikube
I already have minikube installed on my local. Run this to ensure the docker is pushing the image to the minikube docker repo
```
eval $(minikube docker-env)
```
Now you don't need run "docker push" to push the image to remote dockerhub.

### create docker image
```
docker build -t my-flink .
```
After it is done, run ```docker image ls |grep flink``` and you can see the image there. 

## start flink application cluster
./bin/flink run-application \
    --detached \
    --parallelism 4 \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=k8s-ha-app-1 \
    -Dkubernetes.container.image=my-flink \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dkubernetes.taskmanager.cpu=0.5 \
    -Dtaskmanager.numberOfTaskSlots=4 \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    -Dhigh-availability=org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory \
    -Dhigh-availability.storageDir=s3://flink-bucket/flink-ha \
    -Drestart-strategy=fixed-delay \
    -Drestart-strategy.fixed-delay.attempts=10 \
    -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.1.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.1.jar \
    local:///opt/flink/usrlib/my-flink-1.0-SNAPSHOT.jar