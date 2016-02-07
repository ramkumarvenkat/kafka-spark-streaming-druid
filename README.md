# kafka-spark-streaming-druid
Takes a kafka stream into spark, apply transformations and sink into Druid. Everything Dockerised.

This is a quickstart project that can help newbies get started on setting up the infrastructure for their big data needs. The setup consists of kafka, spark and druid and the process is automated using Docker.

<b> How to get started </b>
* Clone the repo
* Edit  [docker-compose.yaml](docker-compose.yaml). 
Change kafka container's env ```ADVERTISED_HOST=10.24.48.147``` to your Docker host's ip. Change spark's volume ```/home/rvenkataraman/lambda-docker-poc/spark-job:/usr/local/jobs``` to point to any directory of your choice in your docker host
* Kafka container needs the ```ADVERTISED_HOST``` to be set for the kafka producer to run from outside the container
* Volume for the spark container is completely optional. This is intended to have the spark jar copied into the docker host and have it automatically shared with the spark container. You can also manually ```scp``` the files if you need
* Copy the spark jar onto the directory shared in volumes on the docker-host or directly to the container's FS
* The spark job needs a config YAML that it picks the properties from. Sample config is here: [pipeline-config.yaml](src/main/resources/pipeline-config.yaml). The defaults should be sane, if you don't fiddle around too much with the [docker-compose.yaml](docker-compose.yaml)
* Start the containers ```docker-compose up -d```
* Inspect if the containers are running ```docker ps```
* Start the kafka producer by running this [KafkaTasks.scala](src/main/scala/pipeline/kafka/KafkaTasks.scala). Please ensure you change the metadata broker ip to the docker-host's ip.
* ssh into the spark container ```docker exec -it <spark-container-id> bash```
* Go to ```/usr/local/spark/bin``` and run ```./spark-submit --master yarn-client --class pipeline.Runner <path-to-spark-fat-jar> <path-to-spark-job-config>```
* You can inspect the YARN ResourceManager at ```docker-host:8088``` and druid indexer at ```docker-host:9090```
* If you are using Imply's ```pivot``` to visualise the druid data-source, the broker is available at ```docker-host:9082```. These ports can be overridden in the ```ports section in [docker-compose.yaml](docker-compose.yaml)

<b> How to use custom data for the events </b>
* If you look at [Avro data-model](src/main/resources/pipeline.model.avro/KafkaEvent.avsc), the data for the kafka event is a ```Map<String,String>```. You can set any set of keys and values into the kafka event here: [KafkaTasks.scala](src/main/scala/pipeline/kafka/KafkaTasks.scala)
* To make it a little easier, I have a sample data-set here: [Sample data-set](src/main/resources/SampleEventDataset.csv). The first line defines the key names and subsequent lines define a list of comma-separated values. If a value is null, it will be left as blank. 
* The [KafkaTasks.scala](src/main/scala/pipeline/kafka/KafkaTasks.scala) needs 2 parameters: [Sample data-set](src/main/resources/SampleEventDataset.csv) and [pipeline-config.yaml](src/main/resources/pipeline-config.yaml)
* You can define custom keys in the sample data-set and push the events to kafka.
* To incorporate those into Druid, make changes to ```aggregators``` section in [pipeline-config.yaml](src/main/resources/pipeline-config.yaml). By default, the dimestions are taken as is (all the keys from the kafka event) and metrics have to defined separately. Make changes to the ```field-name``` to point to the correct field name you had given in your data-set.
* If you want to change metrics aggregation to other custom type/granularity, you can do the same in the config yaml file itself. We use [Tranquility](https://github.com/druid-io/tranquility) to pump data from spark into Druid. 
* If you want to change spark transformations/actions, you can do that in [SparkStreamingTasks.scala](src/main/scala/pipeline/spark/SparkStreamingTasks.scala)

