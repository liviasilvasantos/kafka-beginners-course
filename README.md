# Notes from Apache Kafka Course

[Apache Kafka Series - Learn Apache Kafka for Beginners v3](https://www.udemy.com/course/apache-kafka/?couponCode=CP260817G1)

Instructor: Stephane Maarek.


## Install kafka local

1. Download Kafka from the official website: https://kafka.apache.org/downloads
2. Extract the downloaded file to a directory of your choice.
3. Open a terminal and navigate to the Kafka directory.

### Generate cluster ID

```shell
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
```

### Format the storage directory

```shell
bin/kafka-storage.sh format -t $CLUSTER_ID -c config/server.properties
```

### Boot the Kafka broker

```shell
bin/kafka-server-start.sh config/server.properties
```

## Kafka Commands

### List topics

```shell
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Create a topic

```shell
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo_jav --partitions 3 
```
