

#!/bin/bash

# Running the Zookeeper server

cd kafka_2.11-2.4.1
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Running the Kafka server

bin/kafka-server-start.sh config/server.properties &

# Creating topics test_read, test_read2, test_read3 on a new terminal with replication and partition as 1. These topics will be further used

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test_read


# Listing the existing topics in Kafka

bin/kafka-topics.sh --list --zookeeper localhost:2181

# Running the Kafka Producer by directly piping our csv data file to the producer

kafka-console-producer.sh --broker-list localhost:9092 --topic test_read < /home/ak/project_gladiator/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv

# Running the Kafka Consumer to get the data sent by the Kafka Producer

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_read --from-beginning


# Deleting a Kafka Topic

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test_read


# Running Kafka Connect Standalone command from bin and also calling the file source config properties file


./connect-standalone.sh ../config/connect-standalone.properties ../config/connect-file-source.properties


# Kafka file source config file sample

# name=local-file-source
# connector.class=FileStreamSource
# tasks.max=1
# file=/home/ak/project_gladiator/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv
# topic=test_read

