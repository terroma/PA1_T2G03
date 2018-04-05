#!/usr/bin/env bash
cd ~/learning-Kafka/kafka_2.11-1.0.0/
i=1
while [[ $i -le 3 ]]; do
 	## Create Topics EnrichTopic_{1..3} and EnrichedTopic_{1..3}
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic "EnrichTopic_$i"
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic "EnrichedTopic_$i"
	(( i++ ))
done
echo -e -n "\b##################################\n"
echo -e -n "\b##        Listing Topics        ##\n"
echo -e -n "\b##################################\n"
bin/kafka-topics.sh --list --zookeeper localhost:2181
echo -e -n "\b##################################\n"