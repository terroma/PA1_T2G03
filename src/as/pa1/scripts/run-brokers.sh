#!/usr/bin/env bash
cd ~/learning-Kafka/kafka_2.11-1.0.0/

bin/kafka-server-start.sh ~/NetBeansProjects/PA1_T2G03/src/as/pa1/scripts/configs/server-0.properties &
sleep 5
bin/kafka-server-start.sh ~/NetBeansProjects/PA1_T2G03/src/as/pa1/scripts/configs/server-1.properties &
sleep 5
bin/kafka-server-start.sh ~/NetBeansProjects/PA1_T2G03/src/as/pa1/scripts/configs/server-2.properties &