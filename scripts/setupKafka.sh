#!/bin/bash

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic output
sudo docker exec kafka-broker kafka-topics --create --topic output  --bootstrap-server kafka-broker:29092 --partitions 1 --replication-factor 1
