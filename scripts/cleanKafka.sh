#!/bin/bash

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events;