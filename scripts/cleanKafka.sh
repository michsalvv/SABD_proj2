#!/bin/bash

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic output;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics flink-events --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics output --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-weekly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-monthly --force
