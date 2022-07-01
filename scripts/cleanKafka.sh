#!/bin/bash

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-hourly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-weekly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-monthly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-hourly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-weekly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-daily;



sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-weekly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-monthly --force;


sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-weekly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-daily --force;