#! /bin/bash

## cancel previous flink jobs

for ID in $(sudo docker exec -it jobmanager bash -c "flink list | grep RUNNING | awk '{print \$4}'")
do 
    sudo docker exec -it jobmanager bash -c "flink cancel $ID"
done

## clean kafka topics
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic kafka-events;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics flink-events --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics kafka-events --force;

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-hourly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-weekly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q1-monthly;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-weekly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q1-monthly --force;

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-hourly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-daily;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q2-weekly;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-daily --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q2-weekly --force;

sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q3-daily;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q3-hourly;
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic q3-weekly;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q3-daily --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q3-hourly --force;
sudo docker exec kafka-broker kafka-streams-application-reset --bootstrap-servers kafka-broker:9092 --application-id  kafka-streams --input-topics q3-weekly --force;

## copy jar in flink's jobmanager
sudo docker cp target/SABD_proj2-1.0.jar jobmanager:/home

sudo docker exec taskmanager rm -rf /opt/flink/results/