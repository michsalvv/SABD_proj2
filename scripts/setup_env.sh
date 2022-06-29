#! /bin/bash

## cancel previous flink jobs

for ID in $(sudo docker exec -it jobmanager bash -c "flink list | grep RUNNING | awk '{print \$4}'")
do 
    sudo docker exec -it jobmanager bash -c "flink cancel $ID"
done

## clean kafka topic
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --delete --topic flink-events

## copy jar in flink's jobmanager
sudo docker cp target/SABD_proj2-1.0.jar jobmanager:/home

sudo docker exec taskmanager rm -rf /opt/flink/results/