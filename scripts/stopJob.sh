#!/bin/bash
for ID in $(sudo docker exec -it jobmanager bash -c "flink list | grep RUNNING | awk '{print \$4}'")
do 
    sudo docker exec -it jobmanager bash -c "flink cancel $ID"
done
