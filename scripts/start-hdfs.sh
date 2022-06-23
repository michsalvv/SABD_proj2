#!/bin/bash

#FORMAT AND START HDFS AFTER DOCKER COMPOSE
sudo docker exec -it namenode /bin/sh -c "hdfs namenode -format ; ./usr/local/hadoop/sbin/start-dfs.sh";
sudo docker exec -it namenode /bin/sh -c "hdfs dfs -mkdir /flink-checkpoints";
sudo docker exec -it namenode /bin/sh -c "hdfs dfs -chown flink /flink-checkpoints";