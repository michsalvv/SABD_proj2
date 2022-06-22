#!/bin/bash

#FORMAT AND START HDFS AFTER DOCKER COMPOSE
sudo docker exec -it namenode /bin/sh -c "hdfs namenode -format ; ./usr/local/hadoop/sbin/start-dfs.sh";
sudo docker cp ../docker/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar jobmanager:/opt/flink/lib;
sudo docker cp ../docker/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar taskmanager:/opt/flink/lib;