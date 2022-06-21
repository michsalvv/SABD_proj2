#! /bin/bash

sudo docker exec -it jobmanager /bin/sh -c "flink run -c flink.Main /home/SABD_proj2-1.0.jar"