#! /bin/bash
echo "------------------------------------------------------------"
echo "Cleaning Flink Environment"
echo "------------------------------------------------------------"
sudo sh scripts/setup_env.sh 2>/dev/null

echo "------------------------------------------------------------"
echo "Starting Kafka Producer in Background"
echo "------------------------------------------------------------"
/usr/java/jdk-11.0.15/bin/java -cp target/SABD_proj2-1.0.jar kafka.Producer 1> /dev/null 2>/dev/null & 
sleep 0.4
echo "..."
sleep 0.4
echo "..."
sleep 0.4
echo "..."
sleep 0.4
echo "Producer Started"
sleep 1
echo "------------------------------------------------------------"
echo "Running Query $1 with parallelism $2"
echo "------------------------------------------------------------"
sudo docker exec -it jobmanager /bin/sh -c "flink run -c flink.Main /home/SABD_proj2-1.0.jar $1 $2"