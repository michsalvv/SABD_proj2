#! /bin/bash
if [ "$#" != "2" ]; then
    echo "USAGE: sudo sh scripts/run_query_flink.sh [QUERY] [PARALLELISM]"
    echo " > [QUERY]: Q1 / Q2 / Q3"
    echo " > [PARALLELISM]: 0 ... 12"
    exit 1
fi

if [ "$1" != "Q1" ] ; then
  if [ "$1" != "Q2" ] ; then
    if [ "$1" != "Q3" ] ; then
      echo " > [QUERY]: Q1 / Q2 / Q3"
      exit 1
    fi
  fi
fi


echo "------------------------------------------------------------"
echo "Cleaning Flink Environment"
echo "------------------------------------------------------------"
sudo sh scripts/setup_env.sh 2>/dev/null

echo "------------------------------------------------------------"
echo "Starting Kafka Producer in Background"
echo "------------------------------------------------------------"
echo "INFO: jdk path is: '/usr/java/jdk-11.0.15/bin/java'. Change this script with your jdk-11 path."

# Change here java path
/usr/java/jdk-11.0.15/bin/java -cp target/SABD_proj2-1.0.jar kafka.Producer flink 1> /dev/null 2>/dev/null & 
sleep 0.4
echo "..."
sleep 0.4
echo "..."
sleep 0.4
echo "..."
sleep 0.4
echo "Producer Started"
sleep 0.5
echo "------------------------------------------------------------"
echo "Running Query $1 with parallelism $2"
echo "------------------------------------------------------------"
sudo docker exec -it jobmanager /bin/sh -c "flink run -c flink.Main /home/SABD_proj2-1.0.jar $1 $2"