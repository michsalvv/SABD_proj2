#! /bin/bash
if [ "$#" != "2" ]; then
    echo "USAGE: sudo sh scripts/run_query_kafka.sh [QUERY] [PARALLELISM]"
    echo " > [QUERY]: Q1 / Q2"
    echo " > [PARALLELISM]: 0 ... 12"
    exit 1
fi

if [ "$1" != "Q1" ] ; then
  if [ "$1" != "Q2" ] ; then
      echo " > [QUERY]: Q1 / Q2"
      exit 1
  fi
fi


echo "------------------------------------------------------------"
echo "Cleaning Kafka Environment"
echo "------------------------------------------------------------"
sudo sh scripts/setup_kafka_env.sh 1> /dev/null 2>/dev/null

# Create topic for kafka consumer with parallelism specified by the user
sudo docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --create --topic kafka-events --partitions $2 --replication-factor 1


echo "------------------------------------------------------------"
echo "Starting Kafka Producer in Background"
echo "------------------------------------------------------------"
echo "INFO: jdk path is: '/usr/java/jdk-11.0.15/bin/java'. Change this script with your jdk-11 path."





# Change here java path
/usr/java/jdk-11.0.15/bin/java -cp target/SABD_proj2-1.0.jar kafka.Producer kafka 1> /dev/null 2>/dev/null & 
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
echo "Running Query $1"
echo "------------------------------------------------------------"
/usr/java/jdk-11.0.15/bin/java -cp target/SABD_proj2-1.0.jar kafka.Main $1
