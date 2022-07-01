#! /bin/bash
if [ "$#" != "1" ]; then
    echo "USAGE: sudo sh scripts/run_query_kafka.sh [QUERY]"
    echo " > [QUERY]: Q1 / Q2"
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
sudo sh scripts/setup_env.sh 2>/dev/null

echo "------------------------------------------------------------"
echo "Starting Kafka Producer in Background"
echo "------------------------------------------------------------"
echo "INFO: jdk path is: '/usr/java/jdk-11.0.15/bin/java'. Change this script with your jdk-11 path."

# Change here java path
/usr/java/jdk-11.0.15/bin/java -cp target/SABD_proj2-1.0.jar kafka.Producer 1> /dev/null 2>/dev/null & 
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