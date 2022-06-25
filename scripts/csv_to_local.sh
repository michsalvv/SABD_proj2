#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "No arguments supplied, specify 'q1', 'q2' or 'q3'"
fi

if [ "$1" = "q1" ] || [ "$1" = "q2" ]  ||  [ "$1" = "q3" ] ; then
	sudo rm -d -r results/$1-res/;
	sudo docker cp taskmanager:/opt/flink/$1-res/ results/
else
    echo "Wrong CSV, specify 'q1', 'q2' or 'q3'"
fi

