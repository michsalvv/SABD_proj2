#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "No arguments supplied, specify 'q1', 'q2' or 'q3'"
fi

if [ "$1" = "q1" ] || [ "$1" = "q2" ]  ||  [ "$1" = "q3" ] ; then
	sudo rm -d -r results/$1-res/;
	sudo docker cp taskmanager:/opt/flink/results/$1-res/ results/
	sudo mv results/$1-res/hourly/$(ls results/$1-res/hourly/)/part-0-0.csv results/$1-res/hourly.csv
	sudo mv results/$1-res/weekly/$(ls results/$1-res/weekly/)/part-0-0.csv results/$1-res/weekly.csv
	sudo rm -d -r results/$1-res/hourly
	sudo rm -d -r results/$1-res/weekly

	if [ "$1" = q1 ] ; then
		sudo mv results/$1-res/monthly/$(ls results/$1-res/monthly/)/part-0-0.csv results/$1-res/monthly.csv
		sudo rm -d -r results/$1-res/monthly

	else
		sudo mv results/$1-res/daily/$(ls results/$1-res/daily/)/part-0-0.csv results/$1-res/daily.csv
		sudo rm -d -r results/$1-res/daily

	fi

else
    echo "Wrong CSV, specify 'q1', 'q2' or 'q3'"
fi

