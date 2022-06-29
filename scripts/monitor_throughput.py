#!/usr/bin/python3

import urllib.request
import time
import sys

query = sys.argv[1]

jobID = input("Enter JobID to Monitor: ")
hourVertexID = "hour"
dayVertexID = "day"
weekVertexID = "week"
monthVertexID = "month"

hourMeterID = "hour"
dayMeterID = "day"
weekMeterID = "week"
monthMeterID = "month"

if query == "q1":
    hourVertexID = "54084f2d7567b189abcffcac722eb07c"
    weekVertexID = "37ff21011e035df90de30326f1cbb4a3"
    monthVertexID = "9fa4cb78af2c44fe18706798dab47ffb"
    hourMeterID = "HourMetric.Throughput"
    weekMeterID = "WeekMetric.Throughput"
    monthMeterID = "MonthMetric.Throughput"

if query == "q2":
    hourVertexID = "bad76b9e4dd5066b43dcd20dbe3b1d30"
    dayVertexID = "c71c0e85e3eb5001976f61fd1f844a14"
    weekVertexID = "43b96dcbe43e081dbccdb8c83e216f30"
    hourMeterID = "Hourly_Window_Ranking_ProcessFunction.Throughput"
    dayMeterID = "Daily_Window_Ranking_ProcessFunction.Throughput"
    weekMeterID = "Weekly_Window_Ranking_ProcessFunction.Throughput"

if query == "q3":
    hourVertexID = "4e0711b6163f6dafdca67082f3819e5c"
    dayVertexID = "6bb684fdc432358c715aaedbc372bf83"
    weekVertexID = "49379dd06e51c67b842e9ce40142e66e"
    hourMeterID = "Hour_Output_Formatter_ProcessFunction.Throughput"
    dayMeterID = "Day_Output_Formatter_ProcessFunction.Throughput"
    weekMeterID = "Week_Output_Formatter_ProcessFunction.Throughput"


hourURL = "http://localhost:8081/jobs/" + jobID + \
    "/vertices/"+hourVertexID + \
    "/subtasks/metrics?get=" + hourMeterID

dayURL = "http://localhost:8081/jobs/" + jobID + \
    "/vertices/"+dayVertexID + \
    "/subtasks/metrics?get=" + dayMeterID

weekURL = "http://localhost:8081/jobs/" + jobID + \
    "/vertices/"+weekVertexID + \
    "/subtasks/metrics?get=" + weekMeterID

monthURL = "http://localhost:8081/jobs/" + jobID + \
    "/vertices/"+monthVertexID + \
    "/subtasks/metrics?get=" + monthMeterID

print("Hour Query:", hourURL)
print("Week Query:", weekURL)
print("Month Query:", monthURL)


t = 0

while True:
    print("-------------- Time: " + str(t) + " --------------")
    hourThroughput = urllib.request.urlopen(hourURL).read()
    print(hourThroughput)

    if query != "q1":
        dayThroughput = urllib.request.urlopen(dayURL).read()
        print(dayThroughput)

    weekThroughput = urllib.request.urlopen(weekURL).read()
    print(weekThroughput)

    if query == "q1":
        monthThroughput = urllib.request.urlopen(monthURL).read()
        print(monthThroughput)
    print("------------------------------------------")
    print("")
    t = t+5
    time.sleep(5)
