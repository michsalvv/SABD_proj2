#!/usr/bin/python3

import urllib.request
import time
import sys
import csv
import os

query = sys.argv[1]
parall = sys.argv[2]


def substring_after(s, delim):
    return s.partition(delim)[2]


def substring_between(s, start, end):
    return s[s.find(start)+len(start):s.rfind(end)]


def getThroughputFromURL(url):
    res = str(urllib.request.urlopen(url).read())

    toRemove = ["b'[", "]'", "{", "}", "\"", "id:"]
    for r in toRemove:
        res = res.replace(r, "")

    stat = "stat"
    if "Hour" in res:
        stat = "Hour"
    if "Day" in res or "Daily" in res:
        stat = "Day"
    if "Week" in res:
        stat = "Week"
    if "Month" in res:
        stat = "Month"

    thr = substring_after(res, "sum:")

    return stat + " : " + thr


os.system('cls' if os.name == 'nt' else 'clear')
print("╔═══════════════════════════════════════════════════╗")
print("║                Throughput Monitoring              ║")
print("╚═══════════════════════════════════════════════════╝")
jobID = input("Enter JobID to Monitor: ")
hourVertexID = "hour"
dayVertexID = "day"
weekVertexID = "week"
monthVertexID = "month"

hourMeterID = "hour"
dayMeterID = "day"
weekMeterID = "week"
monthMeterID = "month"

csvHeader = ""

if query == "q1":
    hourVertexID = "54084f2d7567b189abcffcac722eb07c"
    weekVertexID = "37ff21011e035df90de30326f1cbb4a3"
    monthVertexID = "9fa4cb78af2c44fe18706798dab47ffb"
    hourMeterID = "HourMetric.Throughput"
    weekMeterID = "WeekMetric.Throughput"
    monthMeterID = "MonthMetric.Throughput"
    csvHeader = "time;hour;week;month"

if query == "q2":
    hourVertexID = "bad76b9e4dd5066b43dcd20dbe3b1d30"
    dayVertexID = "c71c0e85e3eb5001976f61fd1f844a14"
    weekVertexID = "43b96dcbe43e081dbccdb8c83e216f30"
    hourMeterID = "Hourly_Window_Ranking_ProcessFunction.Throughput"
    dayMeterID = "Daily_Window_Ranking_ProcessFunction.Throughput"
    weekMeterID = "Weekly_Window_Ranking_ProcessFunction.Throughput"
    csvHeader = "time;hour;day;week"


if query == "q3":
    hourVertexID = "4e0711b6163f6dafdca67082f3819e5c"
    dayVertexID = "6bb684fdc432358c715aaedbc372bf83"
    weekVertexID = "49379dd06e51c67b842e9ce40142e66e"
    hourMeterID = "Hour_Output_Formatter_ProcessFunction.Throughput"
    dayMeterID = "Day_Output_Formatter_ProcessFunction.Throughput"
    weekMeterID = "Week_Output_Formatter_ProcessFunction.Throughput"
    csvHeader = "time;hour;day;week"


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
filename = "results/thr_" + query + "_" + parall + ".csv"
if os.path.exists(filename):
    os.remove(filename)
f = open(filename, 'a')

while t != 65:
    csvLine = ""
    print("-------------- Time: " + str(t) + " --------------")
    csvLine = csvLine + str(t) + ";"

    hourThroughput = getThroughputFromURL(hourURL)
    print(hourThroughput)
    csvLine = csvLine + substring_after(hourThroughput, "Hour : ") + ";"

    if query != "q1":
        dayThroughput = getThroughputFromURL(dayURL)
        print(dayThroughput)
        csvLine = csvLine + substring_after(dayThroughput, "Day : ") + ";"

    weekThroughput = getThroughputFromURL(weekURL)
    print(weekThroughput)
    csvLine = csvLine + substring_after(weekThroughput, "Week : ")

    if query == "q1":
        monthThroughput = getThroughputFromURL(monthURL)
        print(monthThroughput)
        csvLine = ";" + csvLine + substring_after(hourThroughput, "Month : ")

    print("CSV: " + csvLine)
    f.write(csvLine + "\n")

    print("------------------------------------------")
    print("")
    t = t+5
    time.sleep(5)
f.close()
