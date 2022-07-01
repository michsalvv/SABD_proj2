#!/usr/bin/python3

import urllib.request
import time
import sys
import os

csv_header = "time;max;mean;median;min;p75;p90;p95;p98;p99;p999;stddev\n"


def substring_after(s, delim):
    return s.partition(delim)[2]


def substring_between(s, start, end):
    return s[s.find(start)+len(start):s.rfind(end)]


def getMetricListFromURL(url):
    toRemove = ["b'[", "]'", "{", "}", "\"", "id:"]

    metricsResult = str(urllib.request.urlopen(url).read())

    for r in toRemove:
        metricsResult = metricsResult.replace(r, "")

    metricsList = metricsResult.split(',')
    latencyList = []

    for s in metricsList:
        if "latency" in s:
            latencyList.append(s)

    return latencyList


def getLatencyValuesFromURL(url):
    toRemove = ["b'[", "]'", "{", "}", "\"", "id:"]

    latencyRes = str(urllib.request.urlopen(url).read())

    for r in toRemove:
        latencyRes = latencyRes.replace(r, "")

    val = substring_after(latencyRes, "value:")
    stat = substring_between(latencyRes, "latency_", ",value")

    return stat + " : " + val


query = sys.argv[1]
parall = sys.argv[2]

os.system('cls' if os.name == 'nt' else 'clear')
print("╔═══════════════════════════════════════════════════╗")
print("║                Latency Monitoring                 ║")
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

metricsListURL = "http://localhost:8081/jobs/" + jobID + "/metrics"
print("Query:", metricsListURL)

ready = False
latencies = getMetricListFromURL(metricsListURL)

if query == "q1":
    hourVertexID = "54084f2d7567b189abcffcac722eb07c"
    weekVertexID = "37ff21011e035df90de30326f1cbb4a3"
    monthVertexID = "9fa4cb78af2c44fe18706798dab47ffb"

if query == "q2":
    hourVertexID = "bad76b9e4dd5066b43dcd20dbe3b1d30"
    dayVertexID = "c71c0e85e3eb5001976f61fd1f844a14"
    weekVertexID = "43b96dcbe43e081dbccdb8c83e216f30"

if query == "q3":
    hourVertexID = "8422bf6d2e8d4274034e58b5c32c2a8d"
    dayVertexID = "5e0502c34a694fec3eae449313a45aa6"
    weekVertexID = "4607136e4110d037328faa5a96eea425"

hourLatencies = []
dayLatencies = []
weekLatencies = []
monthLatencies = []
for i in latencies:
    if hourVertexID in i:
        hourLatencies.append(i)
    if dayVertexID in i:
        dayLatencies.append(i)
    if weekVertexID in i:
        weekLatencies.append(i)
    if monthVertexID in i:
        monthLatencies.append(i)

print("Initializing Metrics List")
if query != "q1":
    while hourLatencies == [] or dayLatencies == [] or weekLatencies == []:
        hourLatencies = []
        dayLatencies = []
        weekLatencies = []
        latencies = getMetricListFromURL(metricsListURL)
        for i in latencies:
            if hourVertexID in i:
                hourLatencies.append(i)
            if dayVertexID in i:
                dayLatencies.append(i)
            if weekVertexID in i:
                weekLatencies.append(i)
        time.sleep(1)
        print("...")
else:
    while hourLatencies == [] or weekLatencies == [] or monthLatencies == []:
        hourLatencies = []
        weekLatencies = []
        monthLatencies = []
        latencies = getMetricListFromURL(metricsListURL)
        for i in latencies:
            if hourVertexID in i:
                hourLatencies.append(i)
            if weekVertexID in i:
                weekLatencies.append(i)
            if monthVertexID in i:
                monthLatencies.append(i)
        time.sleep(1)
        print("...")


t = 0
filenameHour = "results/latency/lat_hour_" + query + "_" + parall + ".csv"
filenameDay = "results/latency/lat_day_" + query + "_" + parall + ".csv"
filenameWeek = "results/latency/lat_week_" + query + "_" + parall + ".csv"
filenameMonth = "results/latency/lat_month_" + query + "_" + parall + ".csv"

if os.path.exists(filenameHour):
    os.remove(filenameHour)
if os.path.exists(filenameDay):
    os.remove(filenameDay)
if os.path.exists(filenameWeek):
    os.remove(filenameWeek)
if os.path.exists(filenameMonth):
    os.remove(filenameMonth)

fileHour = open(filenameHour, 'a+')
fileHour.write(csv_header)
fileWeek = open(filenameWeek, 'a+')
fileWeek.write(csv_header)
if query != "q1":
    fileDay = open(filenameDay, 'a+')
    fileDay.write(csv_header)
else:
    fileMonth = open(filenameMonth, 'a+')
    fileMonth.write(csv_header)

while t != 65:

    print("—————————————————— Time: " + str(t) + " ————————————————————")
    print("------------------------------------------Hour")
    hourStats = []
    for req in hourLatencies:
        u = metricsListURL + "?get=" + req
        v = getLatencyValuesFromURL(u)
        hourStats.append(v)
        print(v)
    hourStats.sort()
    hourCsvLine = str(t)
    for i in hourStats:
        hourCsvLine = hourCsvLine + ";" + substring_after(i, " : ")
    fileHour.write(hourCsvLine + "\n")

    if query != "q1":
        print("------------------------------------------Day")
        dayStats = []
        for req in dayLatencies:
            u = metricsListURL + "?get=" + req
            v = getLatencyValuesFromURL(u)
            dayStats.append(v)
            print(v)
        dayStats.sort()
        dayCsvLine = str(t)
        for i in dayStats:
            dayCsvLine = dayCsvLine + ";" + substring_after(i, " : ")
        fileDay.write(dayCsvLine + "\n")

    print("------------------------------------------Week")
    weekStats = []
    for req in weekLatencies:
        u = metricsListURL + "?get=" + req
        v = getLatencyValuesFromURL(u)
        weekStats.append(v)
        print(v)
    weekStats.sort()
    weekCsvLine = str(t)
    for i in weekStats:
        weekCsvLine = weekCsvLine + ";" + substring_after(i, " : ")

    fileWeek.write(weekCsvLine + "\n")

    if query == "q1":
        print("------------------------------------------Month")
        monthStats = []
        for req in monthLatencies:
            u = metricsListURL + "?get=" + req
            v = getLatencyValuesFromURL(u)
            monthStats.append(v)
            print(v)
        monthStats.sort()
        monthCsvLine = str(t)
        for i in monthStats:
            monthCsvLine = monthCsvLine + ";" + substring_after(i, " : ")

        fileMonth.write(monthCsvLine + "\n")

    print("—————————————————————————————————————————————————")
    print("")
    t = t+5
    time.sleep(5)

fileHour.close()
fileWeek.close()
if query != "q1":
    fileDay.close()
else:
    fileMonth.close()
