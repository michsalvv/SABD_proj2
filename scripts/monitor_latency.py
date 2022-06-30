#!/usr/bin/python3

# ESEMPIO: http://localhost:8081/jobs/165e9f25dc83c53381ca3259b7aafad5/metrics?get=latency.source_id.bc764cd8ddf7a0cff126f51c16239658.operator_id.fde675a803f8a5309afb6ac6f2d1146a.operator_subtask_index.0.latency_mean
# Tutte le latenze sono misurate dalla sorgente kafka

import urllib.request
import time
import sys
import os


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

latencies = []

print("Initializing latency metrics.")
while latencies == []:
    latencies = getMetricListFromURL(metricsListURL)
    print("...")
    time.sleep(0.4)

if query == "q1":
    hourVertexID = "54084f2d7567b189abcffcac722eb07c"
    weekVertexID = "37ff21011e035df90de30326f1cbb4a3"
    monthVertexID = "9fa4cb78af2c44fe18706798dab47ffb"

if query == "q2":
    hourVertexID = "bad76b9e4dd5066b43dcd20dbe3b1d30"
    dayVertexID = "c71c0e85e3eb5001976f61fd1f844a14"
    weekVertexID = "43b96dcbe43e081dbccdb8c83e216f30"

if query == "q3":
    hourVertexID = "4e0711b6163f6dafdca67082f3819e5c"
    dayVertexID = "6bb684fdc432358c715aaedbc372bf83"
    weekVertexID = "49379dd06e51c67b842e9ce40142e66e"

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

t = 0

while True:
    print("——————————————————Time: " + str(t) + "————————————————————")
    print("------------------------------------------Hour")
    for req in hourLatencies:
        u = metricsListURL + "?get=" + req
        v = getLatencyValuesFromURL(u)
        print(v)

    if query != "q1":
        print("------------------------------------------Day")
        for req in weekLatencies:
            u = metricsListURL + "?get=" + req
            v = getLatencyValuesFromURL(u)
            print(v)

    print("------------------------------------------Week")
    for req in weekLatencies:
        u = metricsListURL + "?get=" + req
        v = getLatencyValuesFromURL(u)
        print(v)

    if query == "q1":
        print("------------------------------------------Month")
        for req in monthLatencies:
            u = metricsListURL + "?get=" + req
            v = getLatencyValuesFromURL(u)
            print(v)
    print("—————————————————————————————————————————————————")
    print("")
    t = t+5
    time.sleep(5)
