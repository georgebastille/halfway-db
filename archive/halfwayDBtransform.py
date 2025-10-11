## Program to work out the shortest route utilising Dikstra's Algorithm.
## Limitations: All arcs are considered two-way. No negative weights.
## Gareth Latty - wrote the original classes which Rudeboy Ritchie and
## Luscious Lexy have built on.

import sqlite3
import algorithms
from graph import DiGraph
from itertools import tee, izip
import copy

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

# Combine station + line
def cosl(station, line):
    return station + '-' + str(line)

# Split station + line
def spsl(sl):
    a = sl.split('-')
    return a[0], int(a[1])


## So this bit here connects to the halfwayhouse database to populate the graph

conn = sqlite3.connect('halfway.db')

## This next line converts the pesky unicode output from sqlite3 to a string. Removes the u bit

conn.text_factory = str

"""initialise our lists"""
stops = []

## With keyword, the Python interpreter automatically releases the resources. It also provides error handling
with conn:
    cur = conn.cursor()   
    # A bit of DB house keeping
    cur.execute("""DROP TABLE IF EXISTS FULLROUTES""")
    cur.execute("""CREATE TABLE FULLROUTES(_id INTEGER PRIMARY KEY, STATIONA TEXT, STATIONB TEXT, WEIGHT REAL)""")

    cur.execute("""SELECT S.CODE FROM STATIONS AS S""")
    stops = cur.fetchall()

    cur.execute("""SELECT R.STATIONA, R.STATIONB, R.UNIMPEDED, R.LINE FROM ROUTES AS R""")
    rows = cur.fetchall()
    
### 
# Figure out how many lines go through each station
###

stopLineDict = {}
# Convert result from db to keys in a dict

for stop in stops:
    # Sets cannot contain duplicates so this gets rid of duplicate entires automatically
    stopLineDict[stop[0]]=set()

# For each station, record which lines go through it
for row in rows:
    if (row[0] in stopLineDict):
        stopLineDict[row[0]].add(row[3])
    else:
        print "Error row", row, " contains an invalid station code"

    if (row[1] in stopLineDict):
        stopLineDict[row[1]].add(row[3])
    else:
        print "Error row", row, " contains an invalid station code"

fullStops = []

# Write all the new station names (name-line no)
for station, lines in stopLineDict.iteritems():
    for line in lines:
        fullStops.append(cosl(station, line))

### 
# Update the vertices on the graph with the new station names
###
rows = [(cosl(row[0],row[3]), cosl(row[1],row[3]), row[2], row[3]) for row in rows]

### 
# Create vertices for interstation connections
###

# interstation cost (mins) 
isc = 5
interrows = []
for stopA in fullStops:
    for stopB in fullStops:
        a,b = spsl(stopA)
        c,d = spsl(stopB)
        if (a == c and not (b == d)):
            interrows.append((stopA, stopB, isc))

GMaster = DiGraph('awesome')
for stop in fullStops:
    GMaster.add_node(stop)

for row in rows:
    GMaster.add_edge(row[0], row[1], row[2])

for irow in interrows:
    GMaster.add_edge(irow[0], irow[1], irow[2])


print "Calculate Routes..."

# Holds all routes in memory to aid filtering
fromToDict = {}

for stopFrom in fullStops:
    print "Currently working on:", stopFrom
    stA, t = spsl(stopFrom)
    G = copy.deepcopy(GMaster)
    distances, previous = algorithms.dijkstra(G, stopFrom)

    for stopTo in fullStops:
        stB, t = spsl(stopTo)
        weight = distances[stopTo]
        
        dKey = (stA, stB)

        if (dKey not in fromToDict or fromToDict[dKey] > weight):
            fromToDict[dKey] = weight
    
print "Saving Routes..."
with conn:
    insStr = """INSERT INTO FULLROUTES (STATIONA, STATIONB, WEIGHT) VALUES (?, ?, ?) """

    for stations, weightBB in fromToDict.iteritems():
        print stations[0], stations[1], weightBB
        cur.execute(insStr, (stations[0], stations[1], weightBB))
        
        

