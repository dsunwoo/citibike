from pandas.io.json import json_normalize
import requests
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3 as lite
# a package with datetime objects
import time
# a package for parsing a string into a Python datetime object
from dateutil.parser import parse
import collections

r = requests.get('https://feeds.citibikenyc.com/stations/stations.json')
# Obtain keys
key_list = []  # unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
df = json_normalize(r.json()['stationBeanList'])
# This section of code was for the exercises in Unit 3
"""
#  Challenge exercises from Unit 3.1.3
bmean1 = df['availableBikes'].mean()  # 11.85
bmedian1 = df['availableBikes'].median()  # 8.0
# Calculate on in service stations only
bmean2 = df[df.statusValue == "In Service"].mean()['availableBikes']
bmedian2 = df[df.statusValue == "In Service"].median()['availableBikes']
# Calculate on in service stations: method described in Thinkful.com
bmean3 = df[df['statusValue'] == "In Service"]['availableBikes'].mean()
bmedian3 = df[df['statusValue'] == "In Service"]['availableBikes'].median()

df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()
"""
# Create a database to store results
con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
  cur.executescript('DROP TABLE IF EXISTS citibike_reference')
  cur.executescript('DROP TABLE IF EXISTS available_bikes')
with con:
    cur.execute('CREATE TABLE citibike_reference '
                '(id INT PRIMARY KEY, '
                'totalDocks INT, '
                'city TEXT, '
                'altitude INT, '
                'stAddress2 TEXT, '
                'longitude NUMERIC, '
                'postalCode TEXT, '
                'testStation TEXT, '
                'stAddress1 TEXT, '
                'stationName TEXT, '
                'landMark TEXT, '
                'latitude NUMERIC, '
                'location TEXT )'
                )
# a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference " \
      "(id, totalDocks, city, altitude, stAddress2, longitude, " \
      "postalCode, testStation, stAddress1, stationName, landMark, latitude, location) " \
      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
# for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        # id, totalDocks, city, altitude, stAddress2, longitude, postalCode,
        # testStation, stAddress1, stationName, landMark, latitude, location
        cur.execute(sql,
                    (station['id'],
                     station['totalDocks'],
                     station['city'],
                     station['altitude'],
                     station['stAddress2'],
                     station['longitude'],
                     station['postalCode'],
                     station['testStation'],
                     station['stAddress1'],
                     station['stationName'],
                     station['landMark'],
                     station['latitude'],
                     station['location'])
                    )
# extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist()
# add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]
# create the table
# in this case, we're concatenating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " + ", ".join(station_ids) + ");")
# take the string and parse it into a Python datetime object
exec_time = parse(r.json()['executionTime'])
with con:
    # The following line modified from ...<exec_time.strftime('%s')>... to conform with windows datetime
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
id_bikes = collections.defaultdict(int)  # defaultdict to store available bikes by station

# loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

# iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.items():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) +
                    " WHERE execution_time = " + exec_time.strftime('%Y-%m-%dT%H:%M:%S') + ";")
con.close()
