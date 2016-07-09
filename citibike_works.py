from pandas.io.json import json_normalize
import requests
import matplotlib.pyplot as plt
import pandas as pd
import time
from dateutil.parser import parse
import collections
import sqlite3 as lite

r = requests.get('https://feeds.citibikenyc.com/stations/stations.json')
# Obtain keys
key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
df = json_normalize(r.json()['stationBeanList'])

con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
    cur.executescript('DROP TABLE IF EXISTS citibike_reference')
    cur.exucutescript('DROP TABLE IF EXISTS available_bikes')
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
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
id_bikes = collections.defaultdict(int)  # defaultdict to store available bikes by station

# loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

# iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.items():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) +
                    " WHERE execution_time = " + exec_time.strftime('%s') + ";")
"""
con = lite.connect('citi_bike.db')
cur = con.cursor()

for i in range(60):
    r = requests.get('http://www.citibikenyc.com/stations/json')
    exec_time = parse(r.json()['executionTime']).strftime("%s")

    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time,))

    for station in r.json()['stationBeanList']:
        cur.execute("UPDATE available_bikes SET _%d = %d WHERE execution_time = %s" % (station['id'], station['availableBikes'], exec_time))
    con.commit()

    time.sleep(60)

con.close()  # close the database connection when done
"""