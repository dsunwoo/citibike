import requests
import sqlite3 as lite
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

api_key = "0d9e8b951e27bd2bde8c7820d80e2787"
url = 'https://api.forecast.io/forecast/' + api_key

cities = {"Austin": '30.303936,-97.754355',
          "Chicago": '41.837551,-87.681844',
          "Denver": '39.761850,-104.881105',
          "LosAngeles": '34.019394,-118.410825',
          "NewYork": '40.663619,-73.938589',
          "SanFrancisco": '37.727239,-123.032229'
          }

end_date = datetime.datetime.now()
con = lite.connect('weather.db')
cur = con.cursor()
cities.keys()
with con:
    cur.executescript('DROP TABLE IF EXISTS daily_temp')
    cur.execute('CREATE TABLE daily_temp '
                '(day_of_reading INT, '
                'Austin REAL, '
                'Chicago REAL, '
                'Denver REAL, '
                'LosAngeles REAL, '
                'NewYork REAL, '
                'SanFrancisco REAL);'
                )

# Create empty dataset with n days worth of rows
query_date = end_date - datetime.timedelta(days=30)  # the current value being processed
with con:
    while query_date < end_date:
        cur.execute("INSERT INTO daily_temp(day_of_reading) VALUES (?)", (int(query_date.strftime('%s')),))
        query_date += datetime.timedelta(days=1)

for k, v in cities.items():
    query_date = end_date - datetime.timedelta(days=30)  # set value each time through the loop of cities
    while query_date < end_date:
        # query for the value
        r = requests.get(url + '/' + v + ',' + query_date.strftime('%Y-%m-%dT12:00:00'))

        with con:
            # insert the temperature max to the database
            cur.execute('UPDATE daily_temp SET ' + k + ' = ' + str(r.json()['daily']['data'][0]['temperatureMax']) + ' WHERE day_of_reading = ' + query_date.strftime('%s'))

        # increment query_date to the next day for next operation of loop
        query_date += datetime.timedelta(days=1)  # increment query_date to the next day

df = pd.read_sql_query("SELECT * FROM daily_temp", con, index_col="day_of_reading")

con.close()

# Loop through each city and grab high/low to calculate range
for city in df.columns:
    t_rng = df[city].max() - df[city].min()
    t_mean = df[city].mean()
    t_var = np.std(df[city]) ** 2
    print("City: {} has a mean temperature of {} with a range of {} and a variance of {}."
          .format(city, round(t_mean, 1), round(t_rng, 1), round(t_var, 1)))
