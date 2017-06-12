#!/usr/local/bin/ipython
import csv
#import matplotlib.pyplot as plt
#import plotly.plotly as py
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

# This script will query the tables 

# Expected queries : 

# select longitude_start, latitude_start, count(*) as value_occurence from by_start group by longitude_start, latitude_start; 
# select longitude_start, latitude_start, avg(distance) as distance_moy from by_start group by longitude_start, latitude_start; 
# select call_type, count(*) as occurences from by_call_type group by call_type
# select call_type, avg(distance) as distance from by_call_type group by call_type
# select dayofweek, count(*) as occurences from by_day_of_week group by dayofweek
# select day_of_week, avg(distance) as distance from by_day_of_week group by day_of_week
# select year, month, sum(distance) as total_distance from by_distance group by year, month;
# select longitude_end, latitude_end, count(*) as value_occurence from by_end group by longitude_end, latitude_end; 
# select longitude_end, latitude_end, avg(distance) as distance_moy from by_end group by longitude_end, latitude_end; 
# select hour, year, month,  count(*) as occurences from by_hour group by year, month, hour
# select month, count(*) as occurences from by_month group by month
# select month, avg(distance) as distance from by_month group by month
# select origin_stand, count(*) as occurences from by_origin_stand group by origin_stand
# select origin_stand, year, month,  count(*) as occurences from by_origin_stand group by origin_stand, year, month
# select longitude_start, latitude_start, call_type, count(*) as occurences from by_pos_call_type group by longitude_start, latitude_start, call_type 
# select taxi_id, count(*) as occurences from by_taxi group by taxi_id
# select taxi_id, avg(distance) as distance from taxi_id group by taxi_id
def select():
   rows = session.execute('select longitude_start, latitude_start, count(*) as value_occurence from by_start group by longitude_start, latitude_start;')
   print(type(rows))
   return rows

def main():    
   select()
   # plt.bar(x, y, width, color='blue')
   # fig = plt.gcf()
   # plot_url = py.plot_mpl(fig, filename='mpl-basic-bar')
   

if __name__ == '__main__':
    main()



