#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

# This script will create the tables



#Fait table_by_hour
def create_by_hour():
    create = "CREATE TABLE by_hour (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY(hour, year, month, day, id));"
    session.execute(create)

#Fait table_by_start
def create_by_start():
    create = "CREATE TABLE by_start (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int,  month int, season text, dayOfWeek int, day_type text, PRIMARY KEY((longitude_start, latitude_start), distance, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

#Fait table_by_end
def create_by_end():
    create = "CREATE TABLE by_end (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY((longitude_end, latitude_end), distance, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

#Fait table_by_distance
def create_by_distance():
    create = "CREATE TABLE by_distance (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY(distance, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

#Fait table_by_taxi
def create_by_taxi():
    create = "CREATE TABLE by_taxi (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, month int, year int,  season text, dayOfWeek int, day_type text, PRIMARY KEY(taxi_id, distance, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

#fait table_by_call_type_distance
def create_by_call_type_distance():
    create = "create table by_call_type (id bigint, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigint, timestamp int, day int, hour int, year int ,month int, season text, dayofweek int, day_type text, primary key(call_type, distance, year, month, day, dayofweek, hour, id));"
    session.execute(create)

#Fait table_by_pos_call_type
def create_by_pos_call_type():
    create = "CREATE TABLE by_pos_call_type (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int ,month int, season text, dayOfWeek int, day_type text, PRIMARY KEY((longitude_start, latitude_start), call_type, distance, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

#Fait table_by_day_of_week
def create_by_day_of_week_distance():
    create = "CREATE TABLE by_day_of_week (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY(dayOfWeek, distance, year, month, day, hour, id));"
    session.execute(create)

#Fait table_by_month
def create_by_month_distance():
    create = "CREATE TABLE by_month (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY(month, distance, year, day, dayOfWeek, hour, id));"
    session.execute(create)

#Fait table_by_origin_stand
def create_by_origin_stand():
    create = "CREATE TABLE by_origin_stand (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, year int, month int, season text, dayOfWeek int, day_type text, PRIMARY KEY(origin_stand, year, month, day, dayOfWeek, hour, id));"
    session.execute(create)

def main():    
    create_by_origin_stand()
    create_by_month_distance()
    create_by_day_of_week_distance()
    create_by_call_type_distance()
    create_by_taxi()
    create_by_distance()
    create_by_start()
    create_by_end()
    create_by_hour()
   #create_by_pos_call_type()

if __name__ == '__main__':
    main()



