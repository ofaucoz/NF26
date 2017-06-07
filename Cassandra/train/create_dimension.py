#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

# This script will create the tables


#Fait distance_localisation
def create_distance_localisation():
    create = "CREATE TABLE distance_localisation (longitude_start double ,latitude_start double, distance bigInt, PRIMARY KEY(longitude_start, latitude_start, distance));"
    session.execute(create)

#Fait courses_localisation
def create_courses_localisation():
    create = "CREATE TABLE courses_localisation (longitude_start double ,latitude_start double, courses bigInt, PRIMARY KEY(longitude_start, latitude_start, courses));"
    session.execute(create)

#Fait call_type_localisation
def create_calltype_localisation():
    create = "CREATE TABLE calltype_localisation (longitude_start double ,latitude_start double, call_type bigInt, PRIMARY KEY(longitude_start, latitude_start, call_type));"
    session.execute(create)

#Fait nb_courses_taxi
def create_courses_taxi():
    create = "CREATE TABLE courses_taxi (taxi_id bigInt, courses bigInt, PRIMARY KEY(taxi_id, courses));"
    session.execute(create)

#Fait taxi_localisation
def create_taxi_localisation():
    create = "CREATE TABLE taxi_localisation (taxi_id bigInt, longitude double, latitude double, PRIMARY KEY(taxi_id, longitude, latitude));"
    session.execute(create)

#Fait hour_nb_courses
def create_hour_courses():
    create = "CREATE TABLE courses_hour (hour int, course bigInt, PRIMARY KEY(hour, course));"
    session.execute(create)

#Fait month_nb_courses
def create_month_courses():
    create = "CREATE TABLE courses_month (month int, course bigInt, PRIMARY KEY(month, course));"
    session.execute(create)


#Fait season_nb_courses
def create_season_courses():
    create = "CREATE TABLE courses_season (season int, course bigInt, PRIMARY KEY(season, course));"
    session.execute(create)

#Fait dayOfWeek_nb_courses
def create_dayOfWeek_courses():
    create = "CREATE TABLE courses_dayOfWeek (dayOfWeek int, course bigInt, PRIMARY KEY(dayOfWeek, course));"
    session.execute(create)

#Fait all_table
def create_all_table():
    create = "CREATE TABLE all_table (id bigInt, longitude_start double, latitude_start double, longitude_end double, latitude_end double, distance double, call_type text, origin_call int, origin_stand int, taxi_id bigInt, timestamp int, day int, hour int, month int, season text, day_type text, PRIMARY KEY(id));"
    session.execute(create)
def main():
    #create_courses_localisation()
    #create_calltype_localisation()
    #create_courses_taxi()
    #create_taxi_localisation()
    #create_hour_courses()
    #create_month_courses()
    #create_season_courses()
    #create_dayOfWeek_courses()
    #create_distance_localisation()
    create_all_table()

if __name__ == '__main__':
    main()



