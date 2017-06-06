#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

# This script will create the tables

    # Dimension Call
def create_dimension_call():
    create = "CREATE TABLE CALL (id bigInt,  call_type text, origin_call bigInt, origin_stand int, PRIMARY KEY(id, origin_call, origin_stand, call_type));"
    session.execute(create)

#Dimension Time
def create_dimension_time():
    create = "CREATE TABLE TIME (timestamp bigInt, year int, season int, month int, day int, day_of_week int ,hour int , dayType varchar, PRIMARY KEY(timestamp, year, season, month, day, day_of_week, hour, dayType));"
    session.execute(create)

#Dimension Localisation
def create_dimension_localisation():
    create = "CREATE TABLE LOCALISATION (longitude double, latitude double, PRIMARY KEY(longitude, latitude));"
    session.execute(create)

#Dimension Taxi
def create_dimension_taxi():
    create = "CREATE TABLE TAXI (id bigInt,  primary_key(taxi));"
    session.execute(create)

#Dimension Faits
def create_dimension_taxi():
    create = "CREATE TABLE FAITS (fk_call bigInt, fk_time bigInt, fk_longitude double, fk_latitude double, fk_taxi bigInt, distance bigInt, PRIMARY KEY(fk_call, fk_time, fk_longitude, fk_latitude, fk_taxi, distance));"
    session.execute(create)

def main():
    #create_dimension_call()
    #create_dimension_time()
    #create_dimension_localisation()
    create_dimension_taxi()

if __name__ == '__main__':
    main()



