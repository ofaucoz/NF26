#!/usr/local/bin/ipython
import csv
import datetime
from math import radians, cos, sin, asin, sqrt
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

# This script will insert into the tables

#"CREATE TABLE CALL (id bigInt,  call_type text, origin_call bigInt, origin_stand int, PRIMARY KEY(id, origin_call, origin_stand, call_type));"
#"CREATE TABLE TIME (timestamp bigInt, year int, season int, month int, day int, day_of_week int ,hour int , dayType varchar, PRIMARY KEY(timestamp, year, season, month, day, day_of_week, hour, dayType));"
#"CREATE TABLE LOCALISATION (longitude double, latitude double, PRIMARY KEY(longitude, latitude));"
#"CREATE TABLE TAXI (id bigInt,  primary_key(taxi));"
#"CREATE TABLE FAITS (fk_call bigInt, fk_time bigInt, fk_longitude double, fk_latitude double, fk_taxi bigInt, distance bigInt, PRIMARY KEY(fk_call, fk_time, fk_longitude, fk_latitude, fk_taxi, distance));"
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def RepresentsFloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def main():
    with open('/train.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(reader)
        #"TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID",
        # "TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"
        id_call = 0
        dict_header = dict()
        dict_header[0] = "int"
        dict_header[1] = "string"
        dict_header[2] = "int"
        dict_header[3] = "int"
        dict_header[4] = "int"
        dict_header[5] = "int"
        dict_header[6] = "string"
        dict_header[7] = "string"
        dict_header[8] = "string"
        for row in reader:
            for dict_iter in range(0,len(dict_header)):
                if (row[dict_iter] == ""):
                    row[dict_iter] = 'null'
                #elif (dict_header[dict_iter]=="string"):
                #    row[dict_iter] = "'" + row[dict_iter].replace("'", "") + "'"
            trip_id = row[0]
            call_type = row[1]
            origin_call = row[2]
            origin_stand = row[3]
            taxi_id = row[4]
            timestamp = row[5]
            day_type = row[6]
            missing_data = row[7]
            polyline = row[8]
            try:
                if(missing_data != "FALSE"):
                    date = datetime.datetime.fromtimestamp(int(timestamp))
                    localisation = polyline[2:-1].replace("]","").split(",[")
                    localisation_start = localisation[0]
                    longitude_start, latitude_start = localisation_start.split(",")
                    localisation_end = localisation[-1]
                    longitude_end, latitude_end = localisation_end.split(",")
                    distance = haversine(float(longitude_start), float(latitude_start), float(longitude_end), float(latitude_end))
                    print(distance)
            except ValueError:
                continue
                
if __name__ == '__main__':
    main()



