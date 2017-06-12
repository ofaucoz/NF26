#!/usr/local/bin/ipython
import csv
import datetime
import progressbar
import random
from math import radians, cos, sin, asin, sqrt
from cassandra.query import BatchStatement
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
    rand_value = random.randint(1,100)
    print("Reading CSV")
    tables = ['by_hour', 'by_start', 'by_end', 'by_distance', 'by_taxi', 'by_call_type', 'by_day_of_week', 'by_month', 'by_origin_stand' ,'by_pos_call_type']
    # tables = ['by_pos_call_type']
    with open('/train.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(reader)
        #"TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID",
        # "TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"
        id = 0
        dict_header = dict()
        dict_time = dict()
        dict_time["hour"] = dict()
        dict_time["month"] = dict()
        dict_time["dayOfWeek"] = dict()
        dict_time["season"] = dict()
        dict_header[0] = "int"
        dict_header[1] = "string"
        dict_header[2] = "int"
        dict_header[3] = "int"
        dict_header[4] = "int"
        dict_header[5] = "int"
        dict_header[6] = "string"
        dict_header[7] = "string"
        dict_header[8] = "string"
        p = progressbar.ProgressBar(maxval=len(list(csv.reader(open("/train.csv")))), \
                    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        for row in reader:
            rand = random.randint(1,100)
            p.update(id)
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
            longitude_start = 0.0
            latitude_start = 0.0
            longitude_end = 0.0
            latitude_end = 0.0
            try:
                if(missing_data != "FALSE" and rand > 95):
                    date = datetime.datetime.fromtimestamp(int(timestamp))
                    localisation = polyline[2:-1].replace("]","").split(",[")
                    localisation_start = localisation[0]
                    longitude_start, latitude_start = localisation_start.split(",")
                    localisation_end = localisation[-1]
                    longitude_end, latitude_end = localisation_end.split(",")
                    distance = haversine(float(longitude_start), float(latitude_start),
                            float(longitude_end), float(latitude_end))
                    if ((date.month > 6 and date.month < 9) or 
                        (date.month == 6 and date.day >= 21) or 
                        (date.month == 9 and date.day <= 21)):
                        season = "ete"
                    if ((date.month > 9 and date.month < 12) or 
                        (date.month == 9 and date.day >= 22) or 
                        (date.month == 12 and date.day <= 20)):
                        season = "automne"
                    if ((date.month < 3) or 
                        (date.month == 12 and date.day >= 21) or 
                        (date.month == 3 and date.day <= 19)):
                        season = "hiver"
                    if ((date.month > 3 and date.month < 6) or 
                        (date.month == 3 and date.day >= 19) or 
                        (date.month == 6 and date.day <= 20)):
                        season = "printemps"
                    if (not season in dict_time["season"]):
                        dict_time["season"][season] = 0
                    dict_time["season"][season] += 1

                    for table in tables:
                        if((table == "by_origin_stand") and (origin_stand == 'null')):
                            continue
                        insert = ("insert into " + table + "(id, longitude_start, latitude_start,"
                            + "longitude_end, latitude_end, distance, call_type, origin_call"
                            + ", origin_stand, taxi_id, timestamp, day, hour, month, year, season, dayOfWeek,"
                            + "day_type) values("
                            + str(id) + "," + str(round(float(longitude_start), 3)) + "," 
                            + str(round(float(latitude_start), 3)) 
                            + "," + str(round(float(longitude_end), 3)) 
                            + "," + str(round(float(latitude_end), 3)) + "," + str(round(float(distance),1)) 
                            + "," + "'" + str(call_type) + "'" + "," + str(origin_call) + "," 
                            + str(origin_stand) + "," + str(taxi_id) + "," + str(timestamp) + "," 
                            + str(date.day) + "," + str(date.hour) + "," + str(date.month) + "," 
                            + str(date.year) + "," +  "'" + str(season) + "'" + "," + str(date.weekday()) + ","  
                            + "'" + str(day_type) + "'" + ")")
                        session.execute(insert)
            except ValueError:
                continue
            id += 1
        print("Sending batch query")
        p.finish()

if __name__ == '__main__':
    main()



