#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')
taxi_dataset = open('/train.csv', 'r')
reader = csv.reader(f, delimiter=',', quotechar='"')
typeHeaders = ["text", "text", "bigInt", "bigInt", "bigInt", "bigInt", "text", "boolean", "text"]

# This script will create a table and insert into it

def get_headers():
    return next(reader) 

# Dimension Appel
def create_dimension_call():
    create = "CREATE TABLE CALL (id bigInt,  call_type text, origin_call bigInt);"

# Dimension Time
def create_dimension_time():
	#TODO
    create = "CREATE TABLE TIME"
    

def main():
	#TODO
if __name__ == '__main__':
	#TODO
    main()


