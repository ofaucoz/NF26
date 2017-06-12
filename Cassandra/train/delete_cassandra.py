#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34_taxi')

def main():    
    session.execute("drop table by_call_type ;drop table by_day_of_week ; drop table by_distance ;drop table by_end ; drop table by_hour ;drop table by_month; drop table by_origin_stand ;drop table by_pos_call_type ; drop table by_start ; drop table by_taxi ;
    ")
if __name__ == '__main__':
    main()



