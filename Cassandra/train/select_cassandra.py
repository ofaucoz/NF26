#!/usr/local/bin/ipython
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import heapq
import math
import operator
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

def select_start_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select longitude_start, latitude_start, count(*) as value_occurence from by_start group by longitude_start, latitude_start;')
    for row in rows:
        list_y.append(row.value_occurence)
        list_x.append(str(row.longitude_start) + "," + str(row.latitude_start))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Localisation les plus visitées')
    plt.xlabel('Occurences sur 170000 trajets')
    plt.ylabel('Positition')   
    plt.savefig("barplot_start.png")

def select_start_avg():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select longitude_start, latitude_start, avg(distance) as distance_moy from by_start group by longitude_start, latitude_start;')
    for row in rows:
        list_y.append(row.distance_moy)
        list_x.append(str(row.longitude_start) + "," + str(row.latitude_start))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y)) 
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par localisation')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Positition')   
    plt.savefig("barplot_start_distance.png")

def select_call_type_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select call_type, count(*) as occurences from by_call_type group by call_type;')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(call_type)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Nombre de courses par call_type pour 170000 occurences')
    plt.xlabel('Nombre de courses')
    plt.ylabel('Call_type')   
    plt.savefig("barplot_call_type_count.png")

def select_call_type_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select call_type, avg(distance) as distance from by_call_type group by call_type')
   for row in rows:
        list_y.append(row.distance)
        list_x.append(call_type)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenn par call_type')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Call_type')   
    plt.savefig("barplot_call_type_distance.png")

def select_dayofweek_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select dayofweek, count(*) as occurences from by_day_of_week group by dayofweek')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(row.dayofweek)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Count par jour de la semaine')
    plt.xlabel('Count')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_dayofweek_count.png")

def select_dayofweek_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select day_of_week, avg(distance) as distance from by_day_of_week group by day_of_week')
    for row in rows:
        list_y.append(row.distance)
        list_x.append(row.day_of_week)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par jour de la semaine')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_dayofweek_distance.png")

def select_year_sum_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select year, month, sum(distance) as total_distance from by_distance group by year, month;')
    for row in rows:
        list_y.append(row.total_distance)
        list_x.append(row.year)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance totale par années')
    plt.xlabel('Distance')
    plt.ylabel('Année')   
    plt.savefig("barplot_year_tot_distance.png")

def select_year_month_sum_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select year, month, sum(distance) as total_distance from by_distance group by year, month;')
    for row in rows:
        list_y.append(row.total_distance)
        list_x.append(str(row.year) + "-" + str(row.month))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par jour de la semaine')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_year_month_tot_distance.png")

def select_end_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select longitude_end, latitude_end, count(*) as value_occurence from by_end group by longitude_end, latitude_end; ')
    for row in rows:
        list_y.append(row.value_occurence)
        list_x.append(str(row.longitude_end) + "," + str(row.latitude_end))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Nombre de courses par localisation d\'arrivée')
    plt.xlabel('Courses')
    plt.ylabel('Localisation')   
    plt.savefig("barplot_end_courses.png")

def select_end_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select longitude_end, latitude_end, avg(distance) as distance_moy from by_end group by longitude_end, latitude_end;')
    for row in rows:
        list_y.append(row.distance_moy)
        list_x.append(str(row.longitude_end) + "," + str(row.latitude_end))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par localisation d\'arrivée')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Localisation')   
    plt.savefig("barplot_end_distance.png")

def select_hour_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select hour, year, month,  count(*) as occurences from by_hour group by year, month, hour')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(str(row.hour))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Heures d\'influence')
    plt.xlabel('Nombre de courses')
    plt.ylabel('Heures')   
    plt.savefig("barplot_hour_count.png")

def select_month_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select month, count(*) as occurences from by_month group by month')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(str(row.month))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Mois d\'influence')
    plt.xlabel('Nombre de courses')
    plt.ylabel('Mois')   
    plt.savefig("barplot_month_count.png")

def select_month_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select month, avg(distance) as distance from by_month group by month')
    for row in rows:
        list_y.append(row.distance)
        list_x.append(row.month)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Heures d\'influence')
    plt.xlabel('Nombre de courses')
    plt.ylabel('Heures')   
    plt.savefig("barplot_hour_count.png")

def select_origin_stand_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select origin_stand, count(*) as occurences from by_origin_stand group by origin_stand')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(row.origin_stand)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Meilleurs stand de taxi')
    plt.xlabel('Stands')
    plt.ylabel('Nombre de courses')   
    plt.savefig("barplot_stand_courses.png")

def select_origin_stand_year_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select origin_stand, year, month,  count(*) as occurences from by_origin_stand group by origin_stand, year, month')
    for row in rows:
        list_y.append(row.distance)
        list_x.append(str(row.origin_stand) + " - " + str(row.year))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Meilleurs stands de taxi par années')
    plt.xlabel('Stands - Année')
    plt.ylabel('Nombre de courses')   
    plt.savefig("barplot_stand_year_courses.png")

def select_taxi_count():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select taxi_id, count(*) as occurences from by_taxi group by taxi_id')
    for row in rows:
        list_y.append(row.occurences)
        list_x.append(str(row.taxi_id))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Meilleurs taxi')
    plt.xlabel('Taxi')
    plt.ylabel('Nombre de courses')   
    plt.savefig("barplot_taxi_courses.png")

def select_taxi_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select taxi_id, avg(distance) as distance from taxi_id group by taxi_id')
    for row in rows:
        list_y.append(row.distance)
        list_x.append(str(row.taxi_id))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))  # the x locations for the groups
    ax.barh(ind, list_y, width, color="blue")
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par taxi')
    plt.xlabel('Taxi')
    plt.ylabel('Distance moyenne')   
    plt.savefig("barplot_taxi_distance.png")

def euclideanDistance(ind_1, ind_2):
    distance += pow((ind_1.longitude_start - instance2[x].longitude_start), 2) + pow((ind_1.latitude_start - instance2[x].latitude_start), 2)
    return math.sqrt(distance)

def getNeighbors(trainingSet, testInstance, k):
    distances = []
    length = len(testInstance)-1
    for x in range(len(trainingSet)):
        dist = euclideanDistance(testInstance, trainingSet[x], length)
        distances.append((trainingSet[x], dist))
    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    for x in range(k):
        neighbors.append(distances[x][0])
    return neighbors

def getResponse(neighbors):
    classVotes = {}
    for x in range(len(neighbors)):
        call_type = neighbors[x].call_type
        if response in classVotes:
            classVotes[call_type] += 1
        else:
            classVotes[call_type] = 1
    sortedVotes = sorted(classVotes.iteritems(), key=operator.itemgetter(1), reverse=True)
    return sortedVotes[0][0]

def getAccuracy(testSet, predictions):
    correct = 0
    for x in range(len(testSet)):
        if testSet[x].call_type is predictions[x]:
            correct += 1
    return (correct/float(len(testSet))) * 100.0

def kppv_localisation_call_type():
    rows = session.execute('select longitude_start, latitude_start, call_type, count(*) as occurences from by_pos_call_type group by longitude_start, latitude_start, call_type;')
    split = 0.3
    training_set = list()
    test_set = list()
    estimated_class = list()
    for row in rows:
        if(random.random() > split):
            training_set.append(row)
        else:
            test_set.append(row)
    for testInstance in test_set:
        neighbors = getNeighbors(training_set, testInstance, 3)
        estimated_class.append(getResponse(neighbors))
    getAccuracy(test_set, estimated_class)
    

  

  

    math.hypot(p2[0] - p1[0], p2[1] - p1[1]) # Linear distance 



def main():    
   

if __name__ == '__main__':
    main()



