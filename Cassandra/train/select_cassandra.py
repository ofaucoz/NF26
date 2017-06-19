#!/usr/local/bin/ipython
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import heapq
import math
import operator
import random
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
cluster = Cluster()
session = cluster.connect('e34_taxi')
session.default_timeout = 60

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
    plt.xlabel('Occurences sur 934000 trajets')
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
        list_x.append(row.call_type)
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
    plt.title('Nombre de courses par call_type pour 934000 occurences')
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
        list_x.append(row.call_type)
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
    rows = session.execute('select dayofweek, avg(distance) as distance from by_day_of_week group by dayofweek')
    for row in rows:
        list_y.append(row.distance)
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
    plt.title('Distance moyenne par jour de la semaine')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_dayofweek_distance.png")

def select_year_sum_distance():
    list_x = list()
    list_y = list()
    label_x = list()
    rows = session.execute('select year, sum(distance) as total_distance from by_distance group by year;')
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
    rows = session.execute('select hour,  count(*) as occurences from by_hour group by hour')
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
    list_pos = list()
    written_file = open("origin_stand.txt", 'w')
    f = open('metaData_taxistandsID_name_GPSlocation.csv', 'r')
    stand = csv.reader(f)
    header = next(stand)
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
    for indice in ind:
        for row in stand:
            if row[0] == indice:
                list_pos.append(str(row[2]) + "," + str(row[3]))
    for item in list_pos:
        written_file.write("%s\n" % item)
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
        list_y.append(row.occurences)
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
    rows = session.execute('select taxi_id, avg(distance) as distance from by_taxi group by taxi_id')
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
    distance = 0
    distance += pow((ind_1.longitude_start - ind_2.longitude_start), 2) + pow((ind_1.latitude_start - ind_2.latitude_start), 2)
    return math.sqrt(distance)

def getNeighbors(trainingSet, testInstance, k):
    distances = []
    length = len(testInstance)-1
    for x in range(len(trainingSet)):
        dist = euclideanDistance(testInstance, trainingSet[x])
        distances.append((trainingSet[x], dist))
    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    for x in range(k):
        neighbors.append(distances[x][0])
    return neighbors

def getClass(neighbors):
    classVotes = {}
    for x in range(len(neighbors)):
        call_type = neighbors[x].call_type
        if call_type in classVotes:
            classVotes[call_type] += 1
        else:
            classVotes[call_type] = 1
    sortedVotes = sorted(classVotes.items(), key=operator.itemgetter(1), reverse=True)
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
        neighbors = getNeighbors(training_set, testInstance, 2)
        estimated_class.append(getClass(neighbors))
    print(getAccuracy(test_set, estimated_class))
 
def cluster_points(X, mu):
    clusters  = {}
    for x in X:
        bestmukey = min([(i[0], np.linalg.norm(np.array(x)-np.array(mu)[i[0]])) \
                    for i in enumerate(mu)], key=lambda t:t[1])[0]
        try:
            clusters[bestmukey].append(x)
        except KeyError:
            clusters[bestmukey] = [x]
    return clusters
 
def reevaluate_centers(mu, clusters):
    newmu = []
    keys = sorted(clusters.keys())
    for k in keys:
        newmu.append(np.mean(clusters[k], axis = 0))
    return newmu

def has_converged_plot(mu, oldmu):
    return (set([tuple(a) for a in mu]) == set([tuple(a) for a in oldmu]))

 
def find_centers(X, K):
    # Initialize to K random centers
    oldmu = random.sample(X, K)
    mu = random.sample(X, K)
    while not has_converged_plot(mu, oldmu):
        oldmu = mu
        # Assign all points in X to clusters
        clusters = cluster_points(X, mu)
        # Reevaluate centers
        mu = reevaluate_centers(oldmu, clusters)
    return(mu, clusters)

def kmeans_plot():
    color = ['red','green','blue']
    rows = session.execute('select longitude_start, latitude_start, call_type, count(*) as occurences from by_pos_call_type group by longitude_start, latitude_start, call_type;')
    list_point = []
    list_x = []
    list_y = []
    labels = []
    for row in rows:
        list_point.append([row.longitude_start, row.latitude_start])
        list_x.append(row.longitude_start)
        list_y.append(row.latitude_start)
    mu, clusters = find_centers(list_point, 2)
    for point in list_point:
        for i in range (1,len(clusters)):
            if(point in clusters[i]):
                labels.append(color[i])
    fig = plt.figure()    
    plt.axis([-7,-9,39,42.5])
    plt.scatter(list_x, list_y, c=labels)
    plt.savefig("kmeans.png")

def has_converged(mu, old_mu):
    res = True
    for i in range (0,len(mu)):
        if(not (mu[i]==old_mu[i]).all()):
            res = False
    return res

def random_centroid(num_kmeans, centroid, cluster, number_cluster, statement):
    M = 100
    for i in range (0,num_kmeans):
        M = 100
        for row in session.execute(statement):
            u = random.random()
            if(u<M):
                M = u
                try:
                    centroid[i] = np.array([float(row.longitude_start), float(row.latitude_start), float(row.longitude_end), float(row.latitude_end)])
                except IndexError:
                    centroid[i] = list()
                    centroid[i] = np.array([float(row.longitude_start), float(row.latitude_start), float(row.longitude_end), float(row.latitude_end)])
                try:
                    cluster[i] = np.array([float(row.longitude_start), float(row.latitude_start), float(row.longitude_end), float(row.latitude_end)])
                except IndexError:
                    cluster[i] = list()
                    cluster[i] = np.array([float(row.longitude_start), float(row.latitude_start), float(row.longitude_end), float(row.latitude_end)])
                number_cluster[i] = 1

def attribute_centroid(row, centroid):
    distance_centroid = dict()
    for i in range (1,len(centroid)):
        distance_centroid[i] = np.linalg.norm(row - centroid[i])
    return min(distance_centroid, key=distance_centroid.get)

def kmeans():
    query = "select longitude_start, latitude_start, longitude_end, latitude_end, count(*) as occurences from by_start_end group by longitude_start, latitude_start, longitude_end, latitude_end"
    statement = SimpleStatement(query, fetch_size=10)
    centroid = dict()
    old_centroid = dict()
    cluster = dict()
    number_cluster = dict()
    num_kmeans = 3    
    print("Randomizing old and new centroid")
    random_centroid(num_kmeans, centroid, cluster, number_cluster, statement)
    print(centroid)
    random_centroid(num_kmeans, old_centroid, {}, {}, statement)
    print(old_centroid)
    print("Computing centroid and waiting for convergence")
    while(not has_converged(old_centroid, centroid)):
        print("Not converged")
        for row in session.execute(statement):
            coord = np.array([float(row.longitude_start), float(row.latitude_start), float(row.longitude_end), float(row.latitude_end)])
            old_centroid = centroid
            j = attribute_centroid(coord, centroid)
            cluster[j] += coord
            number_cluster[j] += 1
            centroid[j] = cluster[j]/number_cluster[j]
        print(centroid)
        print(old_centroid)




def main():    
    # select_start_count()
    # select_start_avg()
    # select_call_type_count()
    # select_call_type_distance()
    # select_dayofweek_count()
    # select_dayofweek_distance()
    # select_end_count()
    # select_end_distance()
    # select_hour_count()
    # select_month_count()
    # select_month_distance()
    # select_origin_stand_count()
    # select_origin_stand_year_count()
    # select_taxi_count()
    # select_taxi_distance()
    kppv_localisation_call_type()
    # kmeans()

if __name__ == '__main__':
    main()



