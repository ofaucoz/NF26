#!/usr/local/bin/ipython
import csv
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e34')

# This script will create a table and insert into it the dataset of the top 5000 movies from Imdb (Kaggle)

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

def create():
    f = open('movie_metadata.csv', 'r')
    reader = csv.reader(f, delimiter=',')
    typeHeaders = []
    headers = next(reader)
    firstLine = next(reader)
    create = "CREATE TABLE movies ("
    for i in range (0, len(headers)):
        create += headers[i]
        if(RepresentsInt(firstLine[i])):
            create +=  " bigInt"
            typeHeaders.append("int")
        elif(RepresentsFloat(firstLine[i])):
            create += " decimal"
            typeHeaders.append("decimal")
        else:
            create += " text"
            typeHeaders.append("text")
        create += ","
    create += "primary key (movie_title));"
    session.execute(create)
    return typeHeaders

def insert(typeHeaders):
    f = open('movie_metadata.csv', 'r')
    reader = csv.reader(f, delimiter=',', quotechar='"')
    headers = next(reader)
    for row in reader:
            insert = "INSERT INTO MOVIES ("
            values = "VALUES ("
            insert += ','.join(headers)
            insert += ")"
            for i in range (0, len(headers)):
                    #if the value is null, then put a null else check type and insert according to it
                    if(not row[i] == ""):
                        if(typeHeaders[i] == "text"):
                            values += "'" + row[i].replace("'","") + "'"
                        if((typeHeaders[i] == "int") or (typeHeaders[i] == "decimal")):
                            values += row[i]
                    else:
                        values += "null"
                    values += ","
            # remove last comma 
            values = values[:-1]
            values += ")"
            print(insert + " " + values + "\r\n")		
            session.execute(insert + " " + values)		

def main():
    typeHeaders = create()
    insert(typeHeaders)

if __name__ == '__main__':
    main()


