# Cassandra - Train sur le dataset des taxis

## 1 - Création du MCD

### Header :
"TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID",
"TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"

### Attribute Information:

Each data sample corresponds to one completed trip. It contains a total of 9 (nine) features, described as follows:

#### TRIP_ID:
(String) It contains a unique identifier for each trip;

#### CALL_TYPE:
(char) It identifies the way used to demand this service. It may contain one of three possible values:
- 'A' if this trip was dispatched from the central;
- 'B' if this trip was demanded directly to a taxi driver at a specific stand;
- 'C' otherwise (i.e. a trip demanded on a random street).

#### ORIGIN_CALL:
(integer) It contains a unique identifier for each phone number which was used to demand, at least, one service. It identifies the trip's customer if CALL_TYPE='A'. Otherwise, it assumes a NULL value;

#### ORIGIN_STAND: 
(integer): It contains a unique identifier for the taxi stand. It identifies the starting point of the trip if CALL_TYPE='B'. Otherwise, it assumes a NULL value;

#### TAXI_ID: 
(integer): It contains a unique identifier for the taxi driver that performed each trip;

#### TIMESTAMP: 
(integer) Unix Timestamp (in seconds). It identifies the trip's start;

#### DAYTYPE: 
(char) It identifies the daytype of the trip's start. It assumes one of three possible values:
- 'B' if this trip started on a holiday or any other special day (i.e. extending holidays, floating holidays, etc.);
- 'C' if the trip started on a day before a type-B day;
- 'A' otherwise (i.e. a normal day, workday or weekend).

#### IMPORTANT NOTICE: 
This field has not been correctly calculated. Please see the following links as reliable sources for official holidays in Portugal.

[Public Holidays Portugal](http://holidays.retira.eu/public-holidays/portugal/2017/)

#### MISSING_DATA: 
(Boolean) It is FALSE when the GPS data stream is complete and TRUE whenever one (or more) locations are missing;

#### POLYLINE: 
(String): It contains a list of GPS coordinates (i.e. WGS84 format) mapped as a string. The beginning and the end of the string are identified with brackets (i.e. [ and ], respectively). Each pair of coordinates is also identified by the same brackets as [LONGITUDE, LATITUDE]. This list contains one pair of coordinates for each 15 seconds of trip. The last list item corresponds to the trip's destination while the first one represents its start.

### Faits:

* Longueur du trajet par localisation?
* Call type par localisation?
* Nombre de courses par téléphone?
* Nombre de courses par stand?
* Ratio stand/call?
* Nombre de courses / taxi?
* Localistation / taxi?
* Heure d'influence la plus/moins forte?
* Ratio par rapport au day type?
* Passage le plus fréquent pendant les courses?
* Distance parcourue (calculée à partir des localisations entre les points)

### Dimensions :

* Localisation : localisation(longitude, latitude)?
* Appel : Call type, ORIGIN_CALL, ORIGIN_STAND
* Taxi : ID
* Time : Timestamp,year,month,day,hours, DayType 


## 2 - Importation des données

## 3 - Reporting

