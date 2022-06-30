# Road network framework
Framework that allows to:
- generate a graph instance in neo4j with both PRIMAL and DUAL representation of the road network from OSM data
- import amenities
- import traffic data
- perform routing based on hops, distance, or traffic
- close and open streets to generate alternative graphs an obtain different routes
- perform analysis to investigate the most important junction and roads and the most congested areas of a city

For executing all the operations you will need a running instance of neo4j desktop.

## Requirements
 
In order to create the routing graph [Neo4j][1] must be installed on the local machine.

[1]: https://neo4j.com/docs/operations-manual/current/installation/

Install the Neo4j [Graph Data Science][2](at least version 1.6) and [APOC][3] libraries plug-ins in your instance.
Edit the settings of the graph DBMS created in your instance.
Add this row of code after the decorator 'Other Neo4j system properties':

apoc.import.file.enabled=true

You will need to upgrade your DBMS to version 4.2.6.

[2]: https://neo4j.com/docs/graph-data-science/current/installation/

[3]: https://neo4j.com/labs/apoc/4.1/installation/

Moreover, you will need to install [OSMnx][4] and [overpy][5] library for python, yuo can run pip install -r requirements.txt.

[4]: https://osmnx.readthedocs.io/en/stable/
[5]: https://anaconda.org/conda-forge/overpy

***

## creation of  Juntion Graph (PRIMAL approach)
### import road network
Example of how to import nodes of the road network from OSM:

````shell command
python crateJunctionGraph.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -f modena.graphml
````
In this case Modena road network have been imported. The parameters passed represent:

- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _f_ name of the file where to save the graph with extention '.graphml' (this file will be created by the script and automatically placed in the import folder of neo4j)

### import point of interest

In order to perform routing queries between two points of interest (POI) they must be imported.

Example of how to import POI from OSM for the city of Modena:
````shell
python amenity.py -n neo4j://localhost:7687 -u neo4j -p passwd -x 44.622424 -y 10.884421 -d 5000
````
The parameters passed:
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _x_ and _y_ minimum value of latitude and longitude of the bbox that cover the geographic area from which to search the points of interest.
- _d_ distance in meter from the central point (radius of the area of interest)
***
## Creation of Road Section Graph (DUAL approach)

Example of how to import nodes of the road network from OSM:

````shell command
python crateRoadSectionGraph.py -n neo4j://localhost:7687 -u neo4j -p passwd
````
The parameters passed:
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance

## obtaining some general information about the graphs

````shell command
python graphAnalysis.py -n neo4j://localhost:7687 -u neo4j -p passwd
````
The parameters passed:
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance

## Import traffic

Information about traffic volumes can be included in the graph from a csv file formatted as the 'traffic.csv' file includeed in the folder.

Example of how to import traffic from the file traffic.csv for the city of Modena:
````shell
python traffic.py -n neo4j://localhost:7687 -u neo4j -p passwd -f C:\Users\user\Desktop\TESI\cavaletti\Updated\traffic.csv
````

- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _f_ name of the csv file where traffic information between nodes are provided
 
## Application of graph algorithms to investigate the most important roads or junctions

4 possible analysis, 2 performed on the Junction graph, 1 performed only on the Road Section graph and one performed on both.
The user will be questioned about the analysis he wants to perform on the basis of the aspected results.
All the results are saved in local csv files and a visualization is provided in the web browser.

````shell
python algorithmAppliedToJunctionsAndRoads.py -n neo4j://localhost:7687 -u neo4j -p  ****** -x 44.645885 -y 10.9255707 -f new.csv
````
- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _f_ name of the csv file where to save the results. The name is used as a prefix and some 
suffixes are added to distinguish between the results of the different analysis

## Routing
Routing between two points can be performed by running the following script. A map with the calculated route highlighted is generated.

An example of how to use the script routing.py:

```` shell
python routing.py -s 'La Baracchina' -d 'Michelangelo' -n neo4j://localhost:7687 -u neo4j -p passwd -x 44.645885 -y 10.9255707
````
The parameters passed:

- _s_ name of the source point of interest
- _d_ name of the destination point of interest
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _x_ and _y_ latitude and longitude of the central point of the generated map

The program asks you to enter the modality for routing choosing between: **distance** (d), **hops** (h) or **traffic volume** (t).
The routing based on distance will select the shortest path considering the distance. The routing based on hops will choose the path with the minimum number of hops.
The routing based on traffic volume will selected the path whose edge in hte graph have a lower value of traffic volume.

In order to perform the calculation mode base on the traffic volume, information about traffic volume in each edge must be imported.

## Change the street status: open and close streets
The user can also decide to close a street or to open it. This can be helpfult to simulate different routing scenarios.
An example of how to use the script routing.py:

```` shell
python changeStreetStatus.py -s "Via Wiligelmo" -st "close" -n neo4j://localhost:7687 -u neo4j -p passwd
````
The parameters passed:

- _s_ name of the street
- _st_ "close" for closing "open" for opening
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance

## predefined tests
In the tests folder there is a pre-composed file where the functions of the framework can be tested. The required attributes are in order:
1 = latitude of the central point of the generated map
2 = longitude of the central point of the generated map
3 = name of the destination point of interest
4 = address of the local Neo4j instance
5 = user of the local Neo4j instance
6 = password of the local Neo4j instance
7 = name of the file where to save the graph with extention '.graphml' (this file will be created by the script and automatically placed in the import folder of neo4j)
8 = name of the csv file where traffic information between nodes are provided
9 = name of the csv file where to save the results. The name is used as a prefix and some 
suffixes are added to distinguish between the results of the different analysis
