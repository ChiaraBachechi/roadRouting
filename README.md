# roadRouting
Python script to perform routing queries between two nodes in a road graph on Neo4j

## Requirements
 
In order to create the routing graph [Neo4j][1] must be installed on the local machine.

[1]: https://neo4j.com/docs/operations-manual/current/installation/

Install the Neo4j [Graph Data Science][2] and [APOC][3] libraries plug-ins in your instance.
Edit the settings of the graph DBMS created in your instance.
Add this row of code after the decorator 'Other Neo4j system properties':

apoc.import.file.enabled=true

[2]: https://neo4j.com/docs/graph-data-science/current/installation/

[3]: https://neo4j.com/labs/apoc/4.1/installation/

Moreover, you will need to install [OSMnx][4] and [overpy][5] library for python.

[4]: https://osmnx.readthedocs.io/en/stable/
[5]: https://anaconda.org/conda-forge/overpy

***

## creation of graph
### import road network
Example of how to import nodes of the road network from OSM:

````shell command
python graph.py -x 44.645885 -y 10.9255707 -d 9000 -n neo4j://localhost:7687 -u neo4j -p userPassword -i .\neo4j\import -f modena.graphml
````
In this case Modena road network have been imported. The parameters passed represent:

- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _i_ path of the Neo4j import directory
- _f_ name of the file where to save the graph with extention '.graphml'

### import point of interest

In order to perform routing queries between two points of interest (POI) they must be imported.

Example of how to import POI from OSM for the city of Modena:
````shell
python amenity.py -n neo4j://localhost:7687 -u neo4j -p userPassword -i .\neo4j\import -x 44.622424 -y 10.884421 -z 44.667922 -k 10.964375
````
The parameters passed:
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _i_ path of the Neo4j import directory
- _x_ and _y_ minimum value of latitude and longitude of the bbox that cover the geographic area from which to search the points of interest.
- _z_ and _k_ maximum value of latitude and longitude of the bbox that cover the geographic area from which to search the points of interest.
***
## Routing
Routing between two points can be performed by running the following script. A map with the calculated route highlighted is generated.

An example of how to use the script routing.py:

```` shell
python routing.py -s sourceName -d destinationName -n neo4j://localhost:7687 -u neo4j -p userPassword -x 44.645885 -y 10.9255707
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

Our solution works with the 'traffic.csv' file.

To import this type of data on Neo4j you can run this Cypher query:
````
LOAD CSV WITH HEADERS FROM 'file:///traffic.csv' AS row
WITH row
MATCH
  (a:Node)-[route:ROUTE]-(b:Node)
WHERE a.osm_id = row.node_start AND b.osm_id = row.node_end AND route.osmid = row.id_road_section
CREATE (a)-[r:AADT2019 {traffic_volume: tofloat(row.traffic_volume), year: row.year}]->(b)
