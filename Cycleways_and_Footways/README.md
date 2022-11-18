# Cycleways and Footways networks framework
Framework that allows to:
- extract data regarding cycleways, footways, crossings and street nodes from OSM
- perform data preprocessing on available data of interest in order to determine relationships on them
- generation of the general graphs of cycleways and footways
- generation of the subgraphs of cycleways and footways
- connection of cycleways and footways layers between them 
- connection of cycleways and footways layers with the road network layer
- perform multi-modal routing on the multi-layered graph based on travel time and path safety

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

Moreover, you will need to install [OSMnx][4], [overpy][5], [pandas][6] and [geopandas][7] libraries for python, yuo can run pip install -r requirements.txt.

[4]: https://osmnx.readthedocs.io/en/stable/
[5]: https://anaconda.org/conda-forge/overpy
[6]: https://pandas.pydata.org/docs/
[7]: https://geopandas.org/en/stable/


***

## Extraction of footways and crossing data from OSM

Example of how to extract footways and crossings from OSM:

````shell command
python DataExtractionTotal.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -f streetNodesModena.graphml
````
In this case Modena footways, crossings and street nodes are extracted and stored in files contained in the import folder of the neo4j instance. Cycleways data were already provided in a geojson file. The parameters passed represent:

- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _f_ name of the file where to save the street nodes graph with extention '.graphml' (this file will be created by the script and automatically placed in the import folder of neo4j)

## Data preprocessing

Data preprocessing of the available data extracted from OSM is necessary in order to find relationships between them, like for example determine if two cycleways intersect.
Example of how perform total Data Preprocessing:

````shell command
python DataPreprocessingTotal.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -fc cycleways.json -fcn crossingnodes.json -fcw crossingways.json -ff footways.json
````
The parameters passed are:
- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _fc_ name of the file containing cycleways data
- _fcn_ name of the file containing crossings mapped as nodes data
- _fcw_ name of the file containing crossings mapped as ways data
- _ff_ name of the file containing footways data


## General graphs generation

Example of how to generate cycleways and footways general graphs:

````shell command
python GeneralGraphGeneration.py -n neo4j://localhost:7687 -u neo4j -p passwd -fc cycleways.json -fcn crossingnodes.json -fcw crossingways.json -ff footways.json -fnb neighborhoods.json
````

The script allows to generate general graphs in cycleways and footways layers and also the relationships between nodes of the same general graph and nodes of different general graphs.

The parameters passed are:
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _fc_ name of the file containing cycleways data
- _fcn_ name of the file containing crossings mapped as nodes data
- _fcw_ name of the file containing crossings mapped as ways data
- _ff_ name of the file containing footways data
- _fnb_ name of the file containing neighborhoods data


## Subgraphs generation

Example of how to generate cycleways and footways subgraphs:

````shell command
python SubgraphGeneration.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -fc cycleways.json -fcn crossingnodes.json -fcw crossingways.json -ff footways.json
````

The script allows to generate subgraphs in cycleways and footways layers and also the relationships between nodes of the same subgraph and nodes of different subgraphs.

The parameters passed are:
- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _fc_ name of the file containing cycleways data
- _fcn_ name of the file containing crossings mapped as nodes data
- _fcw_ name of the file containing crossings mapped as ways data
- _ff_ name of the file containing footways data


## Routing

Given the structure of the multi-layer graph, we can perform routing on both general graphs and subgraphs by setting some weights on all relationships. The weights we decided to set are:
- _travel_time_ the time required to travel a path or a section of it, and it is given by dividing the length of the path with the travel speed, which can be different according to the transport mode used (in this case bicycle or foot)
- _cost_ a tradeoff measure between the travel time and the safety of the path, which is an important indicator in deciding which path to take 

After setting weights, we can proceed by applying pathfinding algorithms on graphs in order to find the fastest path and the one which considers also the safety.
The algorithms we used are Dijkstra and A*. The results are then displayed on a map.

### General graphs Routing
The routing performed on general graphs gives back general results, since the nodes represents paths in their entirety.
Example of how to perform routing on general graphs:

````shell command
python GeneralRoutingTotal.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -fcl cycleways.json -ff footways.json
````

The parameters passed are:
- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _p_ password of the local Neo4j instance
- _fcl_ name of the file containing cycleways data
- _ff_ name of the file containing footways data

### Subgraphs Routing
The routing performed on subgraphs gives back more specific and accurate results, since the nodes and relationships represent portions of paths. In this script can also be decided the transport mode, so the user can express his choice on moving by bicycle or by foot. 
Example of how to perform routing on general graphs:

````shell command
python SubgraphRoutingTotal.py -x 44.645885 -y 10.9255707 -d 5000 -n neo4j://localhost:7687 -u neo4j -p passwd -m mode -fcl cycleways.json -ff footways.json
````

The parameters passed are:
- _x_ latitude of the central point of the area of interest
- _y_ longitude of the central point of the area of interest
- _d_ distance in meter from the central point (radius of the area of interest)
- _n_ address of the local Neo4j instance 
- _u_ user of the local Neo4j instance
- _m_ transport mode ("cycleways" or "footways")
- _p_ password of the local Neo4j instance
- _fcl_ name of the file containing cycleways data
- _ff_ name of the file containing footways data




