from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time
import folium
import osmnx as ox
from shapely import wkt
import pandas as pd
import geopandas as gpd


"""In this file we perform routing on projections using A*"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()
        
    def get_import_folder_name(self):
        """gets the path of the import folder of the neo4j instance"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    
    
    def routing_algorithm_based_on_cost(self, lat, lon, dest, projection):
        """Routing considering as weight the cost, which is a tradeoff between the travel time and
           the safety of the path
        """
        with self.driver.session() as session:
            print(dest)
            result = session.write_transaction(self._routing_algorithm_based_on_cost, lat, lon, dest, projection)
            return result

    
    @staticmethod
    def _routing_algorithm_based_on_cost(tx, lat, lon, dest, projection):
        result = tx.run("""
                MATCH(t:Tag)<-[:TAGS]-(poi:PointOfInterest)-[:MEMBER]->(osm:OSMWayNode) 
                where t.name =  $dest 
                with osm CALL spatial.withinDistance('spatial', osm.location, 0.01) yield node unwind(node) as n 
                match(j:Junction) where j.id = n.id and n:Junction  with collect(j)[0] as target 
                with target call spatial.withinDistance('spatial', point({latitude:$lat, longitude:$lon}), 0.01) 
                yield node unwind(node) as n match(j:Junction) where j.id = n.id and n:Junction with collect(j)[0] as source, target 
                CALL gds.shortestPath.astar.stream($projection, {
                sourceNode: source,
                targetNode: target,
                latitudeProperty: 'lat',
                longitudeProperty: 'lon',
                relationshipWeightProperty: 'cost'
                })
                YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                with index,
                    gds.util.asNode(sourceNode).id AS sourceNodeID,
                    gds.util.asNode(targetNode).id AS targetNodeID,
                    totalCost,
                    [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs,
                    costs,
                    nodes(path) as nodespath
                match(n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost 
                return path, nodeIDs, totalCost
                """, lat=lat, lon=lon, dest=dest, projection=projection)

        return result.values()


    def routing_algorithm_based_on_travel_time(self, lat, lon, dest, projection):
        """Routing considering as weight just the travel time"""
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_algorithm_based_on_travel_time, lat, lon, dest, projection)
            return result

    
    @staticmethod
    def _routing_algorithm_based_on_travel_time(tx, lat, lon, dest, projection):
        result = tx.run("""
                MATCH(t:Tag)<-[:TAGS]-(poi:PointOfInterest)-[:MEMBER]->(osm:OSMWayNode) 
                where t.name =  $dest 
                with osm CALL spatial.withinDistance('spatial', osm.location, 0.01) yield node unwind(node) as n 
                match(j:Junction) where j.id = n.id and n:Junction  with collect(j)[0] as target 
                with target call spatial.withinDistance('spatial', point({latitude:$lat, longitude:$lon}), 0.01) 
                yield node unwind(node) as n match(j:Junction) where j.id = n.id and n:Junction with collect(j)[0] as source, target 
                CALL gds.shortestPath.astar.stream($projection, {
                sourceNode: source,
                targetNode: target,
                latitudeProperty: 'lat',
                longitudeProperty: 'lon',
                relationshipWeightProperty: 'travel_time'
                })
                YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                with index,
                    gds.util.asNode(sourceNode).id AS sourceNodeID,
                    gds.util.asNode(targetNode).id AS targetNodeID,
                    totalCost,
                    [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs,
                    costs,
                    nodes(path) as nodespath
                match(n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost 
                return path, nodeIDs, totalCost
                """, lat=lat, lon=lon, dest=dest, projection=projection)

        return result.values()


def read_file(path):
    """Read the file at the specified path"""
    f = open(path)
    fjson = json.load(f)
    df = pd.DataFrame(fjson['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def replace_ids(l):
    """Replace the subgraph's nodes id"""
    for el in l:
        if 'roadbike/' in el:
            l[l.index(el)] = int(el.replace('roadbike/', ''))
        elif 'junctionbike/' in el:
            l[l.index(el)] = int(el.replace('junctionbike/', ''))
        elif 'junctionfoot/' in el:
            l[l.index(el)] = int(el.replace('junctionfoot/', ''))    
        elif 'bike/' in el:
            l[l.index(el)] = int(el.replace('bike/', ''))
        elif 'roadfoot/' in el:
            l[l.index(el)] = int(el.replace('roadfoot/', ''))
        elif 'foot/' in el:
            l[l.index(el)] = int(el.replace('foot/', ''))
        else:
            l[l.index(el)] = int(el)

    return l
    



def creation_map(result_routing_cost, result_routing_travel, nodes, gdf_cycleways, gdf_footways, path, lat, lon):
    """Draw the map with the obtained paths"""
    l_cost = result_routing_cost[0][1]
    l_travel = result_routing_travel[0][1]

    l_cost = replace_ids(l_cost)
    l_travel = replace_ids(l_travel)
    


    nodes_path_cost = nodes[nodes['osmid'].isin(l_cost)]
    nodes_path_travel = nodes[nodes['osmid'].isin(l_travel)]

    coordinates_cost = []
    coordinates_travel = []

    for idx in l_cost:
        row = nodes[nodes['osmid'] == int(idx)].iloc[0]
        coordinates_cost.append((row.y, row.x))
    
    for idx in l_travel:
        row = nodes[nodes['osmid'] == int(idx)].iloc[0]
        coordinates_travel.append((row.y, row.x))


    
    m3 = folium.Map([lat, lon], zoom_start=15)


    
    geo_j = folium.GeoJson(data=gdf_cycleways,
                           style_function=lambda x: {'fillColor': 'orange'})

    folium.GeoJson(data=gdf_footways, 
                           style_function=lambda x: {'color': 'purple', 'weight':'3'}).add_to(geo_j)

    geo_j.add_to(m3)
    

    locs = zip(nodes_path_cost['osmid'], zip(nodes_path_cost.geometry.y, nodes_path_cost.geometry.x))
    for location in locs:
        folium.Circle(location=location[1], popup=location[0], radius=2).add_to(m3)
        
    locs = zip(nodes_path_travel['osmid'], zip(nodes_path_travel.geometry.y, nodes_path_travel.geometry.x))
    for location in locs:
        folium.Circle(location=location[1], popup=location[0], radius=2).add_to(m3)


    folium.PolyLine(coordinates_cost,
                    color='red',
                    weight=3,
                    opacity=0.8).add_to(m3)

    folium.PolyLine(coordinates_travel,
                    color='blue',
                    weight=3,
                    opacity=0.8).add_to(m3)


                            
        
    m3.save(path + "routing_AStar.html")





def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of your starting location""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of your starting location""",
                        required=True)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the name of your destination""",
                        required=True)
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--mode', '-m', dest='mode', type=str,
                        help="""Choose the modality of routing : cycleways or footways.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file.""",
                        required=True)
    parser.add_argument('--nameFilecycleways', '-fcl', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing the cycleways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    G = ox.io.load_graphml(path + options.file_name)
    nodes, edges = ox.graph_to_gdfs(G)
    nodes.reset_index(inplace=True)
    print("Loading grapml file : done")

    """The user can choose how to travel, using a bike or just walking"""
    graph_projection = ""
    if options.mode == 'cycleways':
        graph_projection = "bike_routes"
    else:
        graph_projection = "foot_routes"

    """Extract cycleways and footways data from their json files"""
    gdf_cycleways = read_file(path + options.file_name_cycleways)
    gdf_footways = read_file(path + options.file_name_footways)
    print("Loading cycleways and footways dataframes : done")

    """Routing considering as weight the cost"""
    result_routing_cost = greeter.routing_algorithm_based_on_cost(options.lat, options.lon, options.dest, graph_projection+"_cost")
    print("Find the best path between your source location and the target location, considering the travel time needed and the level of security of the cycleways used : done")


    """Routing considering as weight the travel time"""
    result_routing_travel_time = greeter.routing_algorithm_based_on_travel_time(options.lat, options.lon, options.dest, graph_projection+"_travel_time")
    print("Find the best path between your source location and the target location, considering only the travel time needed : done")


    """Generation of the map with the obtained paths displayed"""
    creation_map(result_routing_cost, result_routing_travel_time, nodes, gdf_cycleways, gdf_footways, path, options.lat, options.lon)
    print("Creation of the map with the two paths drawn on it : done ")

    return 0


if __name__ == "__main__":
    main()