from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time
import folium
from shapely import wkt
import pandas as pd
import geopandas as gpd

"""In this file we are going to show how to perform routing on the layers' general graphs """


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

    
    def routing_algorithm_based_on_cost(self, lat, lon, dest):
        """Perform routing using dijkstra by considering as weight the cost attribute, which represents a tradeoff
           between travel time and safety of the path
        """
        with self.driver.session() as session:
            print(dest)
            result = session.write_transaction(self._routing_algorithm_based_on_cost, lat, lon, dest)
            return result

    
    @staticmethod
    def _routing_algorithm_based_on_cost(tx, lat, lon, dest):
        result = tx.run("""
                MATCH(t:Tag)<-[:TAGS]-(poi:PointOfInterest)-[:MEMBER]->(osm:OSMWayNode) 
                where t.name =  $dest 
                with osm CALL spatial.withinDistance('spatial', osm.location, 0.01) yield node unwind(node) as n 
                match(bl:BicycleLane) where b.id_num = n.id_num and n:BicycleLane with collect(bl)[0] as target 
                with target call spatial.withinDistance('spatial', point({latitude:$lat, longitude:$lon}), 0.01) 
                yield node unwind(node) as n match(bl:BicycleLane) where bl.id = n.id and n:BicycleLane with collect(bl)[0] as source, target 
                CALL gds.shortestPath.dijkstra.stream('routes_generic', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'cost'
                })
                YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                RETURN index,
                    gds.util.asNode(sourceNode).id_num AS sourceNodeID,
                    gds.util.asNode(targetNode).id_num AS targetNodeID,
                    totalCost,
                    [nodeId IN nodeIds | gds.util.asNode(nodeId).id_num] AS nodeIDs,
                    costs,
                    nodes(path) as nodespath
                """, lat=lat, lon=lon, dest=dest)

        return result.values()


def read_file(path):
    """Read the file specified by the path"""
    f = open(path)
    fjson = json.load(f)
    df = pd.DataFrame(fjson['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def creation_map(result_routing_cost, gdf_cycleways, gdf_footways, path, lat, lon):
    """Draw a map which displays the path obtained from the routing process"""
    l_cycleways = []
    l_footways = []

    for el in result_routing_cost[0][4]:
        if 'cycleway/' in el:
            l_cycleways.append(int(el.replace('cycleway/', '')))
        elif 'foot/' in el:
            l_footways.append(int(el.replace('foot/', '')))
    
    
    m3 = folium.Map([lat, lon], zoom_start=15)


    folium.GeoJson(data=gdf_cycleways[gdf_cycleways['id_num'].isin(l_cycleways)],
                           style_function=lambda x: {'fillColor': 'orange'}).add_to(m3)

    folium.GeoJson(data=gdf_footways[gdf_footways['id_num'].isin(l_footways)], 
                           style_function=lambda x: {'color': 'green', 'weight':'3'}).add_to(m3)
                            
        
    m3.save(path + "secondary_routing.html")



    




def add_options():
    """Parameters nedeed to run the script"""
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

    """Extract cycleways and footways data from their json files"""
    gdf_cycleways = read_file(path + options.file_name_cycleways)
    gdf_footways = read_file(path + options.file_name_footways)


    """Perform routing"""
    result_routing_cost = greeter.routing_algorithm_based_on_cost(options.lat, options.lon, options.dest)
    print("Find the best path between your source location and the target location, considering the travel time needed and the level of security of the cycleways used : done")

    """Generation of the map displaying the obtained path"""
    creation_map(result_routing_cost, gdf_cycleways, gdf_footways, path, options.lat, options.lon)
    print("Creation of the map with the two paths drawn on it : done ")




    return 0


main()