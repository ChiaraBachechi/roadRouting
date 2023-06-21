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

    def create_projections(self,mode,weight):
        """Create projections, one considering as weight the travel time and one the cost"""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_projections,mode,weight)
            return result

    @staticmethod
    def _create_projections(tx,mode,weight):
        if(mode == 'cycleways'):
            name = 'bike_routes'
            if(weight == 'cost' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('bike_routes_cost', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)
            if(weight == 'travel_time' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('bike_routes_travel_time', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)
        elif(mode == 'footways'):
            name = 'foot_routes'
            if(weight == 'cost' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('foot_routes_cost', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)
            if(weight == 'travel_time' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('foot_routes_travel_time', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)
        return name
    
    def delete_projected_graph(self,weight,graph_name):
        """This method deletes an existing graph projection. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph,weight,graph_name)

    @staticmethod
    def _drop_projected_graph(tx,weight,graph_name):
        if weight == 'both':
            name = graph_name + '_cost'
            tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
            name = graph_name + '_travel_time'
            result = tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
        else:
            name = graph_name + '_' + weight
            result = tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
        return result
    
    def update_cost(tx,beta=0.5):
        with self.driver.session() as session:
            path = session.read_transaction(self._update_cost,beta)
            
    @staticmethod
    def _update_cost(tx,beta):
        tx.run("""
                MATCH(n1)-[r:BIKE_ROUTE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time 
                MATCH(n3)-[r1:BIKE_ROUTE]-(n4) with max_travel_time, min_travel_time, r1 
                set r1.cost = $beta *(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + (1-$beta2) *(r1.danger-1)/(20-1)
                """,beta = beta, beta2 = beta)

        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time 
                MATCH(n3)-[r1:FOOT_ROUTE]-(n4) with max_travel_time, min_travel_time, r1 
                set r1.cost = $beta *(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + (1-$beta2) *(r1.danger-1)/19
                """,beta = beta, beta2 = beta)"
        return

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
        
    def get_the_nearest_junction_to_POI(self, osmid, foot = False):
        """Routing considering as weight the cost, which is a tradeoff between the travel time and
           the safety of the path
        """
        with self.driver.session() as session:
            print(dest)
            result = session.write_transaction(self._get_the_nearest_junction_to_POI, osmid, foot)
            return result

    @staticmethod
    def _get_the_nearest_junction_to_POI(tx, osmid, foot):
        if foot:
            result = tx.run("""match (p:PointOfInterest {osm_id: $osmid}) 
                                match (p)-[:MEMBER]->(o:OSMWayNode)
                                match (o)<-[:IS_NEAR_TO]-(m) with point({latitude: tofloat(o.lat), longitude: tofloat(o.lon)}) as location,m
                                match (m)-[:CONTAINS]->(j:Junction)
                                where j:FootCross or j:JunctionFootCross or j:RoadFootJunction
                                return distance(location,j.location) as distance, j.id
                                order by distance limit 1""",osmid = osmid)
        else:
            result = tx.run("""match (p:PointOfInterest {osm_id: $osmid}) 
                                match (p)-[:MEMBER]->(o:OSMWayNode)
                                match (o)<-[:IS_NEAR_TO]-(m) with point({latitude: tofloat(o.lat), longitude: tofloat(o.lon)}) as location,m
                                match (m)-[:CONTAINS]->(j:Junction)
                                return distance(location,j.location) as distance, j.id
                                order by distance limit 1""",osmid = osmid)
        return result.values()
        
    def get_the_nearest_junction_to_coordinates(self, lat, lon, foot = False):
        """Routing considering as weight the cost, which is a tradeoff between the travel time and
           the safety of the path
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._get_the_nearest_junction_to_coordinates, lat, lon, foot)
            return result

    @staticmethod
    def _get_the_nearest_junction_to_coordinates(tx, lat, lon, foot):
        if foot:
            print(lat)
            print(lon)
            query = """CALL spatial.withinDistance('spatial', {latitude:"""+str(lat) +""", longitude:"""+str(lon)+"""}, 0.1) 
            yield node,distance 
            where node:FootCross or node:JunctionFootCross or node:RoadFootJunction
            return distance,node.id as osmid order by distance limit 1"""
            result = tx.run(query)
        else:
            print(lat)
            print(lon)
            query = """CALL spatial.withinDistance('spatial', {latitude:"""+str(lat) +""", longitude:"""+str(lon)+"""}, 0.1) 
            yield node,distance 
            where node:Junction
            return distance,node.id as osmid order by distance limit 1"""
            result = tx.run(query)
        return result.values()


    def routing_algorithm(self, source, target, projection, mode,alg = 'd'):
        """Routing considering as weight just the travel time"""
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_algorithm, source, target, projection, mode,alg)
            return result

    
    @staticmethod
    def _routing_algorithm(tx, source, target, projection, mode, alg):
        if(alg =='d'):
            print('algorithm is:')
            print(alg)
            result = tx.run("""
                    match (source:Junction {id: $source})
                    match (target:Junction {id: $target})
                    CALL gds.shortestPath.dijkstra.stream($projection, {
                    sourceNode: source,
                    targetNode: target,
                    relationshipWeightProperty: $mode
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    with index,
                        totalCost,
                        [nodeId IN nodeIds | {latitude:gds.util.asNode(nodeId).lat,longitude:gds.util.asNode(nodeId).lon}] AS nodeCord,
                        [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs
                    match (n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost, nodeCord 
                    return path, nodeCord, totalCost, nodeIDs
                    """, source=source, target=target, projection=projection, mode=mode)
        else:
            print('algorithm is:-----------------------------------')
            print(alg)
            result = tx.run("""
                    match (source:Junction {id: $source})
                    match (target:Junction {id: $target})
                    CALL gds.shortestPath.astar.stream($projection, {
                    sourceNode: source,
                    targetNode: target,
                    latitudeProperty: 'lat',
                    longitudeProperty: 'lon',
                    relationshipWeightProperty: $mode
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    with index,
                        totalCost,
                        [nodeId IN nodeIds | {latitude:gds.util.asNode(nodeId).lat,longitude:gds.util.asNode(nodeId).lon}] AS nodeCord,
                        [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs
                    match(n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost, nodeCord 
                    return path, nodeCord, totalCost, nodeIDs
                    """, source=source, target=target, projection=projection, mode=mode)
        return result.values()
        
    def evaluate_path(self, start, end):
        """getting information about the trip"""
        with self.driver.session() as session:
            result = session.write_transaction(self._evaluate_path, start, end)
            return result
        
    @staticmethod
    def _evaluate_path(tx, start, end):
        result = tx.run("""match (n {id: $start})-[r]->(m {id: $end}) return r.length,r.danger,r.speed
        """,start = start, end = end)
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


def creation_map(result_routing_cost, mapName):
    """Draw the map with the obtained paths"""
    locations = result_routing_cost[0][1]

    coordinates_cost = []

    for x in locations:
        coordinates_cost.append((x['latitude'], x['longitude']))

    m3 = folium.Map([locations[0]['latitude'], locations[0]['longitude']], zoom_start=15)

    #geo_j = folium.GeoJson(data=gdf_cycleways,
                           #style_function=lambda x: {'fillColor': 'orange'})

    #folium.GeoJson(data=gdf_footways,
                   #style_function=lambda x: {'color': 'purple', 'weight': '3'}).add_to(geo_j)

    #geo_j.add_to(m3)

    folium.PolyLine(coordinates_cost,
                    color='red',
                    weight=5,
                    opacity=0.8).add_to(m3)

    m3.save( mapName)

def creation_map_total(result_routing_cost, result_routing_travel, mapName):
    """Draw the map with the obtained paths"""
    locations1 = result_routing_cost[0][1]
    locations2 = result_routing_travel[0][1]

    coordinates_cost = []

    for x in locations1:
        coordinates_cost.append((x['latitude'], x['longitude']))

    coordinates_travel = []

    for x in locations2:
        coordinates_travel.append((x['latitude'], x['longitude']))

    m3 = folium.Map([locations1[0]['latitude'], locations1[0]['longitude']], zoom_start=15)

    #geo_j = folium.GeoJson(data=gdf_cycleways,
                           #style_function=lambda x: {'fillColor': 'orange'})

    #folium.GeoJson(data=gdf_footways,
                   #style_function=lambda x: {'color': 'purple', 'weight': '3'}).add_to(geo_j)

    #geo_j.add_to(m3)

    folium.PolyLine(coordinates_cost,
                    color='green',
                    weight=5,
                    opacity=0.8).add_to(m3)

    folium.PolyLine(coordinates_travel,
                    color='blue',
                    weight=5,
                    opacity=0.8).add_to(m3)

    m3.save(mapName)

def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of your starting location""",
                        required=False)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of your starting location""",
                        required=False)
    parser.add_argument('--latitude_dest', '-x_dest', dest='lat_dest', type=float,
                        help="""Insert latitude of your destination location""",
                        required=False)
    parser.add_argument('--longitude_dest', '-y_dest', dest='lon_dest', type=float,
                        help="""Insert longitude of your destination location""",
                        required=False)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the osm identifier of your destination""",
                        required=False)
    parser.add_argument('--source', '-s', dest='source', type=str,
                        help="""Insert the osm identifier of your source""",
                        required=False)
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
    parser.add_argument('--alg', '-a', dest='alg', type=str,
                        help="""Choose the modality of routing : astar (a) or dijkstra (d).""",
                        required=False, default = 'd')
    parser.add_argument('--weight', '-w', dest='weight', type=str,help="""Insert the weight to use in order to perform the routing : travel_time, cost or both.""",
                        required=True)
    parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
                        help="""Insert the name of the file containing the map with the computed path.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    #G = ox.io.load_graphml(path + options.file_name)
    #nodes, edges = ox.graph_to_gdfs(G)
    #nodes.reset_index(inplace=True)
    #print("Loading grapml file : done")
    
    """The user can choose how to travel, using a bike or just walking"""
    graph_projection = ""
    foot = False
    if options.mode == 'cycleways':
        graph_projection = "bike_routes"
    else:
        graph_projection = "foot_routes"
        foot = True
    

    print("Loading cycleways and footways dataframes : done")
    if options.lat == '':
        if options.source != '':
            result = greeter.get_the_nearest_junction_to_POI(options.source,foot)
            print(result)
            distance_source = result[0][0]
            source_osmid = result[0][1]
        else:
            print("SOURCE INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    else:
        if options.lon != '':
            result = greeter.get_the_nearest_junction_to_coordinates(options.lat, options.lon, foot)
            print(result)
            distance_source = result[0][0]
            source_osmid = result[0][1]
        else:
            print("SOURCE INFORMATION ARE REQUIRED")
            greeter.delete_projected_graph()
            raise RuntimeError("Wrong parameter value")
    
    if options.lat_dest == '':
        if options.dest != '':
            result = greeter.get_the_nearest_junction_to_POI(options.dest,foot)
            print(result)
            distance_dest = result[0][0]
            dest_osmid = result[0][1]
        else:
            print("DESTINATION INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    else:
        if options.lon_dest != '':
            result = greeter.get_the_nearest_junction_to_coordinates(options.lat_dest, options.lon_dest, foot)
            print(result)
            distance_dest = result[0][0]
            dest_osmid = result[0][1]
        else:
            print("DESTINATION INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    print(distance_source,source_osmid,distance_dest,dest_osmid)
    #create graph projections
    graph_name = greeter.create_projections(options.mode, options.weight)
    if options.weight == "cost" or options.weight == "travel_time":
        """Routing considering as weight the cost"""
        result_routing_cost = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection+"_"+options.weight,options.weight, options.alg )
        print("Find the best path between your source location and the target location, considering the travel time needed and the level of security of the paths used : done")
        print(result_routing_cost[0][1])
        listNodes = result_routing_cost[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        """Generation of the map with the obtained paths displayed"""
        creation_map(result_routing_cost, options.mapName)
        print("Creation of the map with the two paths drawn on it : done ")
        print('cost:')
        print(result_routing_cost[0][2])
        print('number of hops:')
        print(len(result_routing_cost[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)

    elif options.weight == "both":
        """Routing considering as weight the cost"""
        result_routing_cost = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection + "_cost", "cost", options.alg )
        print(
            """Find the best path between your source location and the target location,
            considering the travel time needed and the level of security of the paths used : done""")
        listNodes = result_routing_cost[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        print('cost:')
        print(result_routing_cost[0][2])
        print('number of hops:')
        print(len(result_routing_cost[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)
        """Routing considering as weight the travel time"""
        result_routing_travel_time = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection + "_travel_time", "travel_time", options.alg )
        print(
            "Find the best path between your source location and the target location, considering only the travel time needed : done")
        listNodes = result_routing_travel_time[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        print('cost:')
        print(result_routing_travel_time[0][2])
        print('number of hops:')
        print(len(result_routing_travel_time[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)
        """Generation of the map with the obtained paths displayed"""
        creation_map_total(result_routing_cost, result_routing_travel_time, options.mapName)
        print("Creation of the map with the two paths drawn on it : done ")

    else:
        raise RuntimeError("Wrong parameter value")
    greeter.delete_projected_graph(options.weight, graph_name)
    return 0


if __name__ == "__main__":
    main()