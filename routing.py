from neo4j import GraphDatabase
import folium as fo
import argparse
import pandas as pd


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_projected_graph(self):
        """This method creates a new graph as a projection of the existing nodes and relations. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._projected_graph)

    @staticmethod
    def _projected_graph(tx):
        result = tx.run("""
                CALL gds.graph.create.cypher(
                    "graph",
                    "MATCH (n:RoadJunction) RETURN id(n) AS id, n.lat AS lat, n.lon AS lon",
                    "MATCH (:RoadJunction)-[r:ROUTE]->(:RoadJunction) with min(r.AADT) as min_AADT,max(r.AADT) as max_AADT,
                    max(r.distance) as max_dist,min(r.distance) as min_dist
                    MATCH (n:RoadJunction)-[r:ROUTE]->(m:RoadJunction) WHERE r.status = 'active' 
                    RETURN id(n) AS source, id(m) AS target, 
                    0.5 * toFloat((r.AADT-min_AADT)/(max_AADT-min_AADT)) + 0.5 * toFloat((r.distance-min_dist)/(max_dist-min_dist)) as traffic, 
                    r.AADT as AADT, r.distance as distance, type(r) as type"
                )""")
        return result

    def delete_projected_graph(self):
        """This method deletes an existing graph projection. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph)

    @staticmethod
    def _drop_projected_graph(tx):
        result = tx.run("""
                CALL gds.graph.drop('graph')
                        """)
        return result
    
    def generate_possible_combinations(self, source, target):
        """generate_possible_combinations of road junctions from POI"""
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_possible_combinations, source, target)
            return result
    
    @staticmethod
    def _generate_possible_combinations(tx, source, target):
        result = tx.run("""
                    MATCH (p:PointOfInterest{osm_id: $source})-[:MEMBER]->(:OSMWayNode)-[r:NEAR]->(wns:RoadJunction)-[:ROUTE{status: 'active'}]->(:RoadJunction)
                        match (p1:PointOfInterest{osm_id: $target})-[:MEMBER]->(:OSMWayNode)<-[rt:NEAR]-(wnt:RoadJunction)<-[:ROUTE{status: 'active'}]-(:RoadJunction)
                    return distinct p.osm_id as POI_source,
                                    p1.osm_id as POI_target,
                                    r.distance as distance_source, 
                                    wns.id as junction_source,
                                    rt.distance as distance_target,
                                    wnt.id as junction_target
                    order by distance_source,distance_target
                    """, source=source, target=target)
        return result.values()

    def read_distance_path(self, source, target):
        """Finds the shortest path based on distance between the soruce and the target.(A*)"""
        with self.driver.session() as session:
            path = session.read_transaction(self._search_path_a_star, source, target)
            return path

    @staticmethod
    def _search_path_a_star(tx, source, target):
        result = tx.run("""
        match (sWn:RoadJunction {id: $source})
        match(tWn:RoadJunction {id: $target})
                    CALL gds.shortestPath.dijkstra.stream('graph', {
                        relationshipTypes: ['ROUTE'],
                        sourceNode: id(sWn),
                        targetNode: id(tWn),
                        relationshipWeightProperty: 'distance'
                    })
                    YIELD sourceNode, targetNode, totalCost, costs, nodeIds
                    WITH [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS nodeNames
                    UNWIND nodeNames AS node
                    RETURN node.lat, node.lon
                    """, source=source, target=target)
        return result.values()

    def read_shortest_path(self, source, target):
        """Finds the shortest path based on hops between the soruce and the target."""
        with self.driver.session() as session:
            path = session.read_transaction(self._search_path_shortest_path, source, target)
            return path

    @staticmethod
    def _search_path_shortest_path(tx, source, target):
        result = tx.run("""
                    MATCH (wns:RoadJunction {id: $source})
                        match (wnt:RoadJunction {id: $target})
                    WITH wns as sWn, wnt as tWn
                    MATCH path = shortestPath((sWn)-[:ROUTE*]->(tWn)) 
                    UNWIND nodes(path) AS node 
                    RETURN node.lat, node.lon """, source=source, target=target)
        return result.values()

    def read_traffic_path(self, source, target):
        """Finds the shortest path based on traffic between the soruce and the target.(A*)"""
        with self.driver.session() as session:
            path = session.read_transaction(self._search_path_astar_traffic, source, target)
            return path

    @staticmethod
    def _search_path_astar_traffic(tx, source, target):
        result = tx.run("""
        match (sWn:RoadJunction {id: $source})
        match(tWn:RoadJunction {id: $target})
                    CALL gds.shortestPath.dijkstra.stream('graph', {
                        relationshipTypes: ['ROUTE'],
                        sourceNode: id(sWn),
                        targetNode: id(tWn),
                        relationshipWeightProperty: 'traffic'
                    })
                    YIELD sourceNode, targetNode, totalCost, costs, nodeIds
                    WITH [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS nodeNames
                    UNWIND nodeNames AS node
                    RETURN node.lat, node.lon
                    """, source=source, target=target)
        return result.values()


def addOptions():
    parser = argparse.ArgumentParser(description='Routing between two point of interest nodes in OSM.')
    parser.add_argument('--source', '-s', dest='source', type=str,
                        help="""Insert the name of the point of interest in OSM where the route starts.""",
                        required=True)
    parser.add_argument('--destination', '-d', dest='destination', type=str,
                        help="""Insert the name of the point of interest point in OSM where the route ends.""",
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
    parser.add_argument('--lat', '-x', dest='latitude', type=float,
                        help="""Insert the latitude of the central point of the generated map. SRID 4326""",
                        required=True)
    parser.add_argument('--lon', '-y', dest='longitude', type=float,
                        help="""Insert the longitude of the central point of the generated map. SRID 4326""",
                        required=True)
    return parser


def main(args=None):
    argParser = addOptions()
    #retrieving arguments
    options = argParser.parse_args(args=args)
    sourceNode = options.source
    targetNode = options.destination
    #connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #creating the folium map
    m = fo.Map(location=[options.latitude, options.longitude], zoom_start=13)
    #asking the user what type of shortest path he needs
    mode = input('Select shortest path for distance[d], hops[h] or traffic volume[t] ')
    mode = mode.lower()
    #creating the projected graph
    greeter.create_projected_graph()
    ris = []
    result = greeter.generate_possible_combinations(int(sourceNode),int(targetNode))
    df = pd.DataFrame(result, columns = ['POI_source','POI_target','distance_source','junction_source','distance_target','junction_target'])
    df['distance_target_normalized'] = (df['distance_target']-df['distance_target'].min())/(df['distance_target'].max()-df['distance_target'].min())
    df['distance_source_normalized'] = (df['distance_source']-df['distance_source'].min())/(df['distance_source'].max()-df['distance_source'].min())
    df['sum_distance'] = df['distance_target_normalized']+df['distance_source_normalized']
    min_dist = df.groupby(['junction_source','junction_target']).min()['sum_distance'].reset_index(level=0).reset_index(level=0)
    df = df.reset_index(level=0).reset_index(level=0).set_index(['junction_source','junction_target','sum_distance']).join(min_dist.set_index(['junction_source','junction_target','sum_distance']),how = 'inner')
    df = df.reset_index(level=0).reset_index(level=0).reset_index(level=0)[['junction_source','junction_target','distance_target_normalized','distance_source_normalized','sum_distance']]
    for i,row in df.iterrows():
        if mode.startswith('d'):
            result = greeter.read_distance_path(str(row.junction_source), str(row.junction_target))
        elif mode.startswith('h'):
            result = greeter.read_shortest_path(str(row.junction_source),str(row.junction_target))
        elif mode.startswith('t'):
            result = greeter.read_traffic_path(str(row.junction_source), str(row.junction_target))
        if len(result)>0:
            ris = result
            break
    #add the path to the map
    if len(ris) == 0:
        print('\nNo result for query')
    else:
        print(ris)
        fo.PolyLine(ris, color="green", weight=4).add_to(m)
        m.save('map.html')
    greeter.delete_projected_graph()
    greeter.close()
    return 0


main()
