from neo4j import GraphDatabase
import folium as fo
import argparse


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
                    "MATCH (n) where n:Node or n:OSMWayNode RETURN id(n) AS id, n.lat AS lat, n.lon AS lon",
                    "MATCH ()-[r:ROUTE]->() with min(r.AADT) as min_AADT,max(r.AADT) as max_AADT,max(r.distance) as max_dist,min(r.distance) as min_dist MATCH (n)-[r:ROUTE]->(m) WHERE r.status = 'active' RETURN id(n) AS source, id(m) AS target, 0.5 * toFloat((r.AADT-min_AADT)/(max_AADT-min_AADT)) + 0.5 * toFloat((r.distance-min_dist)/(max_dist-min_dist)) as traffic, r.AADT as AADT, r.distance as distance, type(r) as type"
                )
                        """)
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

    def read_distance_path(self, source, target):
        """Finds the shortest path based on distance between the soruce and the target.(A*)"""
        with self.driver.session() as session:
            path = session.read_transaction(self._search_path_a_star, source, target)
            return path

    @staticmethod
    def _search_path_a_star(tx, source, target):
        result = tx.run("""
                    MATCH (p:PointOfInterest{name: $source})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wns:Node)
                        match (p1:PointOfInterest{name: $target})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wnt:Node)
                    WITH wns as sWn, wnt as tWn
                    CALL gds.shortestPath.astar.stream('graph', {
                        relationshipTypes: ['ROUTE'],
                        sourceNode: id(sWn),
                        targetNode: id(tWn),
                        latitudeProperty: 'lat',
                        longitudeProperty: 'lon',
                        relationshipWeightProperty: 'distance'
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
                    WITH [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS nodeNames
                    UNWIND nodeNames AS node
                    RETURN node.lat, node.lon """, source=source, target=target)
        return result.values()

    def read_shortest_path(self, source, target):
        """Finds the shortest path based on hops between the soruce and the target."""
        with self.driver.session() as session:
            path = session.read_transaction(self._search_path_shortest_path, source, target)
            return path

    @staticmethod
    def _search_path_shortest_path(tx, source, target):
        result = tx.run("""
                    MATCH (p:PointOfInterest{name: $source})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wns:Node)
                        match (p1:PointOfInterest{name: $target})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wnt:Node)
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
                    MATCH (p:PointOfInterest{name: $source})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wns:Node)
                        match (p1:PointOfInterest{name: $target})-[:MEMBER]->(:OSMWayNode)-[:ROUTE]->(wnt:Node)
                    WITH wns as sWn, wnt as tWn
                    CALL gds.shortestPath.dijkstra.stream('graph', {
                        relationshipTypes: ['ROUTE'],
                        sourceNode: id(sWn),
                        targetNode: id(tWn),
                        relationshipWeightProperty: 'traffic'
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
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
    #evaluating the shortest path
    if mode.startswith('d'):
        ris = greeter.read_distance_path(sourceNode, targetNode)
    elif mode.startswith('h'):
        ris = greeter.read_shortest_path(sourceNode, targetNode)
    elif mode.startswith('t'):
        ris = greeter.read_traffic_path(sourceNode, targetNode)
    #add the path to the map
    if len(ris) == 0:
        print('\nNo result for query')
    else:
        print(ris)
        greeter.delete_projected_graph()
        fo.PolyLine(ris).add_to(m)
        m.save('map.html')

    greeter.close()
    return 0


main()
