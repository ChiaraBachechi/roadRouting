from neo4j import GraphDatabase
import folium as fo
import argparse


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_projected_graph(self):
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
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph)

    @staticmethod
    def _drop_projected_graph(tx):
        result = tx.run("""
                CALL gds.graph.drop('graph')
                        """)
        return result

    def read_distance_path(self, source, target):
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

    def close_street(self, street):
        with self.driver.session() as session:
            result = session.write_transaction(self._close_one_street, street)
            print('{} is now close'.format(street))
            return result

    @staticmethod
    def _close_one_street(tx, street):
        result = tx.run("""
                    MATCH ()-[r:ROUTE]-() 
                    WHERE r.name = $street  
                        SET r.status='close' 

                    WITH r 
                    MATCH (p:PointOfInterest)-[:MEMBER]->(wn:OSMWayNode)-[re:ROUTE]-(:Node)-[r]-(:Node) 
                        DELETE re 
                    WITH wn, wn.location AS poi 
                    MATCH (n:Node)-[ra:ROUTE]-(:Node) 
                        WHERE n <> wn 
                        AND distance(n.location, poi) < 100 
                        AND ra.status = 'active' 

                    WITH n, wn, distance(n.location, poi) AS dist ORDER BY dist 

                    WITH head(collect(n)) AS nv, wn 
                    MERGE (wn)-[r:ROUTE]->(nv) 
                        ON CREATE SET r.distance = distance(nv.location, wn.location), r.status='active' 
                    MERGE (wn)<-[ri:ROUTE]-(nv) 
                        ON CREATE SET ri.distance = distance(nv.location, wn.location), ri.status='active' """,
                        street=street)
        return result.values()

    def active_street(self, street):
        with self.driver.session() as session:
            result = session.write_transaction(self._active_one_street, street)
            print('{} is now active'.format(street))
            return result

    @staticmethod
    def _active_one_street(tx, street):
        result = tx.run("""
            MATCH (n)-[r:ROUTE]-() 
            WHERE r.name = $street  
                SET r.status = 'active' 
            WITH n 
            MATCH (p:PointOfInterest)-[:MEMBER]->(wn:OSMWayNode) 
            WHERE distance(wn.location, n.location) < 100 
            WITH wn, n 
            MATCH (wn)-[r:ROUTE]-() 
                DELETE r 
            WITH n, wn, distance(wn.location, n.location) AS dist ORDER BY dist 
            WITH head(collect(n)) AS nv, wn 
            MERGE (wn)-[rn:ROUTE]->(nv) 
                ON CREATE SET rn.distance = distance(wn.location, nv.location), rn.status = 'active' 
            MERGE (wn)<-[rni:ROUTE]-(nv) 
                ON CREATE SET rni.distance = distance(wn.location, nv.location), rni.status = 'active' """,
                        street=street)
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
    options = argParser.parse_args(args=args)
    sourceNode = options.source
    targetNode = options.destination
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    m = fo.Map(location=[options.latitude, options.longitude], zoom_start=13)

    mode = input('Select shortest path for distance[d], hops[h] or traffic volume[t] ')
    mode = mode.lower()

    greeter.create_projected_graph()

    if mode.startswith('d'):
        ris = greeter.read_distance_path(sourceNode, targetNode)
    elif mode.startswith('h'):
        ris = greeter.read_shortest_path(sourceNode, targetNode)
    elif mode.startswith('t'):
        ris = greeter.read_traffic_path(sourceNode, targetNode)

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
