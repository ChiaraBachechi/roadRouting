import osmnx as ox
import argparse
from neo4j import GraphDatabase
import os


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def creation_graph(self, file):
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_graph, file)
            return result

    @staticmethod
    def _creation_graph(tx, file):
        result = tx.run("""
                        CALL apoc.import.graphml($file, {storeNodeIds: true, defaultRelationshipType: 'ROUTE'});
                    """, file=file)
        return result.values()

    def set_label(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_label)
            return result

    @staticmethod
    def _creation_label(tx):
        result = tx.run("""
                        MATCH (n) SET n:Node;
                    """)
        return result.values()

    def set_location(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_location)
            return result

    @staticmethod
    def _creation_location(tx):
        result = tx.run("""
                           MATCH (n:Node) SET n.location = point({latitude: tofloat(n.y), longitude: tofloat(n.x)}),
                                            n.lat = tofloat(n.y), 
                                            n.lon = tofloat(n.x);
                       """)
        return result.values()

    def set_distance(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._set_distance)
            return result

    @staticmethod
    def _set_distance(tx):
        result = tx.run("""
                           MATCH (n:Node)-[r:ROUTE]-() SET r.distance=tofloat(r.length), r.status='active'
                       """)
        return result.values()


def add_options():
    parser = argparse.ArgumentParser(description='Creation of routing graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
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
    parser.add_argument('--importDir', '-i', dest='neo4j_import', type=str,
                        help="""Insert the path of the Neo4j import directory, where have to save the .graphml file.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    path = os.path.join(options.neo4j_import, options.file_name)
    G = ox.graph_from_point((options.lat, options.lon),
                            dist=options.dist,
                            simplify=False,
                            custom_filter='["highway"] '
                            '["highway"!="path"] '
                            '["highway"!="cycleway"] '
                            '["highway"!="footway"] '
                            '["highway"!="pedestrian"] '
                            '["highway"!="steps"] '
                            '["highway"!="service"] '
                            '["highway"!="raceway"] '
                            '["highway"!="track"] '
                            )

    ox.save_graphml(G, path)

    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    greeter.creation_graph(options.file_name)
    greeter.set_label()
    greeter.set_location()
    greeter.set_distance()
    greeter.close()

    return 0


main()
