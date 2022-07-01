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
        """create the graph from the .graphml file"""
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_graph, file)
            return result

    @staticmethod
    def _creation_graph(tx, file):
        result = tx.run("""
                        CALL apoc.import.graphml($file, {storeNodeIds: true, defaultRelationshipType: 'ROUTE'});
                    """, file=file)
        return result.values()

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
        """get the name of the import directory for the neo4j instance"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    def set_label(self):
        """set the Node lable to nodes"""
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
        """insert the location in the node attributes"""
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
        """insert the distance in the nodes' relationships."""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_distance)
            return result

    @staticmethod
    def _set_distance(tx):
        result = tx.run("""
                           MATCH (n:Node)-[r:ROUTE]-() SET r.distance=tofloat(r.length), r.status='active'
                       """)
        return result.values()
    
    def set_index(self):
        """create index on nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_index)
            return result

    @staticmethod
    def _set_index(tx):
        result = tx.run("""
                           create index on :Node(id)
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
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    #retireve attributes
    options = argParser.parse_args(args=args)
    #connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' + options.file_name
    #using osmnx to generate the graphml file
    G = ox.graph_from_point((options.lat, options.lon),
                            dist=int(options.dist),
                            dist_type='bbox',
                            simplify=False,
                            network_type='drive'
                            )
    ox.save_graphml(G, path)
    #creating the graph
    greeter.creation_graph(options.file_name)
    #setting nodes' labels
    greeter.set_label()
    #setting nodes' locations
    greeter.set_location()
    #setting nodes' distances
    greeter.set_distance()
    #setting index
    greeter.set_index()
    greeter.close()

    return 0


main()
