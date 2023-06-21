import osmnx as ox
import argparse
from neo4j import GraphDatabase
import os

"""In this file we are going to show how to generate nodes representing the street nodes within 
   footways and crossings
"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def creation_graph(self, file):
        """Import street nodes data on Neo4j as a graph"""
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_graph, file)
            return result

    @staticmethod
    def _creation_graph(tx, file):
        result = tx.run("""
                        CALL apoc.import.graphml($file, {storeNodeIds: true, defaultRelationshipType: 'FOOT_ROUTE'});
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
        """get the path of the import folder of the current Neo4j database instance"""
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
        """Set the label of the nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_label)
            return result

    @staticmethod
    def _set_label(tx):
        result = tx.run("""
                        MATCH(n)-[:FOOT_ROUTE]->(n1) set n:FootNode, n1:FootNode;
                    """)
        tx.run("""match (f:FootNode)-[r:FOOT_ROUTE]->(n) merge (n)-[r1:FOOT_ROUTE]->(f) set r1 = properties(r)""")
        return result.values()

    def set_location(self):
        """Set the location attribute on the nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_location)
            return result

    @staticmethod
    def _set_location(tx):
        result = tx.run("""
                           match (n:FootNode) where not "BikeNode" in labels(n) set n.geometry = "POINT(" + n.x + " " + n.y + ")", 
                           n.location = point({latitude: tofloat(n.y), longitude: tofloat(n.x)}),n.lat = tofloat(n.y), n.lon = tofloat(n.x);
                       """)
        return result.values()

    def set_distance(self):
        """Set the distance attribute on the relationships"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_distance)
            return result

    @staticmethod
    def _set_distance(tx):
        result = tx.run("""
                          MATCH (n:FootNode)-[r:FOOT_ROUTE]-(n1:FootNode) SET r.distance=tofloat(r.length), r.status='active';
                       """)
        return result.values()
    
    def set_index(self):
        """Create a new index on the osm is of the nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_index)
            return result

    @staticmethod
    def _set_index(tx):
        result = tx.run("""
                           create index junction_footcross_index for (fc:FootNode) on (fc.id);
                       """)
        return result.values()
    def merge_with_bike_nodes(self):
        """Create a new index on the osm is of the nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_index)
            return result

    @staticmethod
    def _merge_with_bike_nodes(tx):
        result = tx.run("""
                           match (b:FootNode) match (c:BikeNode) where b.id = c.id CALL apoc.refactor.mergeNodes([b,c],{properties:"combine", mergeRels:true}) yield node return count(*)
                       """)
        return result.values()


def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Creation of routing graph.')
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
    """Parsing parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Generation of the footways subgraph nodes"""
    greeter.creation_graph(options.file_name)
    greeter.set_label()
    greeter.merge_with_bike_nodes()
    greeter.set_location()
    greeter.set_distance()
    greeter.set_index()
    greeter.close()

if __name__ == "__main__":
    main()