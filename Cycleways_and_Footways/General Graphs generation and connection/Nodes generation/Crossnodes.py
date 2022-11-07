from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to generate nodes referring to signaled crossings mapped on OSM as nodes"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def import_crossnodes(self, file):
        """Import crossing nodes data on Neo4j and generate Crossnode nodes"""

        with self.driver.session() as session:
            result = session.write_transaction(self._import_crossnodes, file)
            return result

    @staticmethod
    def _import_crossnodes(tx, file):
        result = tx.run("""
                        CALL apoc.load.json($file) YIELD value AS value 
                        WITH value.data AS data
                        UNWIND data AS cross
                        MERGE (n:Crossing:CrossNode {id_num : "crossnode/" + cross.id_num})
                        ON CREATE SET n.osm_id = cross.id, n.geometry=cross.geometry,
                        n.crossing=cross.crossing, n.kerb=cross.kerb, n.bicycle=cross.bicycle, 
                        n.button_operated=cross.button_operated, n.closest_footways = cross.closest_footways,
                        n.closest_lanes = cross.closest_lanes;
                    """, file=file)

        return result.values()

    def compute_location(self, file):
        """Compute the attribute location on nodes"""

        with self.driver.session() as session:
            result = session.write_transaction(self._compute_location, file)
            return result

    @staticmethod
    def _compute_location(tx):
        result = tx.run("""
                            match(cr:CrossNode) set cr.location = point({latitude:cr.bbox[1], longitude:cr.bbox[0]})
                        """)

        return result.values()


    def import_crossnodes_in_spatial_layer(self):
        """Import CrossNode nodes on a Neo4j Spatial layer"""

        with self.driver.session() as session:
            result = session.write_transaction(self._import_crossnodes_in_spatial_layer)
            return result

    @staticmethod
    def _import_crossnodes_in_spatial_layer(tx):
        result = tx.run("""
                       match(n:CrossNode) with collect(n) as crossnodes UNWIND crossnodes AS cn 
                       CALL spatial.addNode('spatial', cn) yield node return node
        """)
                       
        return result.values()



    



def add_options():
    """Parameters needed to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
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
                        help="""Insert the name of the .geojson file.""",
                        required=True)
    return parser


def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Import crossing nodes data on Neo4j and generate Crossnode nodes"""
    start_time = time.time()
    greeter.import_crossnodes(options.file_name)
    print("import crossing_nodes.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Compute the location attribute on the nodes"""
    start_time = time.time()
    greeter.compute_location()
    print("Compute the location of the nodes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Import CrossNode nodes on a Neo4j Spatial Layer"""
    start_time = time.time()
    greeter.import_crossnodes_in_spatial_layer()
    print("Import crossnodes in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    return 0


main()