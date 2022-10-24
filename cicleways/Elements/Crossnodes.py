from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def import_crossnodes(self, file):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_crossnodes, file)
            return result

    @staticmethod
    def _import_crossnodes(tx, file):
        #import crossnodes from the local file
        result = tx.run("""
                        CALL apoc.load.json($file) YIELD value AS value 
                        WITH value.data AS data
                        UNWIND data AS cross
                        MERGE (n:Crossing:CrossNode {id : cross.id_num})
                        ON CREATE SET n.osm_id = cross.id, n.geometry=cross.geometry,
                        n.crossing=cross.crossing, n.kerb=cross.kerb, n.bicycle=cross.bicycle, 
                        n.button_operated=cross.button_operated
                    """, file=file)

        return result.values()


    def import_crossnodes_in_spatial_layer(self):
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
    #connection to the graph instance
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    #generate nodes from the crossenodes in the local file
    start_time = time.time()
    greeter.import_crossnodes(options.file_name)
    print("import crossing_nodes.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    #insert the crossnodes in the spatial layer
    start_time = time.time()
    greeter.import_crossnodes_in_spatial_layer()
    print("Import crossnodes in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    return 0


main()
