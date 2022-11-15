from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going ti show how to generate nodes referring to signaled crossings mapped as ways on OSM"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def import_crossways(self, file):

        """Import crossing ways nodes on Neo4j and generate CrossWay nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_crossways, file)
            return result

    @staticmethod
    def _import_crossways(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record
                        MERGE(n:Crossing:CrossWay {id_num : "crossway/" + record.id_num}) ON CREATE SET 
                        n.osm_id = record.id, n.geometry = record.geometry, 
                        n.crossing=record.crossing, n.bicycle=record.bicycle, n.closest_lanes = record.closest_lanes, 
                        n.closest_footways = record.closest_footways, n.length = record.length
                    """, file=file)

        return result.values()


    def import_crossways_in_spatial_layer(self):
        """Import CrossWay nodes on a Neo4j Spatial Layer"""

        with self.driver.session() as session:
            result = session.write_transaction(self._import_crossways_in_spatial_layer)
            return result

    @staticmethod
    def _import_crossways_in_spatial_layer(tx):
        result = tx.run("""
                       match(n:CrossWay) with collect(n) as crossway UNWIND crossway AS cw 
                       CALL spatial.addNode('spatial', cw) yield node return node
        """)
                       
        return result.values()



    



def add_options():
    """Parameters needed to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of crossways in the graph.')
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
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Import crossing ways data and generate nodes"""
    start_time = time.time()
    greeter.import_crossways(options.file_name)
    print("import crossing_ways.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Import CrossWay nodes in a Neo4j Spatial Layer"""
    start_time = time.time()
    greeter.import_crossways_in_spatial_layer()
    print("Import crossways in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))



    return 0


if __name__ == "__main__":
    main()