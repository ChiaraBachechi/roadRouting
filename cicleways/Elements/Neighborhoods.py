from neo4j import GraphDatabase
import overpy
import json
import argparse
import os

#generation of the nodes of the neighbourhoods

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def import_neighborhood_node(self, file):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_neighborhood_node, file)
            return result

    @staticmethod
    def _import_neighborhood_node(tx, file):
        #create one node for each neighbourhood defined in the local file
        #insert hte WKT definition of its polygonal geometry inside the properties
        result = tx.run("""
                        CALL apoc.load.csv($file) YIELD map as lines 
                        MERGE(n:Neighborhood {id : lines.id}) ON CREATE SET n.geometry = lines.geometry;
                    """, file=file)

        return result.values()

    
    def import_neighborhoods_in_spatial_layer(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._import_neighborhoods_in_spatial_layer)
            return result

    @staticmethod
    def _import_neighborhoods_in_spatial_layer(tx):
        #inserting the neighbourhoods in the spatial layer
        result = tx.run("""
                       match(n:Neighborhood) with collect(n) as neighborhoods UNWIND neighborhoods AS nb 
                       CALL spatial.addNode('spatial', nb) yield node return node
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
                        help="""Insert the name of the .csv file.""",
                        required=True)
    return parser


def main(args=None):
    #connection to the graph instance
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    #generate a node for each neighborhood in the local file
    greeter.import_neighborhood_node(options.file_name)
    print("import QuartieriModena.csv: done")
    
    #insert the inserted nodes in the spatial layer
    greeter.import_neighborhoods_in_spatial_layer()
    print("Import neighborhood nodes in the spatial layer: done")


    return 0


main()
