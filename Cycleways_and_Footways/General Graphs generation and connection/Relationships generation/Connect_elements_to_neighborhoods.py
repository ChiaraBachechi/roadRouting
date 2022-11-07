from neo4j import GraphDatabase
import overpy
import json
import argparse
import os

"""In this file we are going to show how to connect Neighborhood nodes with all the other kind of nodes we have
   within the Neo4j database instance
"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def elements_within_neighborhoods(self):
        """Look for all the nodes representing elements that lie within a neighborhood"""
        with self.driver.session() as session:
            result = session.write_transaction(self._elements_within_neighborhoods)
            return result

    @staticmethod
    def _elements_within_neighborhoods(tx):
        result = tx.run("""
                        match(n:Neighborhood) with n call spatial.intersects('spatial', n.geometry) 
                        yield node UNWIND node as p match(n1:Neighborhood) 
                        where n.id=n1.id AND NOT p:Neighborhood
                        merge(p)-[:WITHIN]->(n1) return p, n1
        """)
                        #QUERY PER SAPERE I POI ALL'INTERNO DI UN QUARTIERE:

                        #match(poi:PointOfInterest)-[:MEMBER]-(p)-[:WITHIN]-(n:Neighborhood) where n.id="1.0" return poi 
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
    return parser



def main(args=None):
    """Parsing parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Find all the nodes representing elements that lie within a neighborhood and connect the with the corresponding
       Neighborhood node
    """
    greeter.elements_within_neighborhoods()
    print("Connect to the right neighborhood elements within it: done")

    

    return 0


main()