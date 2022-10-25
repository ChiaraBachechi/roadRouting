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

    def connect_footways_to_crossing_nodes(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_footways_to_crossing_nodes)
            return result


    @staticmethod
    def _connect_footways_to_crossing_nodes(tx):
        result = tx.run("""
            match(cr:CrossNode) where NOT isEmpty(cr.closest_footways) unwind cr.closest_footways as foot 
            match(f:Footway) where f.id_num="foot/" + foot merge (f)-[r:CROSS_THE_ROAD]->(cr);
        """)

        result = tx.run("""
            match(n:CrossNode)<-[:CROSS_THE_ROAD]-(p:Footway) with n, p 
            merge (n)-[:CROSS_THE_ROAD]->(p); 

        """)

        result = tx.run("""
            match(f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(f1:Footway) with f, f1, r
            match(f)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(f1) 
            delete r; 

        """)
        return result




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
    return parser



def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    start_time = time.time()
    greeter.connect_footways_to_crossing_nodes()
    print("Connect elements close to the crossing nodes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    

    return 0


main()