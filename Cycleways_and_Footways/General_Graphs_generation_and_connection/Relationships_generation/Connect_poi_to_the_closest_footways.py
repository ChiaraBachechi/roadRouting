from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to connect PointOfInterest nodes with the Footway nodes representing the closest
   footways w.r.t the current POI
 """

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_poi_to_closest_footways(self):
        """Generate relationships between Footway and PointOfInterest nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_poi_to_closest_footways)
            return result


    @staticmethod
    def _connect_poi_to_closest_footways(tx):
        result = tx.run("""
            match(n:OSMWayNode) with n call spatial.withinDistance('spatial', n.location, 0.1) yield node 
            unwind(node) as p match(n1) where n1.id_num=p.id_num and n1:Footway merge (n1)-[:IS_NEAR_TO]->(n)  
        """)
        return result




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
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Generate relationships between Footway and PointOfInterest nodes"""
    start_time = time.time()
    greeter.connect_poi_to_closest_footways()
    print("Connect POI to the closest footways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    

    return 0


#main()