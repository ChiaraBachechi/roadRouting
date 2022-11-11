from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to generate relationships between CrossWay and BicycleLane nodes"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_lanes_to_crossing_nodes(self):
        """Generate relationships between BicycleLane and Footway nodes representing cycling and foot paths that
           touch or intersect
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_lanes_to_crossing_nodes)
            return result


    @staticmethod
    def _connect_lanes_to_crossing_nodes(tx):
        result = tx.run("""
            match(cr:CrossNode) where NOT isEmpty(cr.closest_lanes) unwind cr.closest_lanes as lane 
            match(b:BicycleLane) where b.id_num="cycleway/" + lane merge (b)-[r:CROSS_THE_ROAD]->(cr);
        """)

        result = tx.run("""
            match(cr:CrossNode)<-[:CROSS_THE_ROAD]-(b:BicycleLane) with cr, b 
            merge (cr)-[r:CROSS_THE_ROAD]->(b); 

        """)

        result = tx.run("""
            match(bl:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(bl1:BicycleLane) with bl, bl1, r
            match(bl)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(bl1) 
            delete r; 

        """)
        return result




def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Connection of cycleways through crossings of type node.')
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

    """Generate relationships between BicycleLane and CrossNode nodes"""
    start_time = time.time()
    greeter.connect_lanes_to_crossing_nodes()
    print("Connect elements close to the crossing nodes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    

    return 0


#main()