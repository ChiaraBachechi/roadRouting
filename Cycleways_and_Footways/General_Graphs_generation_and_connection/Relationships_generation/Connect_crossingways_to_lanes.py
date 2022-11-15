from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to how to connect BicycleLane and CrossWay nodes"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_crossways_to_bicycle_lanes(self):
        """Generate relationships between BicycleLane and CrossWay nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_crossways_to_bicycle_lanes)
            return result


    @staticmethod
    def _connect_crossways_to_bicycle_lanes(tx):
        result = tx.run("""
            match(cr:CrossWay) where NOT isEmpty(cr.closest_lanes) unwind cr.closest_lanes as lane 
            match(b:BicycleLane) where b.id_num="cycleway/" + lane merge (b)-[r:CROSS_THE_ROAD]->(cr);
        """)

        result = tx.run("""
            match(cr:CrossWay)<-[:CROSS_THE_ROAD]-(b:BicycleLane) with cr, b merge (cr)-[r:CROSS_THE_ROAD]->(b); 
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
    """Parsing parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Generate relationships between BicycleLane and CrossWay nodes"""
    start_time = time.time()
    greeter.connect_crossways_to_bicycle_lanes()
    print("Connect lanes to the crossing ways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    

    return 0


if __name__ == "__main__":
    main()