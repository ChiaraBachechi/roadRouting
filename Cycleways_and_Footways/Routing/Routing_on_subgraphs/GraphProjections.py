from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show hoe to generate projections of subgraphs in order to perform routing"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def create_projections(self):
        """Create projections, one considering as weight the travel time and one the cost"""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_projections)
            return result

    
    @staticmethod
    def _create_projections(tx):
        tx.run("""
                call gds.graph.create('bike_routes_cost', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)

        tx.run("""
                call gds.graph.create('bike_routes_travel_time', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)

        tx.run("""
                call gds.graph.create('foot_routes_cost', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)

        result = tx.run("""
                call gds.graph.create('foot_routes_travel_time', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)

        return result.values()




def add_options():
    """Parameters nedeed to run the script"""
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

    """Generate projections"""
    greeter.create_projections()
    print("Create graph projections for the routing : done")

    return 0

if __name__ == "__main__":
    main()