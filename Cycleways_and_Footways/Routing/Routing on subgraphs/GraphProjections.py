from ast import operator
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


    def create_projections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_projections)
            return result

    
    @staticmethod
    def _create_projections(tx):
        tx.run("""
                call gds.graph.project('bike_routes_cost', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction', 'RoadJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME', 'ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)

        tx.run("""
                call gds.graph.project('bike_routes_travel_time', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction', 'RoadJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME', 'ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)

        tx.run("""
                call gds.graph.project('foot_routes_cost', ['FootCross', 'JunctionFootCross', 'RoadFootJunction', 'RoadJunction'], 
                ['FOOT_ROUTE', 'ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)

        result = tx.run("""
                call gds.graph.project('foot_routes_travel_time', ['FootCross', 'JunctionFootCross', 'RoadFootJunction', 'RoadJunction'], 
                ['FOOT_ROUTE', 'ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
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
   
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    greeter.create_projections()
    print("Create graph projections for the routing : done")

    return 0


main()