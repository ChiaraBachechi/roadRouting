from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to set weights on subgraphs' relationships"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def set_relations_weights(self, beta = 0.5):
        """Set weights on subgraphs' relationships"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_relations_weights, beta)
            return result

    @staticmethod
    def _set_relations_weights(tx, beta):    
        tx.run("""
                match (b:BikeJunction)-[r:BIKE_ROUTE]-(b2:BikeJunction) 
                match (b)<-[:CONTAINS]-(bl:BicycleLane)-[:CONTAINS]->(b2)
                set r.danger = bl.danger;
                """)
        tx.run("""
                match (b:BikeJunction)-[r:BIKE_ROUTE]-(b2:BikeJunction) 
                match (b)<-[:CONTAINS]-(bl:BicycleLane)-[:CONTINUE_ON_LANE]-(bl2:BicycleLane)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = round((bl.danger + bl2.danger)/2,0,'UP')""")
        tx.run("""
                match (bl:BicycleLane)-[:CONTAINS]->(b:BikeJunction)-[r:BIKE_ROUTE]-(b2:BikeJunction)<-[:CONTAINS]-(bl2:BicycleLane) 
                match (bl)-[:CONTINUE_ON_FOOTWAY]->(f:Footway)-[:CONTINUE_ON_LANE]->(bl2)
                set r.danger = round(toFloat(bl.danger+f.danger)/2.0,0,"UP");""")
        tx.run("""
                match (b:BikeJunction)-[r:BIKE_ROUTE]-(b2:BikeJunction) 
                match (b)<-[:CONTAINS]-(bl:BicycleLane)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(bl2:BicycleLane)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = round(toFloat(bl.danger + bl2.danger)/2,0,'UP') + 15 """)
        tx.run("""match (b:BikeCrossing)-[r:BIKE_ROUTE]-() set r.danger = 20""")
        tx.run("""match (b:BikeRoad)-[r:BIKE_ROUTE]-() set r.danger = 20""")
        tx.run("""MATCH (bl:BicycleLane)-[:CONTAINS]->(bk:BikeJunction)-[r:BIKE_ROUTE]-(bk1:BikeJunction)<-[:CONTAINS]-(bl1:BicycleLane)
                where not exists(r.danger) set r.danger = round((bl.danger + bl1.danger)/2,0,'UP') remove r.daner""")
        """---------------------------Footways-------------------------------"""
        tx.run("""match (f:Footway) 
                  where not  f.highway in ['path','pedestrian','footway','track','steps'] 
                   and not "BicycleLane" in labels(f) 
                   set f.danger = 3""")
        tx.run("""match (f:Footway) 
                  where f.highway in ['path','pedestrian','footway','track','steps']
                  and not "BicycleLane" in labels(f)
                  set f.danger = 1""")
        tx.run("""
                match (b:FootJunction)-[r:FOOT_ROUTE]-(b2:FootJunction) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTAINS]->(b2)
                set r.danger = bl.danger;
                """)
        tx.run("""
                match (b:FootJunction)-[r:FOOT_ROUTE]-(b2:FootJunction) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTINUE_ON_FOOTWAY]-(bl2:Footway)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = round(toFloat(bl.danger + bl2.danger)/2,0,'UP')""")
        tx.run("""
                match (b:FootJunction)-[r:FOOT_ROUTE]-(b2:FootJunction) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(bl2:Footway)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = round(toFloat(bl.danger + bl2.danger)/2,0,'UP') + 15 """)
        tx.run("""
                match (b:FootJunction)-[r:FOOT_ROUTE]->(b2:BikeJunction) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTINUE_ON_CLOSE_LANE_BY_CROSSING_ROAD]->(bl2:BicycleLane)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = 20 """)
        tx.run("""match (b:FootJunction)<-[r:FOOT_ROUTE]-(b2:BikeJunction) 
                match (b)<-[:CONTAINS]-(bl:Footway)<-[:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]-(bl2:BicycleLane)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = 20 """)
        tx.run("""match (b:FootCrossing)-[r:FOOT_ROUTE]-() set r.danger = 20""")
        tx.run("""match (b:FootRoad)-[r:FOOT_ROUTE]-() set r.danger = 20""")
        tx.run("""MATCH(bl:Footway)-[c:CONTAINS]->(bk:FootJunction)-[r:FOOT_ROUTE]-(bk1:FootJunction)<-[c1:CONTAINS]-(bl1:Footway)
                  where not exists(r.danger) 
                  set r.danger = round(toFloat(bl1.danger+bl.danger)/2.0,0,'UP')""")

        tx.run("""
                match(n)-[r:BIKE_ROUTE]-(n1) set r.speed = 15;
                """)
    
        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(n1) set r.speed = 4;
                """)
    
  
        tx.run("""
                MATCH(bl:BicycleLane)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(bl1:BicycleLane) with bl, bl1 
                MATCH(bl)-[:CONTAINS]-(bk)-[r:BIKE_ROUTE]->(bk1)<-[:CONTAINS]-(bl1) 
                set r.speed = 4;
                """)
    
        tx.run("""
                MATCH(n1)-[r:BIKE_ROUTE]->(n2) set r.travel_time = (r.distance * 3.6) /r.speed;
                """)

        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]->(n2) set r.travel_time = (r.distance * 3.6) /r.speed;                
                """)
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
    parser.add_argument('--beta', '-b', dest='beta', type=float,
                        help="""Insert the beta parameter between 0 and 1. The value represent the importance of travel time on the final cost.""",
                        required=False, default = 0.5)
   
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    if(oprions.beta > 1 or options.beta < 0):
        print("The beta parameter value is not valid, 0.5 will be used")
        options.beta = 0.5

    """Set weights on subgraphs' relationshps"""
    greeter.set_relations_weights(options.beta)
    print("Setting the relationships weight for the routing : done")


    return 0


if __name__ == "__main__":
    main()