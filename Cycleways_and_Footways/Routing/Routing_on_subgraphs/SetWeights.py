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

    def set_relations_weights(self):
        """Set weights on subgraphs' relationships"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_relations_weights)
            return result

    @staticmethod
    def _set_relations_weights(tx):    
        tx.run("""
                MATCH(bl:BicycleLane)-[:CONTINUE_ON_LANE*1..2]-(bl1:BicycleLane) 
                with bl, bl1 MATCH(bl)-[:CONTAINS]-(bk:BikeCross)-[r:BIKE_ROUTE]->(bk1:BikeCross)<-[:CONTAINS]-(bl1) 
                set r.speed = 15, r.danger = toFloat((bl.danger + bl1.danger)/2);
                """)
    
        tx.run("""
                MATCH(f:Footway)-[:CONTINUE_ON_FOOTWAY*1..2]-(f1:Footway) 
                with f,f1 MATCH(f)-[:CONTAINS]-(fc:FootCross)-[r:FOOT_ROUTE]->(fc1:FootCross)<-[:CONTAINS]-(f1) 
                set r.speed = 4, r.danger = toFloat((f.danger + f1.danger)/2);
                """)

        tx.run("""
                match(n)-[r:BIKE_ROUTE]-(n1) set r.speed = 15;
                """)
    
        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(n1) set r.speed = 4;
                """)

        tx.run("""
                MATCH(n)-[r:ROUTE]->(n1) set r.danger = 20, r.speed = 15; 
                """)
    
        tx.run("""
                MATCH(bk:BikeCross)-[r:BIKE_ROUTE]->(rj:RoadBikeJunction) set r.danger = 5;
                """)

        tx.run("""
                MATCH(bk:BikeCross)<-[r:BIKE_ROUTE]-(rj:RoadBikeJunction) set r.danger = 5;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[r:BIKE_ROUTE]->(rj:RoadJunction) set r.danger = 20;
                """)

        tx.run("""
                MATCH(bk:BikeCross)<-[r:BIKE_ROUTE]-(rj:RoadJunction) set r.danger = 20;
                """)

    
        tx.run("""
                MATCH(fc:FootCross)-[r:FOOT_ROUTE]->(rj:RoadFootJunction) set r.danger = 5;
                """)
    
        tx.run("""
                MATCH(fc:FootCross)<-[r:FOOT_ROUTE]-(rj:RoadFootJunction) set r.danger = 5;
                """)
    
        tx.run("""
                MATCH(rj:RoadBikeJunction)-[r:BIKE_ROUTE]->(rj1:RoadBikeJunction) set r.danger = 5;
                """)

        tx.run("""
                MATCH(rj:RoadBikeJunction)<-[r:BIKE_ROUTE]-(rj1:RoadBikeJunction) set r.danger = 5;
                """)
    
        tx.run("""
                MATCH(rj:RoadFootJunction)-[r:FOOT_ROUTE]->(rj1:RoadFootJunction) set r.danger = 5;
                """)

        tx.run("""
                MATCH(rj:RoadFootJunction)<-[r:FOOT_ROUTE]-(rj1:RoadFootJunction) set r.danger = 5;
                """)
    
        tx.run("""
                MATCH (n)-[r:BIKE_ROUTE]->(jbk:JunctionBikeCross) set r.danger = 5, r.speed = 4;
                """)

        tx.run("""
                MATCH (n)<-[r:BIKE_ROUTE]-(jbk:JunctionBikeCross) set r.danger = 5, r.speed = 4;
                """)

        tx.run("""
                MATCH (n)-[r:BIKE_ROUTE]->(jbk:JunctionBikeCross) where r.distance > 20 set r.danger = 20, r.speed = 15;
                """)

        tx.run("""
                MATCH (n)<-[r:BIKE_ROUTE]-(jbk:JunctionBikeCross) where r.distance > 20 set r.danger = 20, r.speed = 15;
                """)
    
        tx.run("""
                MATCH(bl:BicycleLane)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(bl1:BicycleLane) with bl, bl1 
                MATCH(bl)-[:CONTAINS]-(bk:BikeCross)-[r:BIKE_ROUTE]->(bk1:BikeCross)<-[:CONTAINS]-(bl1) 
                set r.danger = 20, r.speed = 4;
                """)

        tx.run("""
                match(bl:BicycleLane)-[:CONTAINS]-(n)-[r:BIKE_ROUTE]-(n1)<-[:CONTAINS]-(bl1:BicycleLane) 
                where bl.id_num <> bl1.id_num and not exists((bl)--(bl1)) set r.danger = 5, r.speed = 4;                
                """)
    
        tx.run("""
                MATCH(bl:BicycleLane)-[:CONTAINS]->(bk:BikeCross)-[r:BIKE_ROUTE]-(bk1:BikeCross)<-[:CONTAINS]-(bl1:BicycleLane) 
                where bl.id_num <> bl1.id_num and bk.location <> bk1.location and not exists((bl)-[:CONTINUE_ON_LANE *1..2]-(bl1)) 
                and not exists((bl)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(bl1)) set r.danger = 20, r.speed = 4;                
                """)

        tx.run("""
                MATCH(bl:BicycleLane)-[:CONTAINS]->(bk:BikeCross)-[r:BIKE_ROUTE]->(bk1:BikeCross)<-[:CONTAINS]-(bl1:BicycleLane) 
                where bl.id_num = bl1.id_num set r.danger = bl.danger, r.speed = 15;
                """)
    
        tx.run("""
                match(f:Footway)-[:CONTINUE_ON_LANE]-(bl:BicycleLane) with collect(bl) as lanes, f 
                set f.danger = reduce(sum = 0, lane in lanes | sum + lane.danger);""")

        tx.run("""
                match(f:Footway)-[:CONTINUE_ON_LANE]-(bl:BicycleLane) with collect(bl) as lanes, f set f.danger = f.danger/size(lanes);
                """)
    
        tx.run("""
                match(f:Footway) where isEmpty(f.touched_lanes) set f.danger = 1;
                """)

        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(jbk:JunctionFootCross) set r.danger = 5, r.speed = 4;
                """)
    
        tx.run("""
                MATCH(n)<-[r:FOOT_ROUTE]-(jbk:JunctionFootCross) set r.danger = 5, r.speed = 4;
                """)

        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(jbk:JunctionFootCross) where r.distance > 20 set r.danger = 20, r.speed = 4;
                """)
    
        tx.run("""
                MATCH(n)<-[r:FOOT_ROUTE]-(jbk:JunctionFootCross) where r.distance > 20 set r.danger = 20, r.speed = 4;
                """)
    
        tx.run("""
                match(n)-[r:FOOT_ROUTE]-(n1) set r.speed = 4;
                """)

        tx.run("""
                match(f:Footway) set f.speed = 4;
                """)

        tx.run("""
                match(f:Footway) where f.highway = "path" or f.highway = "cycleway" set f.speed = 15;
                """) 
    
        tx.run("""
                match(f:Footway)-[:CONTAINS]-(n)-[r:FOOT_ROUTE]-(n1)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num <> f1.id_num and not exists((f)--(f1)) set r.danger = 5;
                """)

        tx.run("""
                match(f:Footway)-[:CONTAINS]-(n)-[r:FOOT_ROUTE]-(n1)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num <> f1.id_num and not exists((f)--(f1)) and r.distance > 20 set r.danger = 20;
                """)

        tx.run("""
                MATCH(f:Footway)-[:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                with f, f1 MATCH(f)-[:CONTAINS]-(fc:FootCross)-[r:FOOT_ROUTE]->(fc1:FootCross)<-[:CONTAINS]-(f1) set r.danger = 20, r.speed = 4;
                """)
    
        tx.run("""
                MATCH(f:Footway)-[:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) with f, f1 
                MATCH(f)-[:CONTAINS]-(fc:FootCross)-[r:FOOT_ROUTE]->(jfc:JunctionFootCross)<-[:CONTAINS]-(f1) set r.danger = 20, r.speed = 4;
                """)

        tx.run("""
                MATCH(f:Footway)-[:CONTAINS]->(fc:FootCross)-[r:FOOT_ROUTE]->(fc1:FootCross)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num = f1.id_num set r.danger = f.danger, r.speed = f.speed;
                """)

        tx.run("""
                MATCH(f:Footway)-[:CONTAINS]->(fc:FootCross)-[r:FOOT_ROUTE]->(fc1:JunctionFootCross)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num = f1.id_num and r.distance > 20 set r.danger = 5, r.speed = f.speed;
                """)

        tx.run("""
                MATCH(f:Footway)-[:CONTAINS]->(fc:JunctionFootCross)-[r:FOOT_ROUTE]->(fc1:FootCross)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num = f1.id_num and r.distance > 20 set r.danger = 5, r.speed = f.speed;
                """)

        tx.run("""
                MATCH(f:Footway)-[:CONTAINS]->(fc:JunctionFootCross)-[r:FOOT_ROUTE]->(fc1:JunctionFootCross)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num = f1.id_num and r.distance > 20 set r.danger = 5, r.speed = f.speed;
                """)
    
        tx.run("""
                MATCH(n1)-[r:BIKE_ROUTE]->(n2) set r.travel_time = r.distance/(1000*r.speed);
                """)

        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]->(n2) set r.travel_time = r.distance/(1000*r.speed);                
                """)

        tx.run("""
                MATCH(n1)-[r:ROUTE]->(n2) set r.travel_time = r.distance/(1000*r.speed);
                """)


        tx.run("""
                MATCH(n1)-[r:BIKE_ROUTE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time 
                MATCH(n3)-[r1:BIKE_ROUTE]-(n4) with max_travel_time, min_travel_time, r1 
                set r1.cost = 0.5*(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + 0.5*(r1.danger-1)/19
                """)

        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time 
                MATCH(n3)-[r1:FOOT_ROUTE]-(n4) with max_travel_time, min_travel_time, r1 
                set r1.cost = 0.5*(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + 0.5*(r1.danger-1)/19
                """)

        tx.run("""
                MATCH(n1)-[r:ROUTE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time 
                MATCH(n3)-[r1:ROUTE]-(n4) with max_travel_time, min_travel_time, r1 
                set r1.cost = 0.5*(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + 0.5*(r1.danger-1)/19
                """)

        tx.run("""
                MATCH(b)<-[r:IS_THE_SAME]-(b1) set r.travel_time = 0, r.cost = 0, r.danger = 0;
                """)

        result = tx.run("""
                MATCH(b)-[r:IS_THE_SAME]->(b1) set r.travel_time = 0, r.cost = 0, r.danger = 0;
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
   
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Set weights on subgraphs' relationshps"""
    greeter.set_relations_weights()
    print("Setting the relationships weight for the routing : done")


    return 0


#main()