from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to set the weights on general graph relationships
   in order to perform routing 
"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def set_weights(self):
        """set the weights on general graph relationships"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_weights)
            return result

    
    @staticmethod
    def _set_weights(tx):
        tx.run("match(bl:BicycleLane)-[r:CROSS_THE_ROAD]->(cn:CrossNode) set r.distance = bl.length + 5;") 
        tx.run("match(bl:BicycleLane)<-[r:CROSS_THE_ROAD]-(cn:CrossNode) set r.distance = bl.length + 5;") 
        tx.run("match(bl:BicycleLane)-[r:CROSS_THE_ROAD]->(cn:CrossWay) set r.distance = bl.length + cn.length/2;") 
        tx.run("match(bl:BicycleLane)<-[r:CROSS_THE_ROAD]-(cn:CrossWay) set r.distance = bl.length + cn.length/2;")


        tx.run("match(f:Footway)-[r:CROSS_THE_ROAD]->(cn:CrossNode) set r.distance = f.length + 5;") 
        tx.run("match(f:Footway)<-[r:CROSS_THE_ROAD]-(cn:CrossNode) set r.distance = f.length + 5;") 
        tx.run("match(f:Footway)-[r:CROSS_THE_ROAD]->(cn:CrossWay) set r.distance = f.length + cn.length/2;") 
        tx.run("match(bl:BicycleLane)<-[r:CROSS_THE_ROAD]-(cn:CrossWay) set r.distance = f.length + cn.length/2;")


        tx.run("match(n)-[r:CONTINUE_ON_LANE]->(n1) set r.distance=n.length;")
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY]->(n1) set r.distance=n.length;") 
        tx.run("match(n)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(n1) set r.distance=n.length+r.length/2;") 
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(n1) set r.distance=n.length+r.length/2;") 

        tx.run("match(n)-[r:CONTINUE_ON_LANE]->(n1) set r.danger=n.danger;")
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY]->(n1) set r.danger=n.danger;") 
        tx.run("match(n)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(n1) set r.danger=20;") 
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(n1) set r.danger=20; match(n)-[r:CROSS_THE_ROAD]->(n1) set r.danger=2.5;") 

        tx.run("match(n)-[r:CONTINUE_ON_LANE]->(n1) set r.travel_time=r.distance/(1000*n.speed);")
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY]->(n1) set r.travel_time=r.distance/(1000*n.speed);") 
        tx.run("match(n)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(n1) set r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (r.length/2)/(1000*4);") 
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(n1) set  r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (r.length/2)/(1000*4);") 
        tx.run("match(n)-[r:CROSS_THE_ROAD]->(n1:CrossNode) set  r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (5)/(1000*4);")  
        tx.run("match(n)-[r:CROSS_THE_ROAD]->(n1:CrossWay) set  r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (n1.length/2)/(1000*4);") 
        tx.run("match(n)<-[r:CROSS_THE_ROAD]-(n1:CrossNode) set  r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (5)/(1000*4);")  
        tx.run("match(n)<-[r:CROSS_THE_ROAD]-(n1:CrossWay) set  r.travel_time_1=n.length/(1000*n.speed), r.travel_time_2 = (n1.length/2)/(1000*4);") 


        tx.run("match(n)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(n1) set r.travel_time = r.travel_time_1 + r.travel_time_2;") 
        tx.run("match(n)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(n1) set  r.travel_time = r.travel_time_1 + r.travel_time_2;") 
        tx.run("match(n)-[r:CROSS_THE_ROAD]->(n1:CrossNode) set  r.travel_time = r.travel_time_1 + r.travel_time_2;")  
        tx.run("match(n)-[r:CROSS_THE_ROAD]->(n1:CrossWay) set  r.travel_time = r.travel_time_1 + r.travel_time_2;") 
        tx.run("match(n)<-[r:CROSS_THE_ROAD]-(n1:CrossNode) set  r.travel_time = r.travel_time_1 + r.travel_time_2;")  
        tx.run("match(n)<-[r:CROSS_THE_ROAD]-(n1:CrossWay) set  r.travel_time = r.travel_time_1 + r.travel_time_2;")


        tx.run("MATCH(n1)-[r:CONTINUE_ON_LANE]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time MATCH(n3)-[r1:CONTINUE_ON_LANE]-(n4) with max_travel_time, min_travel_time, r1 set r1.cost = 0.5*(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + 0.5*(r1.danger-1)/5")


        tx.run("MATCH(n1)-[r:CONTINUE_ON_FOOTWAY]-(n2) with max(r.travel_time) as max_travel_time, min(r.travel_time) as min_travel_time MATCH(n3)-[r1:CONTINUE_ON_FOOTWAY]-(n4) with max_travel_time, min_travel_time, r1 set r1.cost = 0.5*(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + 0.5*(r1.danger-1)/5")

        tx.run("MATCH(n1)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(n2) with max(r.travel_time_1) as max_travel_time_1, min(r.travel_time_1) as min_travel_time_1, max(r.travel_time_2) as max_travel_time_2, min(r.travel_time_2) as min_travel_time_2 MATCH(n3)-[r1:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(n4) with max_travel_time_1, min_travel_time_1,  max_travel_time_2, min_travel_time_2, r1, n3 set r1.cost = (0.5*(r1.travel_time_1-min_travel_time_1)/(max_travel_time_1-min_travel_time_1) + 0.5*(n3.danger-1)/4) + (0.5*(r1.travel_time_2-min_travel_time_2)/(max_travel_time_2-min_travel_time_2) + 0.5);")



        tx.run("MATCH(n1)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(n2) with max(r.travel_time_1) as max_travel_time_1, min(r.travel_time_1) as min_travel_time_1, max(r.travel_time_2) as max_travel_time_2, min(r.travel_time_2) as min_travel_time_2 MATCH(n3)-[r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(n4) with max_travel_time_1, min_travel_time_1,  max_travel_time_2, min_travel_time_2, r1, n3 set r1.cost = (0.5*(r1.travel_time_1-min_travel_time_1)/(max_travel_time_1-min_travel_time_1) + 0.5*(n3.danger-1)/4) + (0.5*(r1.travel_time_2-min_travel_time_2)/(max_travel_time_2-min_travel_time_2) + 0.5);")


        tx.run("MATCH(n1)-[r:CROSS_THE_ROAD]-(n2:CrossNode) with max(r.travel_time_1) as max_travel_time_1, min(r.travel_time_1) as min_travel_time_1, max(r.travel_time_2) as max_travel_time_2, min(r.travel_time_2) as min_travel_time_2 MATCH(n3)-[r1:CROSS_THE_ROAD]-(n4:CrossNode) with max_travel_time_1, min_travel_time_1,  max_travel_time_2, min_travel_time_2, r1, n3 set r1.cost = (0.5*(r1.travel_time_1-min_travel_time_1)/(max_travel_time_1-min_travel_time_1) + 0.5*(n3.danger-1)/4) + 0.5;")

        result = tx.run("MATCH(n1)-[r:CROSS_THE_ROAD]-(n2:CrossWay) with max(r.travel_time_1) as max_travel_time_1, min(r.travel_time_1) as min_travel_time_1, max(r.travel_time_2) as max_travel_time_2, min(r.travel_time_2) as min_travel_time_2 MATCH(n3)-[r1:CROSS_THE_ROAD]-(n4:CrossWay) with max_travel_time_1, min_travel_time_1,  max_travel_time_2, min_travel_time_2, r1, n3 set r1.cost = (0.5*(r1.travel_time_1-min_travel_time_1)/(max_travel_time_1-min_travel_time_1) + 0.5*(n3.danger-1)/4) + (0.5*(r1.travel_time_2-min_travel_time_2)/(max_travel_time_2-min_travel_time_2) + 0.5);")

        return result.values()




    def create_projections(self):
        """Generate a projection of the graph, which will be used to perform the routing"""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_projections)
            return result

    
    @staticmethod
    def _create_projections(tx):

        result = tx.run("""
                call gds.graph.project('routes_generic', ['BicycleLane', "Crossing", "Footway"], 
                ['CONTINUE_ON_LANE', 'CROSS_THE_ROAD', 'CONTINUE_ON_FOOTWAY', 'CONTINUE_ON_LANE_BY_CROSSING_ROAD', 'CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD'], 
                {relationshipProperties: 'cost'});
                """)

        return result.values()




def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the name of your destination""",
                        required=True)
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
   
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Set weights on general graph relationships"""
    greeter.set_weights()
    print("Setting the relationships weight for the routing : done")

    """Generate a projection of the graph, which will be used to perform the routing"""
    greeter.create_projections()
    print("Create graph projections : done")

    return 0


if __name__ == "__main__":
    main()