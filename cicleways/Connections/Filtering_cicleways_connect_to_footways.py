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

    def filter_connections_between_cicleways_and_footways(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._filter_connections_between_cicleways_and_footways)
            return result

    @staticmethod
    def _filter_connections_between_cicleways_and_footways(tx):
        #remove the direct connection between a cycleway node and a footway node 
        #if there exists a connection through a footway in 2 to 4 hops
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CONTINUE_ON_FOOTWAY*2..4]->(f) 
                            delete r  
                        """)
        #remove the direct connection between a cycleway node and a footway node 
        #if there exists a connection between the cycleway and the footway through 0 to 4 cycleway nodes and 1 to 4 footways
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CONTINUE_ON_LANE*0..4]->(bi:BicycleLane)-[CONTINUE_ON_FOOTWAY*1..4]->(f:Footway) 
                            delete r  
                        """)
        #remove the direct connection between a cycleway node and a close footway node 
        #if there exists a connection between the cycleway and the footway through 0 to 4 cycleway nodes, 1 close footway, and 0 to 4 footways
        #only in the case where the distance from the close footway is higher than the lenght of the relationship of the 1 close footway
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*0..4]-(bi:BicycleLane)-
                            [r1:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(fi:Footway)-
                            [CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*0..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)
        
        #remove the direct connection between a cycleway node and a close footway node 
        #if there exists a connection between the cycleway and the footway through 1 to 4 footway nodes, 1 footway by crossing the road, and 0 to 4 footways
        #only in the case where the distance from the close footway is higher than the lenght of the relationship of the 1 footway reachable by corssing the road
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*1..4]-(fi:Footway)-
                            [r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(fi2:Footway)-[CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*0..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)
        #remove the direct connection between a cycleway node and a close footway node 
        #if there exists a connection between the cycleway and the footway through 1 to 4 footway nodes, 1 cycleway by crossing the road, and 1 to 4 footways
        #only in the case where the distance from the close footway is higher than the lenght of the relationship of the 1 cycleway reachable by crossing the road
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*1..4]-(fi:Footway)-
                            [r1:CONTINUE_ON_CLOSE_LANE_BY_CROSSING_ROAD]->(bi:BicycleLane)-[CONTINUE_ON_FOOTWAY|CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD*1..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)
        #remove the direct connection between a cycleway node and a close footway node 
        #if there exists a connection between the cycleway and the footway by crossing the road
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(f)
                            delete r 
                        """)

        #remove the direct connection between a footway node and a close cycleway node 
        #if there exists a connection between the cycleway and the footway by crossing the road whose size is 0
        result = tx.run("""
                            match(f:Footway)-[r:CONTINUE_ON_CLOSE_LANE_BY_CROSSING_ROAD]->(b:BicycleLane)
                            with f, r, b match p=(b)-[:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f) where size(relationships(p)) = 0 
                            delete r
                        """)

        return result
    



def add_options():
    parser = argparse.ArgumentParser(description='Filter connections between cicleways.')
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
    #connect to the neo4j instance
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    #remove some connections between cycleways and footways that appear to be not necessary
    start_time = time.time()
    greeter.filter_connections_between_cicleways_and_footways()
    print("Filtering connections between cicleways and footways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    
    return 0


main()
