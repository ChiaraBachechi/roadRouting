from neo4j import GraphDatabase
import json
import argparse
import os
import time

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def filter_cicleways_connections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._filter_cicleways_connections)
            return result

    @staticmethod
    def _filter_cicleways_connections(tx):
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[:CONTINUE_ON_LANE*2..4]->(b1) 
                            delete r  
                        """)


        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[r1:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(bi:BicycleLane)-[:CONTINUE_ON_LANE*1..4]->(b1)
                            where r.length > r1.length 
                            delete r 
                        """)


        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[:CONTINUE_ON_LANE*1..4]->(bi:BicycleLane)-[r1:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1)
                            where r.length > r1.length 
                            delete r 
                        """)

        
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[:CONTINUE_ON_LANE*1..4]->(bi:BicycleLane)-[r1:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->
                            (bi2:BicycleLane)-[:CONTINUE_ON_LANE*1..4]->(b1)
                            where r.length > r1.length 
                            delete r 
                        """)


        result = tx.run("""
                           match (b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane)
                           with b, r, b1 match p=(b)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD*2..3]->(b1) 
                           with reduce(sum=0, relation in relationships(p) | sum + relation.length) as result , r 
                           where r.length > result 
                           delete r
                        """)


        result = tx.run("""
                            match(b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(b1) 
                            delete r  
                        """)


        result = tx.run("""
                            match(b:BicycleLane)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1:BicycleLane) 
                            with b, r, b1 match (b)-[r1:CONTINUE_ON_FOOTWAY]->(f:Footway)<-[r2:CONTINUE_ON_FOOTWAY]-(b1) 
                            where r1.crossing="yes" and r2.crossing="yes" delete r  
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
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    start_time = time.time()
    greeter.filter_cicleways_connections()
    print("Filtering connections between cicleways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    
    return 0


main()