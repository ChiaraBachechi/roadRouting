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
        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CONTINUE_ON_FOOTWAY*2..4]->(f) 
                            delete r  
                        """)

        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CONTINUE_ON_LANE*0..4]->(bi:BicycleLane)-[CONTINUE_ON_FOOTWAY*1..4]->(f:Footway) 
                            delete r  
                        """)

        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*0..4]-(bi:BicycleLane)-
                            [r1:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(fi:Footway)-
                            [CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*0..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)

        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*1..4]-(fi:Footway)-
                            [r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(fi2:Footway)-[CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*0..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)

        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[*1..4]-(fi:Footway)-
                            [r1:CONTINUE_ON_CLOSE_LANE_BY_CROSSING_ROAD]->(bi:BicycleLane)-[CONTINUE_ON_FOOTWAY|CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD*1..4]->(f)
                            where r.length > r1.length 
                            delete r 
                        """)

        result = tx.run("""
                            match (b:BicycleLane)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f:Footway) 
                            with b, r, f match (b)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(f)
                            delete r 
                        """)

        
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
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    start_time = time.time()
    greeter.filter_connections_between_cicleways_and_footways()
    print("Filtering connections between cicleways and footways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    
    return 0


main()