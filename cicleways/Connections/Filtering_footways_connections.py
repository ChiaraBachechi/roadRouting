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

    def filter_footways_connections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._filter_footways_connections)
            return result

    @staticmethod
    def _filter_footways_connections(tx):
        result = tx.run("""
                            match (f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                            with f, r, f1 match (f)-[:CONTINUE_ON_FOOTWAY*2..4]->(f1) 
                            delete r  
                        """)

        result = tx.run("""
                            match (f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                            with f, r, f1 match (f)-[r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(fi:Footway)-[:CONTINUE_ON_FOOTWAY*1..4]->(f1)
                            where r.length > r1.length 
                            delete r 
                        """)

        result = tx.run("""
                            match (f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                            with f, r, f1 match (f)-[:CONTINUE_ON_FOOTWAY*1..4]->(fi:Footway)-[r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1)
                            where r.length > r1.length 
                            delete r 
                        """)

        result = tx.run("""
                            match (f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                            with f, r, f1 match (f)-[:CONTINUE_ON_FOOTWAY*1..4]->(fi:Footway)-[r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->
                            (fi2:Footway)-[:CONTINUE_ON_FOOTWAY*1..4]->(f1)
                            where r.length > r1.length 
                            delete r 
                        """)
        
        result = tx.run("""
                           match (f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway)
                           with f, r, f1 match p=(f)-[:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*2..3]->(f1) 
                           with reduce(sum=0, relation in relationships(p) | sum + relation.length) as result , r 
                           where r.length > result delete r
                        """)


        result = tx.run("""
                            match(f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1:Footway) 
                            with f, r, f1 match (f)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(f1) delete r  
                        """)



        return result

    



def add_options():
    parser = argparse.ArgumentParser(description='Filter connections between footways.')
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
    greeter.filter_footways_connections()
    print("Filtering connections between footways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    

    

    return 0


main()