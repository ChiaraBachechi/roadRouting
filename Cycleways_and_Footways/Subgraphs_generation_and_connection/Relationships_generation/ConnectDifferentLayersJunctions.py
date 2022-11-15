from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to connect the cycleways layer with the footways layer"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_junctions_of_different_layers(self):
        """Connect different layers subgraph nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_of_different_layers)
            return result

    @staticmethod
    def _connect_junctions_of_different_layers(tx):
        tx.run("""
                MATCH (bk:BikeCross), (fc:FootCross) WHERE bk.location = fc.location
                with bk, fc
                MERGE (bk)-[r:IS_THE_SAME]->(fc) ON CREATE SET r.distance=0;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[:IS_THE_SAME]->(fc:FootCross) with bk, fc
                MERGE (fc)-[r:IS_THE_SAME]->(bk) ON CREATE SET r.distance=0;
                """)

        tx.run("""
                MATCH (bk:BikeCross), (fc:JunctionFootCross) WHERE bk.location = fc.location
                with bk, fc
                MERGE (bk)-[r:IS_THE_SAME]->(fc) ON CREATE SET r.distance=0;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[r:IS_THE_SAME]->(fc:JunctionFootCross) with bk, fc
                MERGE (fc)-[r:IS_THE_SAME]->(bk) ON CREATE SET r.distance=0;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[r:IS_THE_SAME]->(fc:JunctionFootCross) remove bk:BikeCross
                set bk:JunctionBikeCross;
                """)

        tx.run("""
                MATCH (bk:JunctionBikeCross), (fc:JunctionFootCross) WHERE bk.location = fc.location
                with bk, fc
                MERGE (bk)-[r:IS_THE_SAME]->(fc) ON CREATE SET r.distance=0;
                """)

        result = tx.run("""
                MATCH(bk:JunctionBikeCross)-[:IS_THE_SAME]->(fc:JunctionFootCross) with bk, fc
                MERGE (fc)-[r:IS_THE_SAME]->(bk) ON CREATE SET r.distance=0;
                """)

        return result.values()



    def delete_roadjunctions_with_same_location_of_footcrosses(self):
        """Delete RoadBikeJunction nodes that are mapped as the same elements of FootCross and JunctionFootCross nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._delete_roadjunctions_with_same_location_of_footcrosses)
            return result


    @staticmethod
    def _delete_roadjunctions_with_same_location_of_footcrosses(tx):

        tx.run("""
                MATCH(fc:FootCross)-[:IS_CONTAINED]->(f:Footway) with fc MATCH(rj:RoadBikeJunction) 
                where rj.location = fc.location remove fc:FootCross set fc:JunctionFootCross;
                """)

                
        tx.run("""
                MATCH(jfc:JunctionFootCross)-[:IS_CONTAINED]->(n) with jfc MATCH(rj:RoadBikeJunction) 
                where rj.location = jfc.location detach delete rj;
                """)
                
        tx.run("""
                MATCH(jbc:JunctionBikeCross)-[:IS_THE_SAME]->(jfc:JunctionFootCross) where not
                exists((jbc)-[:IS_CONTAINED]-()) detach delete jbc;
                """)

                



    def change_labels(self):
        """Add the label Junction to all the subgraph nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._change_labels)
            return result

    
    @staticmethod
    def _change_labels(tx):
        tx.run("""
                match(n:BikeCross) remove n:BikeCross set n:Junction:BikeCross;                       
                """)

        tx.run("""
                match(n:FootCross) remove n:FootCross set n:Junction:FootCross;
                """)
        
        tx.run("""
                match(n:JunctionBikeCross) remove n:JunctionBikeCross set n:Junction:JunctionBikeCross;
                """)
        
        tx.run("""
                match(n:JunctionFootCross) remove n:JunctionFootCross set n:Junction:JunctionFootCross;
                """)

        tx.run("""
                match(n:RoadBikeJunction) remove n:RoadBikeJunction set n:Junction:RoadBikeJunction; 
                """)

        tx.run("""
                match(n:RoadFootJunction) remove n:RoadFootJunction set n:Junction:RoadFootJunction;
                """)

        result = tx.run("""
                match(n:RoadJunction) remove n:RoadJunction set n:Junction:RoadJunction;
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

    """Connect different layers through their subgraphs"""
    greeter.connect_junctions_of_different_layers()
    print("Connect junctions of different layers : done")

    """Delete RoadBikeJunction nodes that are mapped as the same elements of FootCross and JunctionFootCross nodes"""
    greeter.delete_roadjunctions_with_same_location_of_footcrosses()
    print("Delete the road junctions that have the same location of footways that are not linked to cycleways : done")

    """Add the label Junction to all the subgraph nodes"""
    greeter.change_labels()
    print("Make a final change of labels : done")

    return 0


if __name__ == "__main__":
    main()