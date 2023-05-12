from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how subgraph footways layer nodes are generated"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_junctions_to_footways(self):
        """Connect street nodes with their corresponding footway"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_footways)
            return result

    @staticmethod
    def _connect_junctions_to_footways(tx):

        tx.run("""
                match(f:Footway) unwind f.nodes as foot_cross with f, foot_cross match(fc:FootNode) 
                where fc.id = apoc.convert.toString(foot_cross) with f, fc merge (f)-[:CONTAINS]->(fc) set fc:FootJunction;
                """)
        return



    def connect_junctions_to_crossings(self):
        """Connect street nodes with their corresponding crossing (both node and way)"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_crossings)
            return result


    @staticmethod
    def _connect_junctions_to_crossings(tx):
        result = tx.run("""
                match(cw:CrossWay) unwind cw.nodes as junction_cross with cw, junction_cross match(fc:FootNode) 
                where fc.id = apoc.convert.toString(junction_cross) with cw, fc merge(cw)-[:CONTAINS]->(fc) set fc:FootCrossing; 
                """)
        
        tx.run("""
                match(cn:CrossNode) with cn match(bk:FootNode) where cn.osm_id = "node/" + bk.id merge (cn)-[:CONTAINS]->(bk) set bk:FootCrossing;
                """)

        return result.values()

    def change_of_labels(self):
        """Change the label of the street nodes connected to Footways and Crossings and set new indexes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._change_of_labels)
            return result

    @staticmethod
    def _change_of_labels(tx):
        tx.run("""
                MATCH (bk:FootJunction)<-[:CONTAINS]-(cn:CrossNode) remove bk:FootJunction;
                """)
                
        tx.run("""
                MATCH (bk:FootJunction)<-[:CONTAINS]-(cn:CrossWay) remove bk:FootJunction;
                """)

        tx.run("""
                MATCH (jbk:FootNode) WHERE NOT EXISTS(()-[:CONTAINS]->(jbk)) set jbk:FootRoad;
                """)

        return


    def createIndexes(self):
        """Create new indexes for the subgraph"""
        with self.driver.session() as session:
            result = session.write_transaction(self._createIndexes)
            return result

    @staticmethod
    def _createIndexes(tx):

        tx.run("""
                        drop index junction_footcross_index;
                        """)

        tx.run("""
                        create index footcross_index for (jfc:FootCrossing) on (jfc.id);
                        """)

        tx.run("""
                        create index footjunction_index for (fc:FootJunction) on (fc.id);
                        """)

        result = tx.run("""
                        create index footroad_index for (rfj:FootRoad) on (rfj.id);
                        """)

        return result



    def connect_to_road_junctions(self):
        """Connect the street nodes to the junctions nodes of the Road Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_to_road_junctions)
            return result

   
    @staticmethod
    def _connect_to_road_junctions(tx):
        """DA RIVEDERE COMPLETAMENTE"""
        tx.run("""
                MATCH(rbj:RoadFootJunction), (rj:RoadJunction) where rbj.id = rj.id merge (rbj)-[:IS_THE_SAME]-(rj);
                """)

        tx.run("""
                MATCH(rfj:RoadFootJunction)-[r:FOOT_ROUTE]->(rfj1:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rfj)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(rfj:RoadFootJunction)<-[r:FOOT_ROUTE]-(rfj1:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(rfj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(fc:FootCross)-[r:FOOT_ROUTE]->(rfj:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (bk)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(fc:FootCross)<-[r:FOOT_ROUTE]-(rfj:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(fc) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(jfc:JunctionFootCross)-[r:FOOT_ROUTE]->(rfj:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (jfc)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(jfc:JunctionFootCross)<-[r:FOOT_ROUTE]-(rfj:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(bk) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(fc:FootCross)-[r:ROUTE]-(rj:RoadJunction) 
                where not exists((rj)-->(fc)) merge (rj)-[r1:FOOT_ROUTE]-(fc) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(fc:JunctionFootCross)-[r:ROUTE]-(rj:RoadJunction) 
                where not exists((rj)-->(fc)) merge (rj)-[r1:FOOT_ROUTE]->(fc) on create set r1.distance = r.distance;
                """)               

        tx.run("""
                MATCH(rfj:RoadFootJunction)-[:IS_THE_SAME]->(rj:RoadJunction) detach delete rfj;
                """)

        result = tx.run("""
                MATCH(f:Footway)-[:CONTAINS]-(n)-[:FOOT_ROUTE|ROUTE *1..3]-(n1)<-[:CONTAINS]-(f1:Footway) 
                where f.id_num <> f1.id_num and not exists((f)-[*1..3]-(f1)) 
                merge (f)-[:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(f1);
                """)

        return result


    def import_footcrosses_into_spatial_layer(self):
        """Import subgraph footways layer nodes in a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_footcrosses_into_spatial_layer)
            return result

    
    @staticmethod
    def _import_footcrosses_into_spatial_layer(tx):
        tx.run("""
                match(n:FootCross) where not "BikeNode" in labels(n)
                CALL spatial.addNode('spatial', n) yield node return node; 
                """)

        tx.run("""
                match(n:FootJunction) where not "BikeNode" in labels(n) 
                CALL spatial.addNode('spatial', n) yield node return node; 
                """)

        result = tx.run("""
                match(n:FootRoad) where not "BikeNode" in labels(n)  
                CALL spatial.addNode('spatial', n) yield node return node;
                """)

        return        



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

    """Connect street nodes with the corresponding footways"""
    greeter.connect_junctions_to_footways()
    print("Connecting junction bike cross to cycleways : done")

    """Connect street nodes with the corresponding crossings"""
    greeter.connect_junctions_to_crossings()
    print("Connecting junction bike cross to crossings : done")

    """Change the label of the street nodes according to which element they are within"""
    greeter.change_of_labels()
    print("Change the labels of JunctionBikeCross in BikeCross : done")

    """Create new indexes on the subgraph to speed up the search"""
    greeter.createIndexes()
    print("Create new indexes in the subgraph : done")

    """Connect subgraph footways layer to the Road junction layer"""
    #greeter.connect_to_road_junctions()
    print("Connect foot and road foot cross to road junctions : done")

    """Import subgraph cycleways layer nodes in the Neo4j Spatial Layer"""
    greeter.import_footcrosses_into_spatial_layer()
    print("Import the bike crosses into the spatial layer : done")



    return 0


if __name__ == "__main__":
    main()