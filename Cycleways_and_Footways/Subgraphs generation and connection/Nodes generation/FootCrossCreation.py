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

    def connect_junctions_to_footways(self, file):
        """Connect street nodes with their corresponding footway"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_footways, file)
            return result

    @staticmethod
    def _connect_junctions_to_footways(tx, file):
        tx.run("""
                call apoc.load.json($file) yield value as value with value.data as data 
                unwind data as record match(f:Footway) where f.id_num = record.id_num set f.foot_crosses = record.foot_cross;
                """, file=file)

        tx.run("""
                match(f:Footway) unwind f.foot_crosses as foot_cross with f, foot_cross match(fc:JunctionFootCross) 
                where fc.id = apoc.convert.toString(foot_cross) with f, fc merge (f)-[:CONTAINS]->(fc);
                """)
        
        tx.run("""
                match(f:Footway)-[:CONTAINS]->(fc:JunctionFootCross) with f, fc merge (fc)-[:IS_CONTAINED]->(f);  
                """)



    def connect_junctions_to_crossings(self, file):
        """Connect street nodes with their corresponding crossing (both node and way)"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_crossings, file)
            return result


    @staticmethod
    def _connect_junctions_to_crossings(tx, file):
        tx.run("""
                call apoc.load.json("crossing_ways.json") yield value as value with value.data as data 
                unwind data as record match(b:CrossWay) where b.id_num = record.id_num set b.junction_crosses = record.junction_cross;
                """, file=file)

        tx.run("""
                match(cw:CrossWay) unwind cw.junction_crosses as junction_cross with cw, junction_cross match(fc:JunctionFootCross) 
                where fc.id = apoc.convert.toString(junction_cross) with cw, fc merge(cw)-[:CONTAINS]->(fc); 
                """)
        
        tx.run("""
                match(cw:CrossWay)-[:CONTAINS]->(fc:JunctionFootCross) with cw, fc merge (fc)-[:IS_CONTAINED]->(cw); 
                """)

        tx.run("""
                match(cn:CrossNode) with cn match(fc:JunctionFootCross) where cn.osm_id = "node/" + fc.id merge (cn)<-[:IS_MAPPED]-(fc); ; 
                """)

        result = tx.run("""
                match(cn:CrossNode)<-[:IS_MAPPED]-(fc:JunctionFootCross) with cn, fc merge (cn)-[:IS_MAPPED]->(fc);
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
                MATCH(fc:JunctionFootCross)-[:IS_CONTAINED]->(f:Footway)
                remove fc:JunctionFootCross
                set fc:FootCross;
                """)

        tx.run("""
                MATCH(fc:FootCross)-[:IS_MAPPED]->(cn:CrossNode) remove fc:FootCross set fc:JunctionFootCross;
                """)

        tx.run("""
                MATCH(jbk:JunctionFootCross) WHERE NOT EXISTS((jbk)-[:IS_CONTAINED]-()) 
                REMOVE jbk:JunctionFootCross set jbk:RoadFootJunction, jbk.id = "roadfoot/" + jbk.id;
                """)
        
        tx.run("""
                MATCH(fc:FootCross)-[r:FOOT_ROUTE]->(fc1:FootCross) where not exists((fc1)-->(fc)) 
                merge (fc1)-[r1:FOOT_ROUTE]->(fc) on create set r1.distance = r.distance; 
                """)

        tx.run("""
                match(n:JunctionFootCross) set n.id = "junctionfoot/"+n.id;
                """)

        tx.run("""
                match(n:FootCross) set n.id = "foot/"+n.id;
                """)


        tx.run("""
                drop index junction_footcross_index;
                """)
        
        tx.run("""
                create index junction_footcross_index for (jfc:JunctionFootCross) on (jfc.id);
                """)

        tx.run("""
                create index footcross_index for (fc:FootCross) on (fc.id);
                """)

        result = tx.run("""
                create index road_footcross_index for (rfj:RoadFootJunction) on (rfj.id);
                """)

        return result.values()



    def connect_to_road_junctions(self):
        """Connect the street nodes to the junctions nodes of the Road Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_to_road_junctions)
            return result

   
    @staticmethod
    def _connect_to_road_junctions(tx):
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
                match(n:FootCross) with collect(n) as crossnodes UNWIND crossnodes AS cn 
                CALL spatial.addNode('spatial', cn) yield node return node; 
                """)

        tx.run("""
                match(n:JunctionFootCross) with collect(n) as crossnodes UNWIND crossnodes AS cn 
                CALL spatial.addNode('spatial', cn) yield node return node; 
                """)

        result = tx.run("""
                match(n:RoadFootJunction) with collect(n) as crossnodes UNWIND crossnodes AS cn 
                CALL spatial.addNode('spatial', cn) yield node return node;
                """)

        return result.values()        



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
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
                        required=True)
    parser.add_argument('--nameFileCrossingWays', '-fcw', dest='file_name_crossing_ways', type=str,
                        help="""Insert the name of the .json file containing the crossing ways.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Connect street nodes with the corresponding footways"""
    greeter.connect_junctions_to_footways(options.file_name_footways)
    print("Connecting junction bike cross to cycleways : done")

    """Connect street nodes with the corresponding crossings"""
    greeter.connect_junctions_to_crossings(options.file_name_crossing_ways)
    print("Connecting junction bike cross to crossings : done")

    """Change the label of the street nodes according to which element they are within"""
    greeter.change_of_labels()
    print("Change the labels of JunctionBikeCross in BikeCross : done")

    """Connect subgraph footways layer to the Road junction layer"""
    greeter.connect_to_road_junctions()
    print("Connect foot and road foot cross to road junctions : done")

    """Import subgraph cycleways layer nodes in the Neo4j Spatial Layer"""
    greeter.import_footcrosses_into_spatial_layer()
    print("Import the bike crosses into the spatial layer : done")



    return 0


main()