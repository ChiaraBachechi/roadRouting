from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how subgraph cycleways layer nodes are generated"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_junctions_to_cycleways(self):
        """Connect street nodes with their corresponding cycleway"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_cycleways)
            return result

    @staticmethod
    def _connect_junctions_to_cycleways(tx):

        tx.run("""
                match(b:BicycleLane) unwind b.nodes as bike_cross with b, bike_cross match (bk:BikeNode) 
                where bk.id = apoc.convert.toString(bike_cross) with b, bk merge (b)-[:CONTAINS]->(bk) set bk:BikeJunction;
                """)
        return


    def connect_junctions_to_crossings(self):
        """Connect street nodes with their corresponding crossing (both node and way)"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_junctions_to_crossings)
            return result

    @staticmethod
    def _connect_junctions_to_crossings(tx):

        tx.run("""
                match(cw:CrossWay) unwind cw.nodes as junction_cross with cw, junction_cross match (bk:BikeNode) 
                where bk.id = apoc.convert.toString(junction_cross) with cw, bk merge (cw)-[:CONTAINS]->(bk) set bk:BikeCrossing; 
                """)

        tx.run("""
                match(cn:CrossNode) with cn match(bk:BikeNode) where cn.osm_id = "node/" + bk.id merge (cn)-[:CONTAINS]->(bk) set bk:BikeCrossing; 
                """)
        return

    def change_of_labels(self):
        """Change the label of the street nodes connected to Cycleways and Crossings and set new indexes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._change_of_labels)
            return result

    @staticmethod
    def _change_of_labels(tx):
       
        tx.run("""
                MATCH (bk:BikeJunction)<-[:CONTAINS]-(cn:CrossNode) remove bk:BikeJunction;
                """)
                
        tx.run("""
                MATCH (bk:BikeJunction)<-[:CONTAINS]-(cn:CrossWay) remove bk:BikeJunction;
                """)

        tx.run("""
                MATCH (jbk:BikeNode) WHERE NOT EXISTS(()-[:CONTAINS]->(jbk)) set jbk:BikeRoad;
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
                        drop index junction_bikecross_index;
                        """)

        tx.run("""
                        create index bikecross_index for (jbk:BikeCrossing) on (jbk.id);
                        """)

        tx.run("""
                        create index bikejunction_index for (bk:BikeJunction) on (bk.id);
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
                MATCH(rbj:RoadBikeJunction), (rj:RoadJunction) where rbj.id = rj.id merge (rbj)-[:IS_THE_SAME]-(rj);
                """)

        tx.run("""
                MATCH(rbj:RoadBikeJunction)-[r:BIKE_ROUTE]->(rbj1:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rbj)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(rbj:RoadBikeJunction)<-[r:BIKE_ROUTE]-(rbj1:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(rbj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[r:BIKE_ROUTE]->(rbj:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (bk)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;

                """)

        tx.run("""
                MATCH(bk:BikeCross)<-[r:BIKE_ROUTE]-(rbj:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(bk) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(bk:JunctionBikeCross)-[r:BIKE_ROUTE]->(rbj:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (bk)-[r1:ROUTE]-(rj) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(bk:JunctionBikeCross)<-[r:BIKE_ROUTE]-(rbj:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) 
                MERGE (rj)-[r1:ROUTE]-(bk) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(bk:BikeCross)-[r:ROUTE]-(rj:RoadJunction) 
                where not exists((rj)-->(bk)) merge (rj)-[r1:BIKE_ROUTE]->(bk) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(bk:JunctionBikeCross)-[r:ROUTE]-(rj:RoadJunction) 
                where not exists((rj)-->(bk)) merge (rj)-[r1:BIKE_ROUTE]->(bk) on create set r1.distance = r.distance;
                """)

        tx.run("""
                MATCH(rbj:RoadBikeJunction)-[:IS_THE_SAME]->(rj:RoadJunction) detach delete rbj;
                """)

        result = tx.run("""
                MATCH(bl:BicycleLane)-[:CONTAINS]-(n)-[:BIKE_ROUTE|ROUTE *1..3]-(n1)<-[:CONTAINS]-(bl1:BicycleLane) 
                where bl.id_num <> bl1.id_num and not exists((bl)-[*1..3]-(bl1)) 
                merge (bl)-[:CONTINUE_ON_LANE_BY_CROSSING_ROAD]-(bl1);
                """)

        return result 

        


    def import_bikecrosses_into_spatial_layer(self):
        """Import subgraph cycleways layer nodes in a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_bikecrosses_into_spatial_layer)
            return result

    
    @staticmethod
    def _import_bikecrosses_into_spatial_layer(tx):
        tx.run("""
                match (n:BikeCross)
                CALL spatial.addNode('spatial', n) yield node return node;
                """)

        tx.run("""
                match (n:BikeJunction)
                CALL spatial.addNode('spatial', n) yield node return node; 
                """)

        result = tx.run("""
                match (n:BikeRoad)
                CALL spatial.addNode('spatial', n) yield node return node;
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

    """Connect street nodes with the corresponding cycleways"""
    #greeter.connect_junctions_to_cycleways()
    print("Connecting junction bike cross to cycleways : done")

    """Connect street nodes with the corresponding crossings"""
    #greeter.connect_junctions_to_crossings()
    print("Connecting junction bike cross to crossings : done")

    """Change the label of the street nodes according to which element they are within"""
    greeter.change_of_labels()
    print("Change the labels of JunctionBikeCross in BikeCross : done")

    """Create new indexes on the subgraph to speed up the search"""
    greeter.createIndexes()
    print("Create new indexes in the subgraph : done")

    """Connect subgraph cycleways layer to the Road junction layer"""
    #greeter.connect_to_road_junctions()
    print("Connect bike cross and road bike cross junctions to road junctions and delete road bike junctions : done")

    """Import subgraph cycleways layer nodes in the Neo4j Spatial Layer"""
    greeter.import_bikecrosses_into_spatial_layer()
    print("Import the bike crosses into the spatial layer : done")



    return 0


if __name__ == "__main__":
    main()