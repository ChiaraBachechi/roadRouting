from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to generate nodes referring to cycling paths"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_bicycle_lanes(self, file):
        """Import cycling paths data on Neo4j and generate BicycleLane nodes"""

        with self.driver.session() as session:
            result = session.write_transaction(self._import_bicycle_lanes, file)
            return result

    @staticmethod
    def _import_bicycle_lanes(tx, file):
        tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MATCH (n:Footway {osm_id : record.id}) 
                        SET n:BicycleLane, n.touched_lanes = record.touched_lanes;
                """, file=file)
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record
                        MERGE(b:BicycleLane {osm_id : record.id}) ON CREATE SET b.osm_id = record.id, b.ID_E = record.ID_E, 
                        b.geometry = record.geometry, b.id_num = 'cycleway/' + apoc.convert.toString(record.id_num),
                        b.highway=record.highway, b.bicycle=record.bicycle, b.foot=record.foot, 
                        b.lanes=record.lanes, b.cycleway=record.cycleway, b.segregated=record.segregated,
                        b.classifica=record.classifica, b.touched_lanes = record.touched_lanes, 
                        b.length = record.length,
                        b.danger = record.pericolosità, b.bike_crosses = record.bike_cross, b.nodes = record.nodes, b.bike_road_junction = record.bike_road_junction,
                        b.road_junction = record.road_junction;
                    """, file=file)

        return result.values()


    def import_lanes_in_spatial_layer(self):
        """Import BicycleLane nodes on a Neo4j spatial layer """

        with self.driver.session() as session:
            result = session.write_transaction(self._import_lanes_in_spatial_layer)
            return result

    @staticmethod
    def _import_lanes_in_spatial_layer(tx):
        tx.run("""
                call spatial.addWKTLayer('spatial', 'geometry')
                """)
        result = tx.run("""
                       match(b:BicycleLane) with collect(b) as lanes UNWIND lanes AS l CALL spatial.addNode('spatial', l) yield node return node
        """)
                        
        return result.values()


    def add_index(self):
        """Add an index to the numeric id"""

        with self.driver.session() as session:
            result = session.write_transaction(self._add_index)
            return result

    @staticmethod
    def _add_index(tx):
        result = tx.run("""create index cycleway_index for (b:BicycleLane) on (b.id_num)""")
        return result.values()    



    def generate_relationships_touched_lanes(self):
        """Generate relationships between nodes representing cycleways that touch or intersect"""

        with self.driver.session() as session:
            result = session.write_transaction(self._generate_relationships_touched_lanes)
            return result

    
    @staticmethod
    def _generate_relationships_touched_lanes(tx):
        tx.run("""
                match(b:BicycleLane) where NOT isEmpty(b.touched_lanes) unwind b.touched_lanes as cycleway match(b1:BicycleLane) 
                where b1.osm_id = cycleway
                merge (b)-[r:CONTINUE_ON_LANE]->(b1)
                merge (b1)-[r1:CONTINUE_ON_LANE]->(b)
        """)

        result = tx.run("""
                match(b:BicycleLane)-[r:CONTINUE_ON_LANE]->(b1:BicycleLane) where b.id_num = b1.id_num
                delete r
        """)
        #and NOT isEmpty(n1.touched_lanes) and n.geometry <> n1.geometry 
        #result = tx.run("""
        #    match(n:BicycleLane) remove n.touched_lanes; 
        #""")

        return result.values()

    
    def generate_relationships_closest_lanes(self,file):
        """Generate relationships between nodes representing cycleways that are reachable by crossing
           the road where the crossing is not signaled
        """

        with self.driver.session() as session:
            result = session.write_transaction(self._generate_relationships_closest_lanes,file)
            return result


    
    @staticmethod
    def _generate_relationships_closest_lanes(tx,file):
        result = tx.run("""
            call apoc.load.json($file) yield value as value with value.data as data 
            unwind data as record match (b:BicycleLane) where b.osm_id = record.id and NOT isEmpty(record.closest_lanes)
            UNWIND record.closest_lanes as lane with b, lane match (b1:BicycleLane) 
            where b1.osm_id = lane[0] and and b.osm_id <> b1.osm_id and not exists((b)-[:CONTINUE_ON_LANE]->(bl))
            merge (b)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1) on create set r.length = lane[1]
            merge (b1)-[r1:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b) on create set r1.length = lane[1];
        """, file = file)

        return result.values()
      


def add_options():
    """Parameters needed to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of Cycleways in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser


def main(args=None):

    """Parsing parameters in input"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Import cycleways data on Neo4j and generate BicycleLane nodes"""
    start_time = time.time()
    greeter.import_bicycle_lanes(options.file_name)
    print("import cycleways_total.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Import BicycleLane nodes on Neo4j Spatial Layer"""
    start_time = time.time()
    greeter.import_lanes_in_spatial_layer()
    print("Import the cycleways in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


    """Add an index on the numeric id attribute"""
    start_time = time.time()
    greeter.add_index()
    print("Add an index on the id_num : done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Generate relationships between nodes representing cycleways that touch or intersect"""
    start_time = time.time()
    greeter.generate_relationships_touched_lanes()
    print("Find the lanes that touches each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Generate relationships between nodes representing cycleways that are reachable by crossing the road where
       the crossing is not signaled
    """
    start_time = time.time()
    greeter.generate_relationships_closest_lanes(options.file_name)
    print('Find the lanes that are close to each other: done')
    print("Execution time : %s seconds" % (time.time() - start_time))

    return 0


if __name__ == "__main__":
    main()