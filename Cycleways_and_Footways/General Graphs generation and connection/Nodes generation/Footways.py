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


    def import_footways(self, file):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_footways, file)
            return result

    @staticmethod
    def _import_footways(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MERGE(n:Footway {id_num : "foot/" + record.id_num}) 
                        ON CREATE SET n.osm_id = record.id, n.geometry = record.geometry, n.touched_lanes = record.touched_lanes, 
                        n.touched_footways = record.touched_footways, 
                        n.bicycle=record.bicycle, n.bus=record.bus, n.crossing=record.crossing, 
                        n.cycleway=record.cycleway, n.kerb=record.kerb, n.length = record.length, n.highway = record.highway;
                """, file=file)

        return result.values()


    def import_footways_in_spatial_layer(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._import_footways_in_spatial_layer)
            return result

    @staticmethod
    def _import_footways_in_spatial_layer(tx):
        result = tx.run("""
                       match(n:Footway) with collect(n) as footway UNWIND footway AS fw 
                       CALL spatial.addNode('spatial', fw) yield node return node
        """)
                        
        return result.values()


    def add_index(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._add_index)
            return result


    @staticmethod
    def _add_index(tx):
        result = tx.run("""
                       create index footway_index for (n:Footway) on (n.id_num)
        """)
                        
        return result.values()




    def find_touched_footways(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._find_touched_footways)
            return result

    
    @staticmethod
    def _find_touched_footways(tx):
        result = tx.run("""
                match(n:Footway) where NOT isEmpty(n.touched_footways) unwind n.touched_footways as foot 
                match(n1:Footway) where n1.id_num="foot/" + foot and NOT isEmpty(n1.touched_footways) 
                and n.geometry <> n1.geometry merge (n)-[r:CONTINUE_ON_FOOTWAY]->(n1); 
        """)


        #result = tx.run(""" 
        #        match (n:Footway) remove n.touched_footways
        #""")
        return result




    def find_closest_footways(self, file):
         with self.driver.session() as session:
            result = session.write_transaction(self._find_closest_footways, file)
            return result

    
    @staticmethod
    def _find_closest_footways(tx, file):
        result = tx.run("""
                call apoc.load.json($file) yield value as value with value.data as data 
                UNWIND data as record match (f:Footway) where f.id_num = "foot/" + record.id_num and NOT isEmpty(record.closest_footways)
                UNWIND record.closest_footways as foot with f, foot match (f1:Footway) where f1.id_num = "foot/" + foot[0] 
                merge (f)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1) on create set r.length = foot[1]; 
        """, file=file)

        return result





    

def add_options():
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
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    start_time = time.time()
    greeter.import_footways(options.file_name)
    print("import footways_total.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    start_time = time.time()
    greeter.import_footways_in_spatial_layer()
    print("Import the footways in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    start_time = time.time()
    greeter.add_index()
    print("Add index on id_num : done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    start_time = time.time()
    greeter.find_touched_footways()
    print("Connect the footways that touches each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    start_time = time.time()
    greeter.find_closest_footways(options.file_name)
    print("Connect the footways that are close to each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    

    return 0


main()