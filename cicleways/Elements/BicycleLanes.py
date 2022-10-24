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


    def import_bicycle_lanes(self, file):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_bicycle_lanes, file)
            return result

    @staticmethod
    def _import_bicycle_lanes(tx, file):
        #insert the cycleways in the local file as nodes into the graph database
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record
                        MERGE(n:BicycleLane {id_num : record.id_num}) ON CREATE SET n.osm_id = record.id, n.ID_E = record.ID_E, 
                        n.geometry = record.geometry, 
                        n.highway=record.highway, n.bicycle=record.bicycle, n.foot=record.foot, 
                        n.lanes=record.lanes, n.cycleway=record.cycleway, n.segregated=record.segregated,
                        n.classifica=record.classifica, n.touched_lanes = record.touched_lanes, n.length = record.length
                    """, file=file)

        return result.values()


    def import_lanes_in_spatial_layer(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._import_lanes_in_spatial_layer)
            return result

    @staticmethod
    def _import_lanes_in_spatial_layer(tx):
        #emplying Neo4j spatial inserting the cycleway nodes in the spatial layer
        result = tx.run("""
                       match(n:BicycleLane) with collect(n) as lanes UNWIND lanes AS l CALL spatial.addNode('spatial', l) yield node return node
        """)
                        
        return result.values()


    def add_index(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._add_index)
            return result

    @staticmethod
    def _add_index(tx):
        result = tx.run("""
                       create index cicleway_index for (n:BicycleLane) on (n.id_num)
        """)
                        
        return result.values()    



    def find_intersected_lanes(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_intersected_lanes)
            return result

    @staticmethod
    def _find_intersected_lanes(tx):
        #find cycleways geometries that intersect and connceted them with the CONTINUE_ON_LANE relationship
        result = tx.run("""
                    match(n:BicycleLane) with collect(n) as lanes UNWIND lanes as l 
                    call spatial.intersects('spatial', l.geometry) yield node UNWIND node as p 
                    match(n:BicycleLane) where n.id_num=l.id_num AND p.id_num <> n.id_num AND p:BicycleLane 
                    merge(n)-[r:CONTINUE_ON_LANE]->(p) on create set r.lane_length = n.length , r.tot_dist = n.tot_dist
                    return p, n
        
        """)

        return result.values()


    def find_touched_lanes(self):
         with self.driver.session() as session:
            result = session.write_transaction(self._find_touched_lanes)
            return result

    
    @staticmethod
    def _find_touched_lanes(tx):
        #exploit the information obtained through geopandas regarding the cycleways that have only one point in common
        #generating the 'CONTINUE_ON_LANE' relationship
        result = tx.run("""
                match(n:BicycleLane) where NOT isEmpty(n.touched_lanes) unwind n.touched_lanes as cicleway match(n1:BicycleLane) 
                where n1.id_num=cicleway 
                merge (n)-[r:CONTINUE_ON_LANE]->(n1) on create set r.lane_length = n.length, r.tot_dist = n.length return n, n1
        """)
        #and NOT isEmpty(n1.touched_lanes) and n.geometry <> n1.geometry 
        #result = tx.run("""
        #    match(n:BicycleLane) remove n.touched_lanes; 
        #""")

        return result

    
    def find_closest_lanes(self, file):
         with self.driver.session() as session:
            result = session.write_transaction(self._find_closest_lanes, file)
            return result


    
    @staticmethod
    def _find_closest_lanes(tx, file):
        #exploit the information obtained through geopandas regarding the cycleways that are located nearby
        #generating the 'CONTINUE_ON_LANE_BY_CROSSING_ROAD' relationship
        result = tx.run("""
            call apoc.load.json($file) yield value as value with value.data as data 
            unwind data as record match (b:BicycleLane) where b.id_num = record.id_num and NOT isEmpty(record.closest_lanes)
            UNWIND record.closest_lanes as lane with b, lane match (b1:BicycleLane) where b1.id_num = lane[0] 
            merge (b)-[r:CONTINUE_ON_LANE_BY_CROSSING_ROAD]->(b1) on create set r.crossing="yes", r.length = lane[1], 
            r.lane_length = b.length, r.tot_dist = lane[1] + b.length;
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
    #connect to the neo4j instance
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    #importing the cycleways i the graph
    start_time = time.time()
    greeter.import_bicycle_lanes(options.file_name)
    print("import cicleways_total.json: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    #importing the cycleways in the spatial layer
    start_time = time.time()
    greeter.import_lanes_in_spatial_layer()
    print("Import the cicleways in the spatial layer: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    #adding an index on the cycleway identifier
    start_time = time.time()
    greeter.add_index()
    print("Add an index on the id_num : done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    #generate connections between the cycleways that interect
    start_time = time.time()
    greeter.find_intersected_lanes()
    print("Find the intersected lanes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    #generate connections between the cycleways that interect only in a point
    start_time = time.time()
    greeter.find_touched_lanes()
    print("Find the lanes that touches each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    
    #generate connections between the cycleways that are located nearby
    start_time = time.time()
    greeter.find_closest_lanes(options.file_name)
    print('Find the lanes that are close to each other: done')
    print("Execution time : %s seconds" % (time.time() - start_time))

    return 0


main()
