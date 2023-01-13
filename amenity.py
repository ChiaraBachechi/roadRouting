import overpy
import json
from neo4j import GraphDatabase
import argparse
import os


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """get neo4j folder."""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()
        
    def get_import_folder_name(self):
        """get neo4j instance import folder name"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    def import_node(self):
        """import POI nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_node)
            return result

    @staticmethod
    def _import_node(tx):
        result = tx.run("""
                        CALL apoc.load.json("nodefile.json") YIELD value AS value 
                        WITH value.elements AS elements
                        UNWIND elements AS nodo
                        MERGE (n:PointOfInterest {osm_id: nodo.id})-[:MEMBER]->(wn:OSMWayNode {osm_id: nodo.id})
                            ON CREATE SET n.name=nodo.tags.name,wn.lat=tofloat(nodo.lat), 
                                wn.lon=tofloat(nodo.lon), 
                                wn.geometry= 'POINT(' + nodo.lat + ' ' + nodo.lon +')'
                        WITH n, nodo
                        MERGE (n)-[:TAGS]->(t:Tag)
                            ON CREATE SET t += nodo.tags
                        """)
        return result.values()
    def import_node_way(self):
        """import POI nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_node_way)
            return result

    @staticmethod
    def _import_node_way(tx):
        result = tx.run("""
                        CALL apoc.load.json("nodeway.json") YIELD value AS value 
                        WITH value.elements AS elements
                        UNWIND elements AS nodo
                        MERGE (wn:OSMWayNode {osm_id: nodo.id})
                            ON CREATE SET wn.lat=tofloat(nodo.lat), 
                                wn.lon=tofloat(nodo.lon), 
                                wn.geometry='POINT(' + nodo.lat + ' ' + nodo.lon +')'
                        """)
        return result.values()
    def import_way(self):
        """import POI nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_way)
            return result

    @staticmethod
    def _import_way(tx):
        result = tx.run("""
                        CALL apoc.load.json("wayfile.json") YIELD value 
                        with value.elements AS elements
                        UNWIND elements AS way
                        MERGE (w:Way:PointOfInterest {osm_id: way.id}) ON CREATE SET w.name = way.tags.name
                        MERGE (w)-[:TAGS]->(t:Tag) ON CREATE SET t += way.tags
                        WITH w, way.nodes AS nodes
                        UNWIND nodes AS node
                        MATCH (wn:OSMWayNode {osm_id: node})
                        MERGE (w)-[:MEMBER]->(wn)
                        """)
        return result.values()
        
    def import_nodes_into_spatial_layer(self):
        """Import OSMWayNodes nodes in a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_nodes_into_spatial_layer)
            return result
    @staticmethod
    def _import_nodes_into_spatial_layer(tx):
        tx.run("""
                match(n:OSMWayNode)
                CALL spatial.addNode('spatial', n) yield node return node;
                """)
        return result.values()

    def connect_amenity(self):
        """Connect the OSMWayNode of the POI to the nearest Node in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_amenity)

    @staticmethod
    def _connect_amenity(tx):
        result = tx.run("""
                        MATCH (p:OSMWayNode)
                            WHERE NOT (p)-[:ROUTE]->()
                        WITH p, p.location AS poi
                        MATCH (n:RoadJunction)
                            WHERE distance(n.location, poi) < 100
                            AND n <> p
                        WITH n, p, distance(n.location, poi) AS dist ORDER BY dist
                        WITH head(collect(n)) AS nv, p
                        MERGE (p)-[r:ROUTE]->(nv)
                            ON CREATE SET r.distance = distance(nv.location, p.location), r.status = 'active'
                        MERGE (p)<-[ri:ROUTE]-(nv)
                            ON CREATE SET ri.distance = distance(nv.location, p.location), ri.status = 'active'
                    """)
        return result.values()

    def set_location(self):
       """Insert the location in the OSMWayNode."""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_location)
    
    @staticmethod
    def _set_location(tx):
       result = tx.run("""MATCH (n:OSMWayNode) SET n.location = point({latitude: tofloat(n.lat), longitude: tofloat(n.lon)})""")
       return result.values()
       
    def set_index(self):
        """create index on nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_index)
            return result

    @staticmethod
    def _set_index(tx):
        result = tx.run("""
                           create index on :OSMWayNode(osm_id);
                       """)
        result = tx.run("""
                           create index on :PointOfInterest(osm_id);
                       """)
        return result.values()


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
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
                        required=True)
    parser.add_argument('--spatial', '-s', dest='spatial', type=str,
                        help="""True if a neo4j spatial layer is present""",
                        required=False, default = 'False')
    return parser


def main(args=None):
    argParser = add_options()
    #retrieving arguments
    options = argParser.parse_args(args=args)
    #creating an instance of the overpass API
    api = overpy.Overpass()
    #define the bounding circle
    dist = options.dist
    lon = options.lon
    lat = options.lat
    #connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + "\\"
    #query the api for POI ways
    result = api.query(f"""(   
                               way(around:{dist},{lat},{lon})["amenity"];
                           );(._;>;);
                           out body;
                    """)
    #generate a json file with the retrieved information about the nodes that compose each way
    list_node_way = []
    for w in result.ways:
        print(w)
        for n in w.get_nodes(resolve_missing=False):
            d = {'type': 'node', 'id': n.id,
                 'id_way': w.id,
                 'lat': str(n.lat), 
                 'lon': str(n.lon), 
                 'geometry': 'POINT('+str(n.lat) + ' ' + str(n.lon) + ')',
                 'tags': n.tags}
            print(d)
            list_node_way.append(d)
    res = {"elements": list_node_way}
    print("nodes to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + 'nodeway.json', "w") as f:
        json.dump(res, f)
        print("file generated in import directory")
    #import the nodes in the graph as OSMWayNodes
    greeter.import_node_way()
    #generatio of the way file in the import directory
    list_way=[]
    for way in result.ways:
            d = {'type': 'way', 'id': way.id, 'tags': way.tags}
            l_node = []
            for node in way.nodes:
                l_node.append(node.id)
            d['nodes'] = l_node
            list_way.append(d)
    res = {"elements": list_way}
    print("ways to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + "wayfile.json", "w") as f:
        json.dump(res, f)
        print("file generated in import directory")
    #import the ways in the graph as POI nodes
    greeter.import_way()
    print("import wayfile.json: done")
    #query overpass API for POI represented as nodes
    result = api.query(f"""(   
                               node(around:{dist},{lat},{lon})["amenity"];
                           );
                           out body;
                           """)
    #generation of the node file in the import directory
    list_node = []
    for node in result.nodes:
        d = {'type': 'node', 'id': node.id, 
             'lat': str(node.lat), 
             'lon': str(node.lon), 
             'geometry': 'POINT('+str(node.lat) + ' ' + str(node.lon) + ')',
             'tags': node.tags}
        list_node.append(d)
    res = {"elements": list_node}
    print("nodes to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + 'nodefile.json' , "w") as f:
        json.dump(res, f)
        print("file generated in import directory")
    #import the nodes in the graph as POI nodes
    greeter.import_way()
    greeter.import_node()
    #adding the nodes to the spatial layer
    if (options.spatial == 'True'):
        greeter.import_nodes_into_spatial_layer()
    #adding the location property to the OSMWayNodes
    greeter.set_location()
    #connect POI with roads layer
    greeter.connect_amenity()
    greeter.close()

    return 0


main()
