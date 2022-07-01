from neo4j import GraphDatabase
import overpy
import json
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
        """get neo4j instance import folder name""""
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
                                wn.location=point({latitude:tofloat(nodo.lat), longitude:tofloat(nodo.lon)})
                        
                        WITH n, nodo
                        MERGE (n)-[:TAGS]->(t:Tag)
                            ON CREATE SET t += nodo.tags
                    """)
        return result.values()

    def import_way(self):
        """import POI ways in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_way)
            return result

    @staticmethod
    def _import_way(tx):
        result = tx.run("""
                        CALL apoc.load.json("wayfile.json") YIELD value 
                        UNWIND value.elements AS elements
                        WITH [item IN elements WHERE elements.type = 'way'] AS ways
                        UNWIND ways AS way
                        MERGE (w:Way:PointOfInterest {osm_id: way.id}) ON CREATE SET w.name = way.tags.name
                        MERGE (w)-[:TAGS]->(t:Tag) ON CREATE SET t += way.tags
                        WITH w, way.nodes AS nodes
                        UNWIND nodes AS node
                        MATCH (wn:OSMWayNode {osm_id: node})
                        MERGE (w)-[:MEMBER]->(wn)
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
                        MATCH (n:Node)
                            WHERE distance(n.location, poi) < 100
                            AND n <> p
                        WITH n, p, distance(n.location, poi) AS dist ORDER BY dist
                        WITH head(collect(n)) AS nv, p
                        MERGE (p)-[r:ROUTE]->(nv)
                            ON CREATE SET r.distance = distance(nv.location, p.location), r.status = 'active'
                        MERGE (p)<-[ri:ROUTE]-(nv)
                            ON CREATE SET ri.distance = distance(nv.location, p.location), ri.status = 'active'
                    """)


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
    #query the api for POI nodes
    result = api.query(f"""[out:json];
                           (   
                               node(around:{dist},{lat},{lon})["amenity"];
                           );
                           out body;
                           """)

    list_node = []
    #save nodes in a local file
    for node in result.nodes:
        d = {'type': 'node', 'id': node.id, 'lat': str(node.lat), 'lon': str(node.lon), 'tags': node.tags}
        list_node.append(d)

    res = {"elements": list_node}
    print("nodes to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\nodefile.json'

    with open(path, "w") as f:
        json.dump(res, f)

    api = overpy.Overpass()

    result = api.query(f"""[out:json];
                           (   
                               way(around:{dist},{lat},{lon})["amenity"];
                           );
                           out body;
                    """)

    list_way = []
    #for node in result.nodes:
        # d = {'type': 'node', 'id': node.id, 'lat': str(node.lat), 'lon': str(node.lon)}
        # list_way.append(d)

    for way in result.ways:
        d = {'type': 'way', 'id': way.id, 'tags': way.tags}
        l_node = []
        for node in way.nodes:
            l_node.append(node.id)
        d['nodes'] = l_node
        list_way.append(d)

    res = {"elements": list_way}

    # print("ways to import:")
    # print(res)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + "\\wayfile.json"

    with open(path, "w") as f:
        json.dump(res, f)

    greeter.import_node()
    print("import nodefile.json: done")
    # greeter.import_way()
    # print("import wayfile.json: done")
    greeter.connect_amenity()
    greeter.close()

    return 0


main()
