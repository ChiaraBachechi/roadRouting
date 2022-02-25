from neo4j import GraphDatabase
import folium as fo
import argparse


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def close_street(self, street):
        with self.driver.session() as session:
            result = session.write_transaction(self._close_one_street, street)
            print('{} is now close'.format(street))
            return result

    @staticmethod
    def _close_one_street(tx, street):
        result = tx.run("""
                    MATCH ()-[r:ROUTE]-() 
                    WHERE r.name = $street  
                        SET r.status='close' 

                    WITH r 
                    MATCH (p:PointOfInterest)-[:MEMBER]->(wn:OSMWayNode)-[re:ROUTE]-(:Node)-[r]-(:Node) 
                        DELETE re 
                    WITH wn, wn.location AS poi 
                    MATCH (n:Node)-[ra:ROUTE]-(:Node) 
                        WHERE n <> wn 
                        AND distance(n.location, poi) < 100 
                        AND ra.status = 'active' 

                    WITH n, wn, distance(n.location, poi) AS dist ORDER BY dist 

                    WITH head(collect(n)) AS nv, wn 
                    MERGE (wn)-[r:ROUTE]->(nv) 
                        ON CREATE SET r.distance = distance(nv.location, wn.location), r.status='active' 
                    MERGE (wn)<-[ri:ROUTE]-(nv) 
                        ON CREATE SET ri.distance = distance(nv.location, wn.location), ri.status='active' """,
                        street=street)
        return result.values()

    def active_street(self, street):
        with self.driver.session() as session:
            result = session.write_transaction(self._active_one_street, street)
            print('{} is now active'.format(street))
            return result

    @staticmethod
    def _active_one_street(tx, street):
        result = tx.run("""
            MATCH (n)-[r:ROUTE]-() 
            WHERE r.name = $street  
                SET r.status = 'active' 
            WITH n 
            MATCH (p:PointOfInterest)-[:MEMBER]->(wn:OSMWayNode) 
            WHERE distance(wn.location, n.location) < 100 
            WITH wn, n 
            MATCH (wn)-[r:ROUTE]-() 
                DELETE r 
            WITH n, wn, distance(wn.location, n.location) AS dist ORDER BY dist 
            WITH head(collect(n)) AS nv, wn 
            MERGE (wn)-[rn:ROUTE]->(nv) 
                ON CREATE SET rn.distance = distance(wn.location, nv.location), rn.status = 'active' 
            MERGE (wn)<-[rni:ROUTE]-(nv) 
                ON CREATE SET rni.distance = distance(wn.location, nv.location), rni.status = 'active' """,
                        street=street)
        return result.values()


def addOptions():
    parser = argparse.ArgumentParser(description='Routing between two point of interest nodes in OSM.')
    parser.add_argument('--street', '-s', dest='street_name', type=str,
                        help="""Insert the name of the street whose status need to be to changed.""",
                        required=True)
    parser.add_argument('--status', '-st', dest='new_status', type=str,
                        help="""Insert 'open' to open the street and 'close' to close the street.""",
                        required=True)
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
    argParser = addOptions()
    options = argParser.parse_args(args=args)
    street = options.street_name
    status = options.new_status
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    if (status == 'open'):
        greeter.active_street(street)
    else:
        greeter.close_street(street)
    greeter.close()
    return 0


main()
