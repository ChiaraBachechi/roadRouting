import osmnx as ox
import argparse
from neo4j import GraphDatabase
import os


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def creation_graph(self):
        #creation of the dual graph
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_graph)
            return result

    @staticmethod
    def _creation_graph(tx):
        #creation of nodes, a node for each street
        result = tx.run("""
                        match (m)-[r:ROUTE {status: 'active'}]->(n) 
                        with distinct r.osmid as street_names
                        unwind street_names as street_name
                        create (road:RoadOsm {osmid: street_name})
                        with street_name
                        match (m)-[r1:ROUTE {osmid: street_name, status: 'active'}]->(n)
                        with avg(r1.AADT) as AADT, sum(r1.distance) as dist,street_name,r1.name as road_name
                        match (d:RoadOsm {osmid: street_name}) set d.traffic = AADT/dist, 
                                        d.AADT=AADT,d.distance = dist,d.name= road_name
                    """)
        print(result.values())
        #creation of relations, a relation for each connection 
        #(a junction will be represented by more than one connection)
        result = tx.run("""
                        match (m)-[r:ROUTE]->(n) 
                        with distinct r.osmid as street_names
                        unwind street_names as street_name
                        match (m)-[r1:ROUTE {osmid: street_name}]->(n)
                        with m,street_name
                        match (x)-[r2:ROUTE]->(m)
                        where r2.osmid <> street_name
                        with r2.osmid as source,street_name,m
                        match (r1:RoadOsm {osmid:source}),(r2:RoadOsm {osmid:street_name})
                        create (r1)-[r:CONNECTED {junction: m.id,location: m.location}]->(r2)
                    """)
        print(result.values())
        return result.values()


def add_options():
    parser = argparse.ArgumentParser(description='Creation of routing graph.')
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
    argParser = add_options()
    #retrieve arguments
    options = argParser.parse_args(args=args)
    #connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #creation of the dual graph
    greeter.creation_graph()
    greeter.close()
    return 0


main()
