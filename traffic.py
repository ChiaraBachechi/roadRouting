from neo4j import GraphDatabase
import argparse
import os
import shutil


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        #gets the path of the neo4j instance
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
        #gets the name of the import folder of the neo4j instance
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    def import_traffic(self):
        #imports the traffic from the csv file to the graph
        with self.driver.session() as session:
            result = session.write_transaction(self._import_traffic)
            return result

    @staticmethod
    def _import_traffic(tx):
        result = tx.run("""
                        load csv with headers from 'file:///traffic.csv' as row
                           match (a:Node {id: row.node_start})
                           match(b:Node {id: row.node_end})
                           merge (a)-[:AADT2019 {traffic_volume:round(toFloat(row.traffic_volume),2)
                                                         ,year: row.year,osmid: row.id_road_section}]->(b);
                    """)
        return result.values()

    def add_route_AADT_property(self):
        #insert AADT as a property of the route relationship in the primal graph
        with self.driver.session() as session:
            result = session.write_transaction(self._add_route_AADT_property)
            return result

    @staticmethod
    def _add_route_AADT_property(tx):
        result = tx.run("""
                        load csv with headers from 'file:///traffic.csv' as row
                           match (a:Node {id: row.node_start})-[route:ROUTE]->(b:Node {id: row.node_end})
                           call { with row
                                  match (a:Node {id: row.node_start})-[r:AADT2019]->(b:Node {id: row.node_end})
                                  return avg(r.traffic_volume) as avgTraf
                                }
                           set route.AADT = avgTraf;
                    """)
        return result.values()

    def estimate_AADT_property(self):
        #estimates the AADT where no traffic data are provided
        with self.driver.session() as session:
            result = session.write_transaction(self._estimate_AADT_property)
            return result

    @staticmethod
    def _estimate_AADT_property(tx):
        #considering nearest AADT relationships
        result = tx.run("""
                        MATCH (n:Node)-[route:ROUTE]->(m:Node)  WHERE NOT EXISTS(route.AADT)
                           call { with n
                                  match (n)-[r:AADT2019*1..5]->(b:Node)
                                  unwind r as p
                                  return avg(p.traffic_volume) as avgTraf
                                }
                           set route.AADT = avgTraf
                    """)
        result = tx.run("""
                        MATCH (n:Node)-[route:ROUTE]->(m:Node)  WHERE NOT EXISTS(route.AADT)
                           call { with n
                                  match (n)-[r:ROUTE*1..3]->(b:Node)
                                  unwind r as p
                                  return avg(p.AADT) as avgTraf
                                }
                           set route.AADT = avgTraf
                    """)
        return result.values()
    
    def find_highway_types(self):
        #returns all the highway types from the route relationships' attributes
        with self.driver.session() as session:
            result = session.write_transaction(self._find_highway_types)
            return result

    @staticmethod    
    def _find_highway_types(tx):
        result = tx.run("""
                        MATCH (:Node)-[route:ROUTE]->(:Node)
                                 return route.highway,round(avg(route.AADT),2) as mean
        """)
        return result.values()

    def estimate_AADT_from_road_type(self,name,value):
        #evaluates AADT considering the AADT of roads of the same type
        with self.driver.session() as session:
            result = session.write_transaction(self._estimate_AADT_from_road_type,name,value)
            return result
    @staticmethod    
    def _estimate_AADT_from_road_type(tx,name,value):
        query = """
                       match ()-[r:ROUTE]->() WHERE NOT EXISTS(r.AADT) and r.highway = '""" + str(name) + """'
                          set r.AADT=""" + str(value) 
        print(query)
        # result = tx.run("""
                       # match ()-[r:ROUTE]->() WHERE NOT EXISTS(r.AADT) and r.highway = '$h'
                          # call { MATCH (:Node)-[route:ROUTE]->(:Node)  
                                 # where route.highway = '$h2'
                                 # return avg(route.AADT) as mean
                               # }
                          # set r.AADT= mean
               # """, h = h, h2 = h)
        result = tx.run(query)
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
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name and path of the .csv file.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    #retrieve arguments
    options = argParser.parse_args(args=args)
    #connecting neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #copying the traffic file in the import folder of neo4j
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\traffic.csv'
    shutil.copyfile(options.file_name, path)
    #import traffic data in the graph
    greeter.import_traffic()
    #insert AADT property in the ROUTE relationships of the primal graph
    greeter.add_route_AADT_property()
    #extimate traffic flow where the route relation has no AADT property
    greeter.estimate_AADT_property()
    #retrieve all the highway types in the graph
    road_types = greeter.find_highway_types()
    #for each road type estimate the AADT where missing
    for h in road_types:
        if h[1]:
            greeter.estimate_AADT_from_road_type(h[0],h[1])
    greeter.close()
    return 0


main()
