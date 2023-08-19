from neo4j import GraphDatabase
import json
import shutil
import argparse
import os

class App:
    """In this file we are going to extract from OSM crossings mapped as nodes"""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""

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
        """gets the path of the import folder of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()
    def generate_GTFS_based_graph(self):
        """gets the path of the neo4j instance"""

        with self.driver.session() as session:
            print("constraint creation")
            session.run("create constraint for (a:Agency) require a.id is unique;")
            session.run("create constraint for (r:Route) require r.id is unique;")
            session.run("create constraint for (t:Trip) require t.id is unique;")
            session.run("create index for (t:Trip) on (t.service_id);")
            session.run("create constraint for (s:Stop) require s.id is unique;") 
            session.run("create index for (s:Stoptime) on (s.stop_sequence);")
            session.run("create index for (s:Stop) on (s.name);")
            session.run("create constraint for (s:Service) require s.service_id is unique;")
            session.run("create constraint for (d:Day) require d.day is unique;")
            print('Constraint e indici creati...')
      
        print("Inserting Agencies")
        query = """load csv with headers from  
              'file:///agency.txt' as csv  
              create (:Agency {name: csv.agency_name, url: csv.agency_url, timezone: csv.agency_timezone});"""
        session.run(query)

        print("Inserting Routes")
        query = """load csv with headers from  
              'file:///routes.txt' as csv  
              match (a:Agency {name: 'aMo Modena'})  
              create (a)-[:OPERATES]->(:Route {id: csv.route_id, short_name: csv.short_name, long_name: csv.route_long_name, type: toInteger(csv.route_type)});"""
        session.run(query)

        print("Insering Trips")
        query = """load csv with headers from 
              'file:///trips.txt' as csv
              match (r:Route {id: csv.route_id})
              create (r)<-[:USES]-(:Trip {service_id: csv.service_id, id: csv.trip_id, direction_id: csv.direction_id, shape_id: csv.shape_id, headsign: csv.trip_headsign});"""
        session.run(query)

        print("Inserimenting Stops")
        query = """load csv with headers from 
              'file:///stops.txt' as csv  
              create (:Stop {id: csv.stop_id, name: csv.stop_name, lat: toFloat(csv.stop_lat), lon: toFloat(csv.stop_lon)});"""
        session.run(query)

        print("Inserting StopTimes")
        query = """CALL apoc.periodic.iterate(
              "load csv with headers from 'file:///stop_times.txt' as csv return csv",
              "match (t:Trip {id: csv.trip_id}), (s:Stop {id: csv.stop_id}) create (t)<-[:PART_OF_TRIP]-(st:Stoptime {arrival_time: time(csv.arrival_time), departure_time: time(csv.departure_time), stop_sequence: toInteger(csv.stop_sequence)})-[:LOCATED_AT]->(s)",
              {batchSize:1000, parallel:true})"""
        session.run(query)

        print("Inserting StopTimes relationships")
        query = """match (s1:Stoptime)-[:PART_OF_TRIP]->(t:Trip),  
              (s2:Stoptime)-[:PART_OF_TRIP]->(t)  
              where s2.stop_sequence=s1.stop_sequence+1  
              create (s1)-[p:PRECEDES]->(s2) set p.waiting_time = duration.inSeconds(s1.departure_time,s2.departure_time).seconds;"""
        session.run(query)
        
        print("creation of Service nodes")
        query = """match (t:Trip) with distinct t.service_id as service merge (s:Service{id:service})"""
        session.run(query)
        
        print("Connection of Sevice nodes and Trip nodes")
        query = """match (t:Trip)  match  (s:Service{id:t.service_id}) merge (t)-[:SERVICE_TYPE]->(s)"""
        session.run(query)
        
        print("Generate the Date nodes")
        query = """CALL apoc.periodic.iterate(
            "load csv with headers from 'file:///calendar_dates.txt' as csv return csv",
            "match (s:Service {id: csv.service_id}) merge (d:Day {day:date({year: toInteger(left(csv.date,4)), month: toInteger(substring(csv.date, 4, 2)), day: toInteger(right(csv.date,2))})}) merge (s)-[:VALID_IN]->(d) SET d.exception_type = csv.exception_type",
            {batchSize:500})"""
        session.run(query)
        
        


def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Generation of the trips-expanded GTFS-based graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--GTFSpath', '-GTFS', dest='GTFS_path', type=str,
                        help="""Insert the path where the GTFS files are located""",
                        required=True)
    return parser
def main(args=None):
    """Parsing of input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)

    """Generation of the App object to connect with the neo4j database instance"""
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    destination_path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    origin_path = options.GTFS_path
    if os.path.isfile(origin_path + '\\agency.txt'):
        shutil.copyfile(origin_path + '\\agency.txt', destination_path + 'agency.txt')
    else:
        print('missing agency.txt file in directory')
        exit()
    if os.path.isfile(origin_path + '\\routes.txt'):
        shutil.copyfile(origin_path + '\\routes.txt', destination_path + 'routes.txt')
    else:
        print('missing routes.txt file in directory')
        exit()
    if os.path.isfile(origin_path + '\\trips.txt'):
        shutil.copyfile(origin_path + '\\trips.txt', destination_path + 'trips.txt')
    else:
        print('missing trips.txt file in directory')
        exit()
    if os.path.isfile(origin_path + '\\stops.txt'):
        shutil.copyfile(origin_path + '\\stops.txt', destination_path + 'stops.txt')
    else:
        print('missing stops.txt file in directory')
        exit()
    if os.path.isfile(origin_path + '\\calendar_dates.txt'):
        shutil.copyfile(origin_path + '\\calendar_dates.txt', destination_path + 'calendar_dates.txt')
    else:
        #shutil.copyfile(origin_path + '\\calendar.txt', destination_path + 'calendar.txt')
        exit()
    if os.path.isfile(origin_path + '\\stop_times.txt'):
        shutil.copyfile(origin_path + '\\stop_times.txt', destination_path + 'stop_times.txt')
    else:
        print('missing stop_times.txt file in directory')
        exit()
    greeter.generate_GTFS_based_graph()
    
main()