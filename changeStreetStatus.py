from neo4j import GraphDatabase
import folium as fo
import argparse


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def close_street(self, street):
        """the method closes the given street to traffic 
           setting its status to close and connecting its POI to other roads"""
        with self.driver.session() as session:
            result = session.write_transaction(self._close_one_street, street)
            print('{} is now close'.format(street))
            return result

    @staticmethod
    def _close_one_street(tx, street):
        result = tx.run("""
                    MATCH ()-[r:ROUTE]-() 
                    WHERE r.name = $street  
                        SET r.status='close'""",
                        street=street)
        esult = tx.run("""
                    MATCH (r:RoadOsm)
                    WHERE r.name = $street  
                        SET r.status='close'""",
                        street=street)
        return result.values()
    
    def close_street_by_osmid(self, osmid):
        """the method closes the given street to traffic 
           setting its status to close and connecting its POI to other roads"""
        with self.driver.session() as session:
            result = session.write_transaction(self._close_one_street_by_osmid, osmid)
            print('{} is now close'.format(osmid))
            return result

    @staticmethod
    def _close_one_street_by_osmid(tx, osmid):
        result = tx.run("""
                    MATCH ()-[r:ROUTE]-() 
                    WHERE r.osmid = $osmid  
                        SET r.status='close' """,
                        osmid=osmid)
        result = tx.run("""
                    MATCH (r:RoadOsm)
                    WHERE r.osmid = $osmid  
                        SET r.status='close'""",
                        osmid=osmid)
        return result.values()

    def active_street(self, street):
        """the method opens the given street to traffic 
           setting its status to active and re-connecting its POI to other roads"""
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
            """,street=street)
        result = tx.run("""
            MATCH (r:RoadOsm)
            WHERE r.name = $street 
                SET r.status = 'active' 
            """,street=street)
        return result.values()
    
    def active_street_by_osmid(self, osmid):
        """the method opens the given street to traffic 
           setting its status to active and re-connecting its POI to other roads"""
        with self.driver.session() as session:
            result = session.write_transaction(self._active_one_street_by_osmid, osmid)
            print('{} is now active'.format(osmid))
            return result

    @staticmethod
    def _active_one_street_by_osmid(tx, osmid):
        result = tx.run("""
            MATCH (n)-[r:ROUTE]-() 
            WHERE r.osmid = $osmid  
                SET r.status = 'active' 
            """,osmid=osmid)
        result = tx.run("""
            MATCH (r:RoadOsm)
            WHERE r.osmid = $osmid  
                SET r.status = 'active' 
            """,osmid=osmid)
        return result.values()


def addOptions():
    parser = argparse.ArgumentParser(description='Routing between two point of interest nodes in OSM.')
    parser.add_argument('--street', '-s', dest='street_name', type=str,
                        help="""Insert the name of the street whose status need to be to changed.""",
                        required=False)
    parser.add_argument('--osmid', '-id', dest='osmid_name', type=str,
                        help="""Insert the OSM id of the street whose status need to be to changed.""",
                        required=False, default = "")
    parser.add_argument('--status', '-st', dest='new_status', type=str,
                        help="""Insert 'open' to open the street and 'close' to close the street.""",
                        required=True, default = "")
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
    #retrieving attributes
    options = argParser.parse_args(args=args)
    street = options.street_name
    osmid = options.osmid_name
    status = options.new_status
    if(street == "" and osmid == ""):
        print("ERROR: no osmid nor street name provided")
        return 0
    #connecting to Neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    if (status == 'open'):
        if(osmid == ""):
            #opening the given street
            greeter.active_street(street)
        else:
            greeter.active_street_by_osmid(osmid)
    elif (status == 'close'):
        if(osmid == ""):
            #closing the given street
            greeter.close_street(street)
        else:
            greeter.close_street_by_osmid(osmid)
    greeter.close()
    return 0


main()
