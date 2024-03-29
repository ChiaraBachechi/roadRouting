import osmnx as ox
import argparse
from neo4j import GraphDatabase
import os


class App:
    """In this file we are going to extract street nodes from OSM"""

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

 

def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Creation of routing graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
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
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file without extention.""",
                        required=True)
    return parser


def getBicycleNodes(dist, lat, lon, greeter, filename):
    """Get the street nodes data from OSM"""

    G = ox.graph_from_point((lat, lon),
                            dist=int(dist),
                            dist_type='bbox',
                            simplify=False,
                            network_type='all_private',
                            custom_filter = '["highway"]["bicycle"!~"no"][!"boundary"]["highway"!~"motorway_link"]["highway"!~"motorway"]["highway"!~"trunk"]["highway"!~"trunk_link"]["highway"!~"motorway_junction"][!"railway"][!"destination"]'
                            )

    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' + filename + '_bike.graphml'
    ox.save_graphml(G, path)

def getFootNodes(dist, lat, lon, greeter, filename):
    """Get the street nodes data from OSM"""

    G = ox.graph_from_point((lat, lon),
                            dist=int(dist),
                            dist_type='bbox',
                            simplify=False,
                            network_type='all_private',
                            custom_filter = '["foot"!~"no"]["highway"!~"motorway_link"]["highway"!~"motorway"]["highway"!~"trunk"]["highway"!~"trunk_link"]["highway"!~"motorway_junction"][!"railway"]'
                            )

    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' + filename + '_foot.graphml'
    ox.save_graphml(G, path)

def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Get the street nodes data from OSM"""
    getBicycleNodes(options.dist, options.lat, options.lon, greeter, options.file_name)
    getFootNodes(options.dist, options.lat, options.lon, greeter, options.file_name)

if __name__ == "__main__":
    main()