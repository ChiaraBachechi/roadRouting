from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt
import osmnx as ox

import Elaboration_on_cicleways, Elaboration_on_footways, Elaboration_on_footways_and_cicleways, \
    Elaboration_on_crossing_nodes, Elaboration_on_crossing_ways, Elaboration_crossing_nodes_and_cycleways, \
    Elaboration_crossing_ways_and_cicleways, Elaboration_crossing_nodes_and_footways, Elaboration_crossing_ways_and_footways, \
    Elaboration_street_nodes

"""In this file we are going to make some data preprocessing on all the data of interest
in order to discover some relationships between them 
"""



class App:
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
    parser.add_argument('--nameFilecycleways', '-fc', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing cycleways.""",
                        required=True)
    parser.add_argument('--nameFileCrossingNodes', '-fcn', dest='file_name_crossing_nodes', type=str,
                        help="""Insert the name of the .json file containing crossing nodes.""",
                        required=True)
    parser.add_argument('--nameFileCrossingWays', '-fcw', dest='file_name_crossing_ways', type=str,
                        help="""Insert the name of the .json file containing crossing ways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing footways.""",
                        required=True)
    return parser

def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    gdf_cycleways = Elaboration_on_cicleways.read_file(path + options.file_name_cycleways)
    gdf_footways = Elaboration_on_footways.read_file(options.file_name_footways)
    gdf_crossing_nodes = Elaboration_on_crossing_nodes.read_file(options.file_name_crossing_nodes)
    gdf_crossing_ways = Elaboration_on_crossing_ways.read_file(path + options.file_name_crossing_ways)

    Elaboration_on_cicleways.preprocessing(gdf_cycleways)
    Elaboration_on_footways.preprocessing(gdf_footways)
    Elaboration_on_crossing_nodes.preprocessing(gdf_crossing_nodes)
    Elaboration_on_crossing_ways.preprocessing(gdf_crossing_ways)

    Elaboration_on_footways_and_cicleways.find_cycleways_touching_and_close_to_footways(gdf_footways, gdf_cycleways)
    Elaboration_crossing_nodes_and_cycleways.find_cycleways_close_to_crossing_ways(gdf_cycleways, gdf_crossing_nodes)
    Elaboration_crossing_nodes_and_footways.find_footways_close_to_crossing_nodes(gdf_footways, gdf_crossing_nodes)
    Elaboration_crossing_ways_and_cicleways.find_cycleways_close_to_crossing_ways(gdf_cycleways, gdf_crossing_ways)
    Elaboration_crossing_ways_and_footways.find_footways_close_to_crossing_ways(gdf_footways, gdf_crossing_ways)

    Elaboration_street_nodes.preprocessing(gdf_cycleways, gdf_footways, gdf_crossing_ways, options)

    Elaboration_on_cicleways.save_gdf(gdf_cycleways, path)
    Elaboration_on_footways.save_gdf(gdf_footways, path)
    Elaboration_on_crossing_nodes.save_gdf(gdf_crossing_nodes, path)
    Elaboration_on_crossing_ways.save_gdf(gdf_crossing_ways, path)

main()




