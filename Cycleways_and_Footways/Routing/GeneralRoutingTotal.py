from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time
import folium
from shapely import wkt
import pandas as pd
import geopandas as gpd

from Cycleways_and_Footways.Routing import Routing_on_General_graphs


"""In this file we are going to show how to perform routing on the general graphs"""

def add_options():
    """Parameters nedeed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of your starting location""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of your starting location""",
                        required=True)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the name of your destination""",
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
    parser.add_argument('--nameFilecycleways', '-fcl', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing the cycleways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
                        required=True)
    return parser



def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)

    """Setting weights on general graph's relationships and create projections"""
    greeterSetWeights = Routing_on_General_graphs.SetWeights.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterSetWeights.create_projections()
    greeterSetWeights.close()

    """ROUTING ON GENERAL GRAPHS"""
    greeterRouting = Routing_on_General_graphs.Routing.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeterRouting.get_path()[0][0] + '\\' + greeterRouting.get_import_folder_name()[0][0] + '\\'


    gdf_cycleways = Routing_on_General_graphs.Routing.read_file(path + options.file_name_cycleways)
    gdf_footways = Routing_on_General_graphs.Routing.read_file(path + options.file_name_footways)

    result_routing_cost = greeterRouting.routing_algorithm_based_on_cost(options.lat, options.lon, options.dest)
    Routing_on_General_graphs.Routing.creation_map(result_routing_cost, gdf_cycleways, gdf_footways, path, options.lat, options.lon)



main()
