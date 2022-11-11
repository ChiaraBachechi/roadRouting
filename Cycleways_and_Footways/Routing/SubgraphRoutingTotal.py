from neo4j import GraphDatabase
import overpy
import osmnx as ox
import json
import argparse
import os
import time
import folium
from shapely import wkt
import pandas as pd
import geopandas as gpd

from Cycleways_and_Footways.Routing import Routing_on_subgraphs


"""In this file we are going to show how to perform routing on the subgraphs"""




def add_options():
    """Parameters needed to run the script"""
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
    parser.add_argument('--mode', '-m', dest='mode', type=str,
                        help="""Choose the modality of routing : cycleways or footways.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file.""",
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
    greeterSetWeights = Routing_on_subgraphs.SetWeights.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterSetWeights.set_relations_weights()
    greeterSetWeights.close()

    """Create projections"""
    greeterProj = Routing_on_subgraphs.GraphProjections.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterProj.create_projections()
    greeterProj.close()


    """ROUTING ON SUBGRAPHS USING DIJKSTRA"""

    greeterDijkstra = Routing_on_subgraphs.Routing_Dijkstra.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeterDijkstra.get_path()[0][0] + '\\' + greeterDijkstra.get_import_folder_name()[0][0] + '\\'

    G = ox.io.load_graphml(path + options.file_name)
    nodes, edges = ox.graph_to_gdfs(G)
    nodes.reset_index(inplace=True)

    """The user can choose how to travel, using a bike or just walking"""
    graph_projection = ""
    if options.mode == 'cycleways':
        graph_projection = "bike_routes"
    else:
        graph_projection = "foot_routes"

    """Extract cycleways and footways data from their json files"""
    gdf_cycleways = Routing_on_subgraphs.Routing_Dijkstra.read_file(path + options.file_name_cycleways)
    gdf_footways = Routing_on_subgraphs.Routing_Dijkstra.read_file(path + options.file_name_footways)

    """Routing considering as weight the cost"""
    result_routing_cost = greeterDijkstra.routing_algorithm_based_on_cost(options.lat, options.lon, options.dest,
                                                                  graph_projection + "_cost")

    """Routing considering as weight the travel time"""
    result_routing_travel_time = greeterDijkstra.routing_algorithm_based_on_travel_time(options.lat, options.lon, options.dest,
                                                                                graph_projection + "_travel_time")

    """Generation of the map with the obtained paths displayed"""
    Routing_on_subgraphs.Routing_Dijkstra.creation_map(result_routing_cost, result_routing_travel_time, nodes, gdf_cycleways, gdf_footways, path, options.lat,
                 options.lon)


    greeterDijkstra.close()


    """ROUTING ON SUBGRAPHS USING A*"""

    greeterAstar = Routing_on_subgraphs.Routing_AStar.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeterAstar.get_path()[0][0] + '\\' + greeterAstar.get_import_folder_name()[0][0] + '\\'

    G = ox.io.load_graphml(path + options.file_name)
    nodes, edges = ox.graph_to_gdfs(G)
    nodes.reset_index(inplace=True)

    """The user can choose how to travel, using a bike or just walking"""
    graph_projection = ""
    if options.mode == 'cycleways':
        graph_projection = "bike_routes"
    else:
        graph_projection = "foot_routes"

    """Extract cycleways and footways data from their json files"""
    gdf_cycleways = Routing_on_subgraphs.Routing_AStar.read_file(path + options.file_name_cycleways)
    gdf_footways = Routing_on_subgraphs.Routing_AStar.read_file(path + options.file_name_footways)

    """Routing considering as weight the cost"""
    result_routing_cost = greeterAstar.routing_algorithm_based_on_cost(options.lat, options.lon, options.dest,
                                                                  graph_projection + "_cost")

    """Routing considering as weight the travel time"""
    result_routing_travel_time = greeterAstar.routing_algorithm_based_on_travel_time(options.lat, options.lon, options.dest,
                                                                                graph_projection + "_travel_time")

    """Generation of the map with the obtained paths displayed"""
    Routing_on_subgraphs.Routing_AStar.creation_map(result_routing_cost, result_routing_travel_time, nodes, gdf_cycleways, gdf_footways, path, options.lat,
                 options.lon)

    greeterAstar.close()

main()





