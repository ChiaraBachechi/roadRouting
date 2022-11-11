from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

from Cycleways_and_Footways.General_Graphs_generation_and_connection import Nodes_generation
from Cycleways_and_Footways.General_Graphs_generation_and_connection import Relationships_generation


"""In this file we are going to show how to generate different layers' general graphs"""


def add_options():
    """Parameters needed to run the script"""

    parser = argparse.ArgumentParser(description='Generation of the general graphs.')
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
    parser.add_argument('--nameFileNeighborhoods', '-fnb', dest='file_name_neighborhoods', type=str,
                        help="""Insert the name of the .csv file containing neighborhoods.""",
                        required=True)
    return parser



def main(args=None):
    """Parsing parameters in input"""
    argParser = add_options()
    options = argParser.parse_args(args=args)

    """The main function can be split in two section : 1) generation of nodes; 
    2) generation of relationships between both nodes of the same layer and different layers"""

    """SECTION 1: GENERATION OF NODES"""

    """Generation of cycleways general graph nodes"""
    greeterCycleways = Nodes_generation.BicycleLanes.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterCycleways.import_bicycle_lanes(options.file_name_cycleways)
    greeterCycleways.import_lanes_in_spatial_layer()
    greeterCycleways.add_index()
    greeterCycleways.find_touched_lanes()
    greeterCycleways.find_closest_lanes(options.file_name_cycleways)
    greeterCycleways.close()

    """Generation of footways general graph nodes"""
    greeterFootways = Nodes_generation.Footways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterFootways.import_footways(options.file_name)
    greeterFootways.import_footways_in_spatial_layer()
    greeterFootways.add_index()
    greeterFootways.find_touched_footways()
    greeterFootways.find_closest_footways(options.file_name)
    greeterFootways.close()

    """Generation of nodes representing crossings mapped as nodes on OSM"""
    greeterCrossnodes = Nodes_generation.Crossnodes.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterCrossnodes.import_crossnodes(options.file_name)
    greeterCrossnodes.compute_location()
    greeterCrossnodes.import_crossnodes_in_spatial_layer()
    greeterCrossnodes.close()

    """Generation of nodes representing crossings mapped as ways on OSM"""
    greeterCrossways = Nodes_generation.Crossways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterCrossways.import_crossways(options.file_name)
    greeterCrossways.import_crossways_in_spatial_layer()
    greeterCrossways.close()


    """Generation of Neighborhood nodes"""
    greeterNeighborhoods = Nodes_generation.Neighborhoods.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterNeighborhoods.import_neighborhood_node(options.file_name)
    greeterNeighborhoods.import_neighborhoods_in_spatial_layer()



    """SECTION 2: GENERATION OF RELATIONSHIPS"""

    """Generation of relationships between cycleways and footways general graphs nodes"""
    greeterConnection_BL_FW = Relationships_generation.Connect_bicyclelanes_to_footways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_BL_FW.connect_footways_to_touched_bicycle_lanes()
    greeterConnection_BL_FW.connect_footways_to_close_lanes(options.file_name_footways)
    greeterConnection_BL_FW.close()

    """Generation relationships between crossnodes and footways nodes"""
    greeterConnection_CN_FW = Relationships_generation.Connect_crossingnodes_to_closest_footways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_CN_FW.connect_footways_to_crossing_nodes()
    greeterConnection_CN_FW.close()

    """Generation relationships between crossnodes and cycleways nodes"""
    greeterConnection_CN_BL = Relationships_generation.Connect_crossingnodes_to_closest_lanes.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_CN_BL.connect_lanes_to_crossing_nodes()
    greeterConnection_CN_BL.close()

    """Generation relationships between crossways and footways nodes"""
    greeterConnection_CW_FW = Relationships_generation.Connect_crossingways_to_footways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_CW_FW.connect_crossways_to_footways()
    greeterConnection_CW_FW.close()

    """Generation relationships between crossnodes and cycleways nodes"""
    greeterConnection_CW_BL = Relationships_generation.Connect_crossingways_to_lanes.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_CW_BL.connect_crossways_to_bicycle_lanes()
    greeterConnection_CW_BL.close()


    """Generation of relationships between general graphs nodes and Neighborhood nodes"""
    greeterConnection_Els_NB = Relationships_generation.Connect_elements_to_neighborhoods.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_Els_NB.elements_within_neighborhoods()
    greeterConnection_Els_NB.close()


    """Generation of relationships between POI nodes and cycleways general graph nodes"""
    greeterConnection_POI_BL = Relationships_generation.Connect_poi_to_closest_bicyclelanes.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_POI_BL.connect_poi_to_closest_bicycle_lanes()
    greeterConnection_POI_BL.close()

    """Generation of relationships between POI nodes and cycleways general graph nodes"""
    greeterConnection_POI_FW = Relationships_generation.Connect_poi_to_the_closest_footways.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnection_POI_FW.connect_poi_to_closest_footways()
    greeterConnection_POI_FW.close()


    return 0


main()
































