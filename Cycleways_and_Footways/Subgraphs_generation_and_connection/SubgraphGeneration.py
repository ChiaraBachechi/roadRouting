from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

import Nodes_generation.JunctionBikeCrossCreation
import Nodes_generation.BikeCrossCreation
import Nodes_generation.JunctionFootCrossCreation
import Nodes_generation.FootCrossCreation

import Relationships_generation.ConnectDifferentLayersJunctions

"""In this file we are going to show how to generate different layers' subgraphs"""

def add_options():
    """Parameters needed to run the script"""
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
                        help="""Insert the name of the .graphml file.""",
                        required=True)
    parser.add_argument('--nameFilecycleways', '-fc', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing the cycleways.""",
                        required=True)
    parser.add_argument('--nameFileCrossingWays', '-fcw', dest='file_name_crossing_ways', type=str,
                        help="""Insert the name of the .json file containing the crossing ways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
                        required=True)

    return parser




def main(args=None):
    """Parsing parameters in input"""
    argParser = add_options()
    options = argParser.parse_args(args=args)

    """The main function can be split in two section : 1) generation of nodes and relationships; 
    2) generation of relationships between layers"""

    """SECTION 1: GENERATION OF NODES"""

    """Generation of cycleways subgraph nodes and relationships"""
    '''
    print("Generation of cycleways subgraph nodes and relationships")
    greeterJBK = Nodes_generation.JunctionBikeCrossCreation.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterJBK.creation_graph(options.file_name)
    greeterJBK.set_label()
    greeterJBK.set_location()
    greeterJBK.set_distance()
    greeterJBK.set_index()
    greeterJBK.close()

    print("Connection_layers")
    greeterBK = Nodes_generation.BikeCrossCreation.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterBK.connect_junctions_to_cycleways(options.file_name_cycleways)
    greeterBK.connect_junctions_to_crossings(options.file_name_crossing_ways)
    print("Connection_layers : done")
    print("Change labels and create indexes")
    greeterBK.change_of_labels()
    greeterBK.createIndexes()
    print("Change labels and create indexes : done")
    #greeterBK.connect_to_road_junctions()
    greeterBK.import_bikecrosses_into_spatial_layer()
    print("junctions imported in the spatial layer")
    greeterBK.close()
    print("Generation of cycleways subgraph nodes and relationships : done")

    """Generation of footways subgraph nodes and relationships"""
    print("Generation of footways subgraph nodes and relationships")
    greeterJFC = Nodes_generation.JunctionFootCrossCreation.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterJFC.creation_graph(options.file_name)
    greeterJFC.set_label()
    greeterJFC.set_location()
    greeterJFC.set_distance()
    '''
    #greeterJFC = Nodes_generation.JunctionFootCrossCreation.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #greeterJFC.set_index()
    #greeterJFC.close()

    print("Connection_layers")
    greeterFC = Nodes_generation.FootCrossCreation.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterFC.connect_junctions_to_footways(options.file_name_cycleways)
    greeterFC.connect_junctions_to_crossings(options.file_name_crossing_ways)
    print("Connection_layers : done")
    print("Change labels and create indexes")
    greeterFC.change_of_labels()
    greeterFC.createIndexes()
    print("Change labels and create indexes : done")
    #greeterFC.connect_to_road_junctions()
    greeterFC.import_footcrosses_into_spatial_layer()
    print("junctions imported in the spatial layer")
    greeterFC.close()
    print("Generation of cycleways subgraph nodes and relationships : done")

    """SECTION 2: CONNECTION OF SUBGRAPHS LAYERS"""
    greeterConnectionLayers = Relationships_generation.ConnectDifferentLayersJunctions.App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    greeterConnectionLayers.connect_junctions_of_different_layers()
    greeterConnectionLayers.delete_roadjunctions_with_same_location_of_footcrosses()
    greeterConnectionLayers.change_labels()
    greeterConnectionLayers.close()

    return 0

main()







































































