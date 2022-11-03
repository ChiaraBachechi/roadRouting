import argparse
import requests
import geopandas as gpd
from Get_footways_from_OSM import createQueryFootways, App
from Get_crossing_ways_from_OSM import createQueryCrossingWays
from Get_crossing_nodes_from_OSM import createQueryCrossingNodes
from GraphmlFileCreation import getStreetNodes
from Tools import *
import os



def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of CROSSING NODES in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be covered""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file.""",
                        required=True)
    return parser



def getData(url, query, greeter, strIdx, strType, filename):
    """Get the data of interest from OSM"""

    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    print("Get Data from OSM")
    features = [elem_to_feature(elem, strType) for elem in data]
    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    print("GeoDataFrame generated!!")
    list_ids = [strIdx + str(elem["id"]) for elem in data]
    gdf.insert(0, 'id', list_ids)

    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    save_gdf(gdf, path, filename)
    print("Data stored in json format")


def main(args=None):
    """In this file we are going to extract all the data of interest from OSM
    and save them within json files
    """

    argParser = add_options()
    options = argParser.parse_args(args=args)
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    url = 'http://overpass-api.de/api/interpreter'

    queryFootways = createQueryFootways(dist, lat, lon)
    getData(url, queryFootways, greeter, "way/", "LineString", "footways.json")
    print("Extracting footways data : done")
    
    queryCrossNodes = createQueryCrossingNodes(dist, lat, lon)
    getData(url, queryCrossNodes, greeter, "node/", "Point", "crossingnodes.json")
    print("Extracting crossing nodes data : done")

    queryCrossWays = createQueryCrossingWays(dist, lat, lon)
    getData(url, queryCrossWays, greeter, "way/", "LineString", "crossingways.json")
    print("Extracting crossing ways data : done")

    getStreetNodes(dist, lat, lon, greeter, options.file_name)
    print("Extracting street nodes : done")

main()





















