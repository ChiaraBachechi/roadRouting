import argparse
import requests
import geopandas as gpd
from Get_footways_from_OSM import createQueryFootways, App
from Get_crossing_ways_from_OSM import createQueryCrossingWays
from Get_crossing_nodes_from_OSM import createQueryCrossingNodes
from GraphmlFileCreation import getStreetNodes
from Get_cycleway_from_OSM import createQueryCycleways,getDataCycleways
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
    parser.add_argument('--nameFileFootway', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the footways .json file.""",
                        required=True)
    parser.add_argument('--nameFileCrossingNodes', '-fcn', dest='file_name_crossingnodes', type=str,
                        help="""Insert the name of the crossing nodes .json file.""",
                        required=True)
    parser.add_argument('--nameFileCrossingWays', '-fcw', dest='file_name_crossingways', type=str,
                        help="""Insert the name of the crossing ways .json file.""",
                        required=True)
    parser.add_argument('--nameFileStreet', '-fsn', dest='file_name_streetnodes', type=str,
                        help="""Insert the name of the street nodes .graphml file.""",
                        required=True)
    parser.add_argument('--nameFileCycleway', '-fcl', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the cycleways .json file.""",
                        required=True)

    parser.add_argument('--nameFileNeighborhood', '-fnb', dest='file_name_neighborhood', type=str,
                        help="""Insert the name of the neighborhood .json file.""",
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

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    url = 'http://overpass-api.de/api/interpreter'
    queryCycleways = createQueryCycleways(dist, lat, lon)
    getDataCycleways(url, queryCycleways, options.file_name_cycleways, path)
    print("Extracting CYCLEWAYS data : done")
    
    """Generate overpass query to fetch footways data and extract them"""
    queryFootways = createQueryFootways(dist, lat, lon)
    getData(url, queryFootways, greeter, "way/", "LineString", options.file_name_footways)
    print("Extracting footways data : done")
    
    """Generate overpass query to fetch crossing nodes data and extract them"""
    queryCrossNodes = createQueryCrossingNodes(dist, lat, lon)
    getData(url, queryCrossNodes, greeter, "node/", "Point", options.file_name_crossingnodes)
    print("Extracting crossing nodes data : done")

    """Generate overpass query to fetch crossing ways data and extract them"""
    queryCrossWays = createQueryCrossingWays(dist, lat, lon)
    getData(url, queryCrossWays, greeter, "way/", "LineString", options.file_name_crossingways)
    print("Extracting crossing ways data : done")

    """Extract street nodes data from OSM"""
    getStreetNodes(dist, lat, lon, greeter, options.file_name_streetnodes)
    print("Extracting street nodes : done")

    """Save neighborhoods data in the import folder of the neo4j instance"""
    gdf_neighborhoods = gpd.read_file("QuartieriModena.geojson",  crs={'init': 'epsg:4326'}, geometry='geometry')
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    save_gdf(gdf_neighborhoods, path, options.file_name_neighborhood)



main()





















