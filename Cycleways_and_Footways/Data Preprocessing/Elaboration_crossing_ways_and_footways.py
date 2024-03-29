from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt


"""In this file we are going to make some preprocessing in order to find
   relations between pedestrian paths and crossings mapped as ways
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

    parser = argparse.ArgumentParser(description='Data elaboration of footways and crossing ways.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFileCrossings', '-fw', dest='file_name_crossings', type=str,
                        help="""Insert the name of the .json file of crossing ways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file of footways.""",
                        required=True)
    return parser


def read_file(path):
    """read the file specified by the path"""

    f = open(path)
    json_file = json.load(f)
    df = pd.DataFrame(json_file['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:4326')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def find_footways_close_to_crossing_ways(gdf_footways, gdf_crossing_ways):
    """Find the footways that are close to a signaled crossing mapped as a way"""

    #gdf_crossing_ways.to_crs(epsg=3035, inplace=True)

    list_closest_footways = []
    for i in range(gdf_crossing_ways.shape[0]):
        list_closest_footways.append([])

    gdf_crossing_ways['closest_footways'] = list_closest_footways

    s = gdf_footways['geometry']

    for index, r in gdf_crossing_ways.iterrows(): 
        print(index)
        polygon = r['geometry']
        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_crossing_ways[gdf_crossing_ways['id'] == r.id]['closest_footways'].iloc[0].append(
                gdf_footways.iloc[i].id)

        l1 = list(s.sindex.query(polygon, predicate="touches"))
        for i in l1:
            gdf_crossing_ways[gdf_crossing_ways['id'] == r.id]['closest_footways'].iloc[0].append(
                gdf_footways.iloc[i].id)

    return gdf_crossing_ways



def save_gdf(gdf, path):
    """save the geopandas DataFrame in a json file"""

    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path , orient='table')




def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    """Read the content of the json files and store it in a geodataframe"""
    gdf_footways = read_file(path + options.file_name_footways)
    gdf_crossing_ways = read_file(path + options.file_name_crossings)

    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_crossing_ways.to_crs(epsg=3035, inplace=True)

    """Find relationships between footways and crossings mapped as ways"""
    gdf_crossing_ways = find_footways_close_to_crossing_ways(gdf_footways, gdf_crossing_ways)
    print("Find crossing ways that are close or touching cycleways : done ")
    
    save_gdf(gdf_crossing_ways, path + options.file_name_crossings)


if __name__ == "__main__":
    main()